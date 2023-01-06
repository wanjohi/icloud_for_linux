import os
import sys
from datetime import datetime, timezone
import logging
import time
import argparse
from pathlib import Path
from pyicloud.base import PyiCloudService
from pyicloud.exceptions import PyiCloudAPIResponseException
from pyicloud.include.constants import DATA_TIMEOUT_S
from pyicloud.utils import store_password_in_keyring, get_password, password_exists_in_keyring
from inotify_simple import INotify, flags
import asyncio

# create console handler and set level to debug
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# Use the pyicloud logger
LOGGER = logging.getLogger()

# add ch to logger
LOGGER.addHandler(ch)
LOGGER.setLevel(logging.DEBUG)


# Cookies path
cookie_file_path = "~/.local/share/icloud_for_linux/"

root_name = "icloud"
root_path = os.path.join(Path.home(), root_name)

# Inotify flags to watch
watch_flags = flags.CREATE | flags.CLOSE_WRITE | flags.MOVED_TO | \
 flags.DELETE | flags.DELETE_SELF | flags.MOVED_FROM | flags.MOVE_SELF

iNotice = INotify()

# Map of inotify watch ids
inotifyMap = {}

# Paths already watching
activeWatch = []

# Files currently being synced
lockedFiles = set()


async def syncWorker(root, node, node_path):
    """Concurrent worker to sync files"""
    if node is not None and os.path.exists(node_path) and os.path.isfile(node_path):
        # Check if the cloud file is newer
        fileTime = os.path.getmtime(node_path)
        fileTime = datetime.fromtimestamp(fileTime, timezone.utc)
        nodeTime = node.date_modified.replace(tzinfo=timezone.utc)
        if nodeTime < fileTime:
            LOGGER.debug(f"local {node_path} is newer, uploading...")
            lockedFiles.add(node_path)
            await modifyNode(node.parent, node_path)
            # upload
        elif nodeTime == fileTime:
            LOGGER.debug(f"{node_path} hasn't changed")
        else:
            LOGGER.debug(f"remote {node_path} is newer, uploading...")
            lockedFiles.add(node_path)
            await downloadNode(node, node_path)

    elif node is not None and os.path.exists(node_path) and os.path.isdir(node_path):
        # Add children to the sync queue
        coros = []
        for child in await node.get_children():
            LOGGER.debug(f"Adding to sync list: {node_path}/{child.name}")
            coros.append(syncWorker(root, child, os.path.join(node_path, child.name)))
        await asyncio.gather(*coros)

        # Check if directory is being watched and queue children for sync
        if node_path not in activeWatch:
            # If we already created a watch, don't create another
            LOGGER.debug(f"Watching: {node_path}")
            wd = iNotice.add_watch(node_path, watch_flags)
            inotifyMap[wd] = (node, node_path)
            activeWatch.append(node_path)

    elif node is not None:
        # File exists remotely but not locally
        lockedFiles.add(node_path)
        await downloadNode(node, node_path)

        if os.path.isdir(node_path):
            coros = []
            # Add children to the sync queue
            for child in await node.get_children():
                LOGGER.debug(f"Adding to sync list: {node_path}/{child.name}")
                coros.append(syncWorker(root, child, os.path.join(node_path, child.name)))
            
            await asyncio.gather(*coros)

    else:
        is_parent, node = await getParentNode(node_path, root)
        # Create the new node
        if is_parent:
            # If we couldn't find the node, create node on parent node
            lockedFiles.add(node_path)
            await createNode(node, node_path)
        else:
            # Node was found, lets run this again
            await syncWorker(root, node, node_path)

        if os.path.isdir(node_path):
            coros = []
            # Add contained files/dir to queue
            for filename in os.listdir(node_path):
                child = os.path.join(node_path, filename)
                LOGGER.debug(f"Adding to sync list: {child}")
                coros.append(syncWorker(root, None, child))
            
            await asyncio.gather(*coros)


async def getParentNode(node_path, root):
    """Get node's parent based on path"""

    # We only want to split relative path
    rel_path = str(node_path).replace(str(root_path),"")
    paths = list(filter(None, rel_path.split("/")))
    LOGGER.debug(f"Sync Worker List of paths: {paths}")

    node = root
    is_parent = False
    for path in paths:
        try:
            # Node exists online, move on to the next
            node = await node[path]
            continue
        except (KeyError, IndexError) as err:
            LOGGER.debug(f"Could not get node for {node_path}")
            is_parent = True
    return is_parent, node


async def downloadNode (node, node_path):
    """ Download node"""

    if node.type == "file":
        LOGGER.debug(f"Downloading: {node_path}")
        await node.open(node_path)
            
        # Set the correct timestamps
        if os.path.exists(node_path):
            accessTime = node.date_changed.replace(tzinfo=timezone.utc)
            modTime = node.date_modified.replace(tzinfo=timezone.utc)
            LOGGER.debug(f"mod time: {node.date_modified.strftime('%m/%d/%Y, %H:%M:%S %Z')}, access time : {node.date_changed.strftime('%m/%d/%Y, %H:%M:%S %Z')}")
            os.utime(node_path, times=(accessTime.timestamp(), modTime.timestamp()))
    else:
        LOGGER.debug(f"Creating: {node_path}")
        os.makedirs(node_path)

       
async def createNode (parent_node, file_path):
    """Create node on icloud service"""

    filename = os.path.basename(file_path)

    if os.path.isdir(file_path): 
        LOGGER.debug(f"Creating Dir: {filename}")
        await parent_node.mkdir(filename)
    else:
        # Lets skip the if file is empty, we can't upload 0 bytes files
        # We should catch it with the close write event
        if os.path.getsize(file_path) < 1:
            LOGGER.debug(f"{file_path} was size 0 skipping")
            return
        
        # Upload file
        LOGGER.debug(f"Create node Uploading new: {file_path}")
        with open(file_path, 'rb') as file_in:
            await parent_node.upload(file_in)

async def modifyNode (node, file_path):
    """Modify node on icloud service"""

    filename = os.path.basename(file_path)
    child_node = await node[filename]

    if os.path.isdir(file_path):
        LOGGER.debug(f"Renaming Dir: {child_node.name} to {filename}")
        await child_node.rename(filename)
    else:
        try:
            # Lets skip if file is empty, we can't upload 0 bytes files
            # We should catch it with the close write event
            if os.path.getsize(file_path) < 1:
                LOGGER.warn(f"{file_path} was size 0 skipping")
                return
                
            # Move original file to trash, then upload edited file
            LOGGER.debug(f"Modify node deleting old: {filename}")
            data = await child_node.delete()
            LOGGER.debug(f"Data: {data}")
            
            if data['items'][0]["status"] != "OK":
                LOGGER.error(f"Failed to delete: {filename}")
                return

            asyncio.sleep(1)

            LOGGER.debug(f"Modify node uploading new: {file_path}")
            with open(file_path, 'rb') as file_in:
                await node.upload(file_in)

        except KeyError:
            # Log error if we can't find the file
            LOGGER.error(f"Could not find the node for: {file_path}")

async def base(username, password):
    
    # Create root folder if it doesn't exist
    os.makedirs(root_path, exist_ok=True) # @todo: figure out which permission to set
    
    api = await PyiCloudService.create(username, password, cookie_directory=cookie_file_path)

    # Check if 2FA is required
    if api.requires_2fa:
        print("Two-factor authentication required.")
        code = input("Enter the code you received of one of your approved devices: ")

        success = await api.validate_2fa_code(code)
        if not success:
            print("Failed to verify security code")
            sys.exit(1)

        if not api.is_trusted_session:
            print("Session is not trusted. Requesting trust...")
            result = api.trust_session()

            if not result:
                print("Failed to request trust. You will likely be prompted for the code again in the coming weeks")
    
    try:
        LOGGER.info("Fetch root node details")
        root = await api.drive.root()

        LOGGER.debug(f"Watching: {root_path}")
        wd = iNotice.add_watch(root_path, watch_flags)
        inotifyMap[wd] = (root, root_path)
        activeWatch.append(root_path)

    except PyiCloudAPIResponseException as error:
        LOGGER.error(error)
    
    tasks = None
    task_list = []
    if len(os.listdir(root_path)) > 0:
        # First run, go through all directories and make sure all new local files are uploaded
        for filename in os.listdir(root_path):
            child = os.path.join(root_path, filename)
            LOGGER.debug(f"Adding to sync list: {child}")
            task_list.append(syncWorker(root, None, child))

    else:
        # Download all remote files if this icloud folder is empty
        for child in await root.get_children():
            LOGGER.debug(f"Adding to download list: icloud/{child.name}")
            task_list.append(syncWorker(root, child, os.path.join(root_path, child.name)))
    
    tasks = asyncio.gather(*task_list)

    # Below we handle watching and syncing deleted changes in local folder
    start_time = time.time()
    while(True):

        # If we finished with the sync tasks and it has been 10 mins, restart
        elapsed_s = time.time() - start_time
        if tasks.done():
            # Clear out locked files from previous sync
            lockedFiles.clear()

            if elapsed_s >= DATA_TIMEOUT_S:
                start_time = time.time()
                LOGGER.debug("Sync queue is empty, restarting....")
                # Add root children to the download queue
                task_list = []
                for child in await root.get_children():
                    LOGGER.debug(f"Adding to download list: icloud/{child.name}")
                    task_list.append(syncWorker(root, child, os.path.join(root_path, child.name)))
                
                tasks = asyncio.gather(*task_list)

        # Handle events of unblock after 5 mins so we can update the download queue
        # with code above
        for event in iNotice.read(timeout=0):
            wd, _, _,  filename = event
            # Get the node information
            node, node_path = inotifyMap[wd]
            await handleEvent(root, wd, flags.from_mask(event.mask), node, node_path, filename)
        
        # Sleep and let other threads run
        await asyncio.sleep(1)

    
async def handleEvent(root, wd, all_flags, node, node_path, filename):
    """ Handle an inotify event"""

    for flag in all_flags:
        LOGGER.info(f"Event occurred: {str(flag)}")
    
    # Ignore events for files we are actively syncing
    file_path = os.path.join(node_path, filename)
    if file_path in lockedFiles:
       return

    try:
        # Deletes and move from are handled immediately, everything else is added to the queue
        if any(f in all_flags for f in [flags.DELETE, flags.MOVED_FROM]):
            LOGGER.debug(f"Deleting: {(await node[filename]).name}")
            await node[filename].delete()

        elif any(f in all_flags for f in [flags.DELETE_SELF , flags.MOVE_SELF]):
            LOGGER.debug(f"Deleting: {(await node).name}")
            iNotice.rm_watch(wd)
            await node.delete()
        elif any(f in all_flags for f in [flags.CLOSE_WRITE , flags.MOVED_TO]) or \
        all(f in all_flags for f in [flags.CREATE , flags.ISDIR]):
            await syncWorker(root, None, file_path)

    except (KeyError, IndexError):
        LOGGER.debug(f"{file_path} was already removed")

    except FileNotFoundError:
        LOGGER.error(f"{file_path} not found.")


def main(args=None):
    """Main commandline entrypoint."""
    if args is None:
        args = sys.argv[1:]
    
    parser = argparse.ArgumentParser(description="Icloud Drive for linux")
    
    parser.add_argument(
        "--username",
        action="store",
        dest="username",
        default="",
        help="Apple ID to Use",
    )

    parser.add_argument(
        "-n",
        "--non-interactive",
        action="store_false",
        dest="interactive",
        default=True,
        help="Disable interactive prompts.",
    )

    command_line = parser.parse_args(args)

    username = command_line.username
    if not username:
        parser.error("No username supplied")
    
    password = get_password(username, interactive=command_line.interactive)
    if not password_exists_in_keyring(username):
        store_password_in_keyring(username, password)

    asyncio.run(base(username, password))
    

if __name__ == "__main__":
    main()