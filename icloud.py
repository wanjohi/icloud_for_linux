import os
import sys
import datetime
import logging
import time
import argparse
from pathlib import Path
from pyicloud.base import PyiCloudService
from pyicloud.exceptions import PyiCloudAPIResponseException
from pyicloud.utils import store_password_in_keyring, get_password, password_exists_in_keyring
from queue import Queue, Empty
import threading
from shutil import copyfileobj
from inotify_simple import INotify, flags

LOGGER = logging.getLogger("pyicloud")

# Cookies path
cookie_file_path = ".cookies"

root_name = "icloud"
root_path = os.path.join(Path.home(), root_name)

# Download queue
downQueue = Queue()

# Inotify flags to watch
watch_flags = flags.DELETE | flags.DELETE_SELF
iNotice = INotify()

# Map of inotify watch ids
inotifyMap = {}

# Recently downloaded files to ignore inotify watches
inotifyIgnore = []

# Paths already watching
activeWatch = []

def downloadWorker():
    """ Worker to downloader files from icloud"""
    while(True):
        # Download
        node, node_path  = downQueue.get()
        nodeExist = os.path.exists(node_path)

        try:
            if node.type == "file":
                if nodeExist:
                    # Check if the cloud file is newer
                    modTime = os.path.getctime(node_path)
                    utcModTime = datetime.datetime.utcfromtimestamp(modTime)
                    if node.date_modified <= utcModTime:
                        LOGGER.debug(f"{node_path} is up to date")
                        continue

                # Lets get inotify not to trigger on this node path
                inotifyIgnore.append(node_path)

                # Download the file
                LOGGER.debug(f"Downloading: {node_path}")
                with node.open(stream=True) as response:
                    with open(node_path, 'wb') as file_out:
                        copyfileobj(response.raw, file_out)
                
                # Set the correct timestamps
                os.utime(node_path, (node.date_changed.timestamp(), node.date_modified.timestamp()))
            else:
                if node_path not in activeWatch:
                    # If we already created a watch, don't create another
                    LOGGER.debug(f"Watching: {node_path}")
                    wd = iNotice.add_watch(node_path, watch_flags)
                    inotifyMap[wd] = (node, node_path)
                    activeWatch.append(node_path)

                # Create folder if it doesn't exist
                if not nodeExist:
                    # Lets get inotify not to trigger on this node path
                    LOGGER.debug(f"Dir doesn't existing, creating: {node_path}")
                    inotifyIgnore.append(node_path)
                    os.makedirs(node_path)
                
                # Add children to the download queue
                for child in node.get_children():
                    LOGGER.debug(f"Adding to download list: {node_path}/{child.name}")
                    downQueue.put((child, os.path.join(node_path, child.name)))
        finally:
            downQueue.task_done()
            time.sleep(1)
    

def syncWorker(root):
    """ Checks if local files still exist in icloud, deletes them if not"""
    syncQueue = Queue()

    while(True):
        # Starting with root directory
        for filename in os.listdir(root_path):
            obj = os.path.join(root_path, filename)
            syncQueue.put(obj)

        # Work through all items in queue
        while (True):
            try:
                obj = syncQueue.get(False)

                # Lets make sure the whole path wasn't delete in an earlier loop
                if not os.path.exists(obj):
                    LOGGER.debug(f"Sync worker can't find: {obj}")
                    continue

                # We only want to split relative path
                rel_path = str(obj).replace(str(root_path),"")
                paths = list(filter(None, rel_path.split("/")))
                LOGGER.debug(f"Sync Worker List of paths: {paths}")

                node = root
                parentNode = None
                for path in paths:
                    try:
                        # Node exists online, move on to the next
                        parentNode = node
                        node = node[path]
                        continue
                    except (KeyError, IndexError):
                        LOGGER.info(f"Sync Worker Uploading missing node: {obj}")
                        createNode(obj, node)
                        break
                
                # Check if local file is newer
                if node is not None and os.path.isfile(obj):
                    modTime = os.path.getctime(obj)
                    utcModTime = datetime.datetime.utcfromtimestamp(modTime)
                    if node.date_modified < utcModTime:
                        # Our file is newer, upload it
                        LOGGER.info(f"Sync Worker Local file is newer: {obj}")
                        modifyNode(obj, parentNode)
                
                if os.path.isdir(obj):
                    # Add contained files/dir to queue
                    for filename in os.listdir(obj):
                        child = os.path.join(obj, filename)
                        LOGGER.info(f"Sync Worker Adding to queue: {child}")
                        syncQueue.put(child)
            except Empty:
                LOGGER.info(f"Sync queue is empty, restarting checks")
                break
            finally:
                syncQueue.task_done()
                time.sleep(1)
        
def createNode (file_path, node):

    filename = os.path.basename(file_path)

    if os.path.isdir(file_path): 
        LOGGER.debug(f"Creating Dir: {filename}")
        node.mkdir(filename)
    else:
        # Lets skip the if file is empty, we can't upload 0 bytes files
        # We should catch it with the close write event
        if os.path.getsize(file_path) < 1:
            LOGGER.debug(f"{file_path} was size 0 skipping")
            return
        
        # Upload file
        LOGGER.debug(f"Create node Uploading new: {file_path}")
        with open(file_path, 'rb') as file_in:
            node.upload(file_in)

def modifyNode (file_path, node):

    filename = os.path.basename(file_path)

    if os.path.isdir(file_path):
        LOGGER.debug(f"Renaming Dir: {node[filename].name} to {filename}")
        node[filename].rename(filename)
    else:
        try:
            # Lets skip if file is empty, we can't upload 0 bytes files
            # We should catch it with the close write event
            if os.path.getsize(file_path) < 1:
                LOGGER.debug(f"{file_path} was size 0 skipping")
                return
                
            # Move original file to trash, then upload edited file
            LOGGER.debug(f"Modify node deleting old: {filename}")
            node[filename].delete()

            time.sleep(2)

            LOGGER.debug(f"Modify node uploading new: {file_path}")
            with open(file_path, 'rb') as file_in:
                node.upload(file_in)

        except KeyError:
            # Must be a new file
            LOGGER.debug(f"Modify node old file not found, uploading: {file_path}")
            with open(file_path, 'rb') as file_in:
                node.upload(file_in)

def base(username, password):
    
    # Create root folder if it doesn't exist
    os.makedirs(root_path, exist_ok=True) # @todo: figure out which permission to set
    
    api = PyiCloudService(username, password, cookie_directory=cookie_file_path)

    # Check if 2FA is required
    if api.requires_2fa:
        print("Two-factor authentication required.")
        code = input("Enter the code you received of one of your approved devices: ")

        if not api.validate_2fa_code(code):
            print("Failed to verify security code")
            sys.exit(1)

        if not api.is_trusted_session:
            print("Session is not trusted. Requesting trust...")
            result = api.trust_session()

            if not result:
                print("Failed to request trust. You will likely be prompted for the code again in the coming weeks")
    
    try:
        LOGGER.debug("Fetch root node details")
        root = api.drive.root

        LOGGER.debug(f"Watching: {root_path}")
        wd = iNotice.add_watch(root_path, watch_flags)
        inotifyMap[wd] = (root, root_path)
        activeWatch.append(root_path)

        # Add children to list
        for child in root.get_children():
            LOGGER.debug(f"Adding to download list: icloud/{child.name}")
            downQueue.put((child, os.path.join(root_path, child.name)))

    except PyiCloudAPIResponseException as error:
        LOGGER.error(error)
    
    threads = list()
    # Add a download worker
    task = threading.Thread(target=downloadWorker, daemon=True).start()
    threads.append(task)
    # Add a sync worker
    task = threading.Thread(target=syncWorker, daemon=True, args=(root,)).start()
    threads.append(task)

    # Below we handle watching and syncing deleted changes in local folder
    while(True):
        # If we finished downloading, lets check for new files in icloud drive
        if downQueue.empty():
            # Reset the notify ignore list since we are done downloading
            inotifyIgnore.clear()

            # Add root children to the download queue
            for child in root.get_children():
                LOGGER.debug("Download queue is empty, restarting....")
                LOGGER.debug(f"Adding to download list: icloud/{child.name}")
                downQueue.put((child, os.path.join(root_path, child.name)))

        # Handle events of unblock after 15 mins so we can update the download queue
        # with code above
        for event in iNotice.read(timeout=900000, read_delay=1):
            wd, _, _,  filename = event
            # Get the node information
            node, node_path = inotifyMap[wd]

            if node_path not in inotifyIgnore:
                handleEvent(wd, flags.from_mask(event.mask), node, node_path, filename)

    
def handleEvent(wd, all_flags, node, node_path, filename):
    """ Handle an inotify event"""

    for flag in all_flags:
        LOGGER.debug(f"Event occurred: {str(flag)}")

    try:
        if flags.DELETE in all_flags:
            LOGGER.debug(f"Deleting: {node[filename].name}")
            node[filename].delete()

        elif flags.DELETE_SELF in all_flags:
            LOGGER.debug(f"Deleting: {node.name}")
            iNotice.rm_watch(wd)
            node.delete()
    except (KeyError, IndexError):
        file_path = os.path.join(node_path, filename)
        LOGGER.debug(f"{file_path} was already removed")

    except FileNotFoundError:
        LOGGER.debug(f"{file_path} not found moving on.")


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

    base(username, password)

if __name__ == "__main__":
    main()