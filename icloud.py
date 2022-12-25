import os
import sys
from datetime import datetime, timezone
import logging
import time
import argparse
from pathlib import Path
from pyicloud.base import PyiCloudService
from pyicloud.exceptions import PyiCloudAPIResponseException
from pyicloud.utils import store_password_in_keyring, get_password, password_exists_in_keyring
from queue import Queue
import threading
from shutil import copyfileobj
from inotify_simple import INotify, flags

# create console handler and set level to debug
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# Use the pyicloud logger
LOGGER = logging.getLogger()

# add ch to logger
LOGGER.addHandler(ch)
LOGGER.setLevel(logging.INFO)


# Cookies path
cookie_file_path = ".cookies"

root_name = "icloud"
root_path = os.path.join(Path.home(), root_name)

# Download queue
downQueue = Queue()

# Sync queue
syncQueue = Queue()

# Inotify flags to watch
watch_flags = flags.DELETE | flags.DELETE_SELF | flags.MOVED_FROM | flags.MOVE_SELF
iNotice = INotify()

# Map of inotify watch ids
inotifyMap = {}

# Paths already watching
activeWatch = []

# Number of threads for each worker
maxThreads = 5

# Files currently being worked on
lock = threading.RLock()
lockedFiles = []

# Lock to avoid concurrent uploads
uploadlock = threading.RLock()

def downloadWorker():
    """ Worker to downloader files from icloud"""
    while(True):
        # Download
        node, node_path  = downQueue.get()
        nodeExist = os.path.exists(node_path)
        gotFileLock = False

        try:
            # Lock the file since we are working on it
            with lock:
                if node_path not in lockedFiles:
                    lockedFiles.append(node_path)
                    gotFileLock = True
                else:
                    # Someone is already here, move on
                    continue

            if node.type == "file":
                if nodeExist:
                    # Check if the cloud file is newer
                    fileTime = os.path.getmtime(node_path)
                    fileTime = datetime.fromtimestamp(fileTime, timezone.utc)
                    nodeTime = node.date_modified.replace(tzinfo=timezone.utc)
                    if nodeTime <= fileTime:
                        LOGGER.debug(f"{node_path} is up to date")
                        continue

                # Download the file
                LOGGER.info(f"Downloading: {node_path}")

                with node.open(stream=True) as response:
                    with open(node_path, 'wb') as file_out:
                        copyfileobj(response.raw, file_out)
                
                # Set the correct timestamps
                accessTime = node.date_changed.replace(tzinfo=timezone.utc)
                modTime = node.date_modified.replace(tzinfo=timezone.utc)
                LOGGER.debug(f"mod time: {node.date_modified.strftime('%m/%d/%Y, %H:%M:%S %Z')}, access time : {node.date_changed.strftime('%m/%d/%Y, %H:%M:%S %Z')}")
                os.utime(node_path, times=(accessTime.timestamp(), modTime.timestamp()))

            else:
                # Create folder if it doesn't exist
                if not nodeExist:
                    # Lets get inotify not to trigger on this node path
                    LOGGER.info(f"Dir doesn't existing, creating: {node_path}")
                    os.makedirs(node_path)
                
                if node_path not in activeWatch:
                    # If we already created a watch, don't create another
                    LOGGER.debug(f"Watching: {node_path}")
                    wd = iNotice.add_watch(node_path, watch_flags)
                    inotifyMap[wd] = (node, node_path)
                    activeWatch.append(node_path)
                
                # Add children to the download queue
                for child in node.get_children():
                    LOGGER.debug(f"Adding to download list: {node_path}/{child.name}")
                    downQueue.put((child, os.path.join(node_path, child.name)))
        finally:
            downQueue.task_done()
            # Remove lock
            with lock:
                if gotFileLock:
                    lockedFiles.remove(node_path)
            time.sleep(1)
    

def syncWorker(root):
    """ Checks if local files still exist in icloud, deletes them if not"""

    # Work through all items in queue
    while (True):
        try:
            obj = syncQueue.get()
            gotFileLock = False

            # Lock the file since we are working on it
            with lock:
                if obj not in lockedFiles:
                    lockedFiles.append(obj)
                    gotFileLock = True
                else:
                    # Someone is already here, move on
                    continue

            # Lets make sure the whole path wasn't delete in an earlier loop
            if not os.path.exists(obj):
                LOGGER.warn(f"Sync worker can't find: {obj}")
                continue

            # We only want to split relative path
            rel_path = str(obj).replace(str(root_path),"")
            paths = list(filter(None, rel_path.split("/")))
            LOGGER.debug(f"Sync Worker List of paths: {paths}")

            node = root
            parentNode = None
            newNode = False
            for path in paths:
                try:
                    # Node exists online, move on to the next
                    parentNode = node
                    node = node[path]
                    continue
                except (KeyError, IndexError):
                    LOGGER.info(f"Sync Worker Uploading missing node: {obj}")
                    with uploadlock:
                        createNode(obj, node)
                    newNode = True
                    break
            
            # Check if local file is newer
            if not newNode and node is not None and os.path.isfile(obj):
                fileTime = os.path.getmtime(obj)
                fileTime = datetime.fromtimestamp(fileTime, timezone.utc)
                nodeTime = node.date_modified.replace(tzinfo=timezone.utc)
            
                if nodeTime < fileTime:
                    # Our file is newer, upload it
                    LOGGER.info(f"Sync Worker Local file is newer: {obj}")
                    LOGGER.debug(f"node time: {nodeTime.strftime('%m/%d/%Y, %H:%M:%S %Z')}, local file : {fileTime.strftime('%m/%d/%Y, %H:%M:%S %Z')}")
                    with uploadlock:
                        modifyNode(obj, parentNode)
            
            if os.path.isdir(obj):
                # Add contained files/dir to queue
                for filename in os.listdir(obj):
                    child = os.path.join(obj, filename)
                    LOGGER.debug(f"Sync Worker Adding to queue: {child}")
                    syncQueue.put(child)

        except PyiCloudAPIResponseException:
            LOGGER.info(f"Failed syncing: {obj}")
        finally:
            syncQueue.task_done()
            # Remove lock
            with lock:
                if gotFileLock:
                    lockedFiles.remove(obj)
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
                LOGGER.warn(f"{file_path} was size 0 skipping")
                return
                
            # Move original file to trash, then upload edited file
            LOGGER.debug(f"Modify node deleting old: {filename}")
            data = node[filename].delete()
            LOGGER.debug(f"Data: {data}")
            
            if data['items'][0]["status"] != "OK":
                LOGGER.error(f"Failed to delete: {filename}")
                return

            time.sleep(1)

            LOGGER.debug(f"Modify node uploading new: {file_path}")
            with open(file_path, 'rb') as file_in:
                node.upload(file_in)

        except KeyError:
            # Log error if we can't find the file
            LOGGER.error(f"Could not find the node for: {file_path}")

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
        LOGGER.info("Fetch root node details")
        root = api.drive.root

        LOGGER.debug(f"Watching: {root_path}")
        wd = iNotice.add_watch(root_path, watch_flags)
        inotifyMap[wd] = (root, root_path)
        activeWatch.append(root_path)

        # Set up the download queue
        for child in root.get_children():
            LOGGER.debug(f"Adding to download list: icloud/{child.name}")
            downQueue.put((child, os.path.join(root_path, child.name)))
        
        # Set up the sync queue
        for filename in os.listdir(root_path):
            obj = os.path.join(root_path, filename)
            syncQueue.put(obj)

    except PyiCloudAPIResponseException as error:
        LOGGER.error(error)
    
    threads = list()
    
    for _ in range(maxThreads):
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
            LOGGER.debug("Download queue is empty, restarting....")

            # Add root children to the download queue
            for child in root.get_children():
                LOGGER.debug(f"Adding to download list: icloud/{child.name}")
                downQueue.put((child, os.path.join(root_path, child.name)))

        # If we finished with the sync queue restart it
        if syncQueue.empty():
            LOGGER.debug("Sync queue is empty, restarting....")
            for filename in os.listdir(root_path):
                obj = os.path.join(root_path, filename)
                syncQueue.put(obj)

        # Handle events of unblock after 5 mins so we can update the download queue
        # with code above
        for event in iNotice.read(timeout=300000):
            wd, _, _,  filename = event
            # Get the node information
            node, node_path = inotifyMap[wd]
            handleEvent(wd, flags.from_mask(event.mask), node, node_path, filename)

    
def handleEvent(wd, all_flags, node, node_path, filename):
    """ Handle an inotify event"""

    for flag in all_flags:
        LOGGER.info(f"Event occurred: {str(flag)}")

    try:
        if any(f in all_flags for f in [flags.DELETE, flags.MOVED_FROM]):
            LOGGER.debug(f"Deleting: {node[filename].name}")
            node[filename].delete()

        elif any(f in all_flags for f in [flags.DELETE_SELF , flags.MOVE_SELF]):
            LOGGER.debug(f"Deleting: {node.name}")
            iNotice.rm_watch(wd)
            node.delete()
    except (KeyError, IndexError):
        file_path = os.path.join(node_path, filename)
        LOGGER.debug(f"{file_path} was already removed")

    except FileNotFoundError:
        LOGGER.error(f"{file_path} not found moving on.")


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