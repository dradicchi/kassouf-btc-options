###
### Dumps and saves a full backup of Deribit MongoDB database.
###

import os
import subprocess
from datetime import datetime
import sys

# Sets the database name and base backup directory.
database_name = "deribit_btc_options"
bckp_base_dir = "/users/dradicchi/Documents/Projects/python_work/deribit_btc_options/data/"

##
## Gets the most recent backup directory.
##

subdirs = [os.path.join(bckp_base_dir, d) for d in os.listdir(bckp_base_dir) if os.path.isdir(os.path.join(bckp_base_dir, d))]

if not subdirs:
    print("No backup directories found.")
    sys.exit()
else:
    # Sort the directories by modification time in descending order
    subdirs.sort(key=os.path.getmtime, reverse=True)
    most_recent_backup_dir = subdirs[0]
    subsubdir = [os.path.join(most_recent_backup_dir, d) for d in os.listdir(most_recent_backup_dir) if os.path.isdir(os.path.join(most_recent_backup_dir, d))]
    bckp_dir = os.path.join(most_recent_backup_dir, subsubdir[0])


##
## Restores the most recent backup.
##

if most_recent_backup_dir:
    print(f"Most recent backup directory: {most_recent_backup_dir}")
        
    # Constructs the mongorestore command,
    mongorestore_cmd = [
        'mongorestore',
        '--db', database_name,
        '--drop',  # Drop the target database collections before restoring
        '--dir', bckp_dir
    ]

    # Runs the mongorestore command.
    try:
        subprocess.run(mongorestore_cmd, check=True)
        print(f"Restoration of database '{database_name}' completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while restoring the database: {e}")







