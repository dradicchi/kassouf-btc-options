###
### Dumps and saves a full backup of Deribit MongoDB database.
###

import os
import subprocess
from datetime import datetime

# Sets the database name and backup directory.
database_name = "deribit_btc_options"
backup_directory = "/users/dradicchi/Documents/Projects/python_work/deribit_btc_options/data"

# Ensures the backup directory exists.
if not os.path.exists(backup_directory):
    os.makedirs(backup_directory)

# Defines the output directory for the backup.
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_dir = os.path.join(backup_directory, f"{database_name}_backup_{timestamp}")

# Ensures the output directory exists.
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Construct the mongodump command.
mongodump_cmd = [
    'mongodump',
    '--db', database_name,
    '--out', output_dir
]

# Run the mongodump command.
try:
    subprocess.run(mongodump_cmd, check=True)
    print(f"Backup of database '{database_name}' completed successfully.")
except subprocess.CalledProcessError as e:
    print(f"An error occurred while backing up the database: {e}")