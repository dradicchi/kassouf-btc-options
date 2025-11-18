###
### Runs a ordered and not parallel sequence of python scripts to build and
### update the Deribit's BTC options database.
###

import subprocess
import os
import datetime

# Defines the main paths.
r_path = '/Users/dradicchi/Documents/Projects/python_work/deribit_btc_options/'
env_path = os.path.join(r_path, 'deribit_btc_options_env/')
scripts_path = os.path.join(r_path, 'v1')

# Sets the execution base commands.
activate_env = os.path.join(env_path, 'bin/activate')
python_exec = os.path.join(env_path,  'bin/python3')

# Gets all database building scripts in the scripts folder.
# scripts = [os.path.join(scripts_path, f) 
#            for f in os.listdir(scripts_path) 
#            if f.startswith('build_') and f.endswith('.py')]

# Sets an ordered (FIFO) list of scripts:
scripts = [
           'build_hist_btc_index_price.py',
           'build_hist_btc_iv_implied_volatility.py',
          ]

# Tries to execute each script in the defined order.
for script in scripts:

    command = f"source {activate_env} && {python_exec} {script}"

    try:
        completed_process = subprocess.run(command, 
                                           shell=True, 
                                           check=True, 
                                           text=True, 
                                           capture_output=True)

        print(f"Success! ENV: {env_path}, SCR: {script_name}.")
        print(f"Output:\n{completed_process.stdout}")

    except FileNotFoundError:
        print(f"Executable script not found! SCR: {script_name}.")

    except subprocess.CalledProcessError as e:
        print(f"Runtime error! SCR: {script_name}.")
        print(f"Error output: \nSCR: {script_name}; \nERR: {e.stderr}.")









###
### Timer flow control
###

td = datetime.datetime.now()

# Runs the daily list.
if daily and td.strftime("%H:%M:%S") == "00:00:00":
    for script in daily:
        run_script(script)

# Runs the weekly list.
if (weekly and (td.strftime("%H:%M:%S") == "00:00:00") and 
   (td.strftime("%A").lower() == "sunday")):
    for script in weekly:
        run_script(script)

# Runs the monthly list.
if monthly and (td.strftime("%H:%M:%S") == "00:00:00") and (td.day == 1):
    for script in monthly:
        run_script(script)

# Runs on-demand / every time list.
if whenever:
    for script in whenever:
        run_script(script)