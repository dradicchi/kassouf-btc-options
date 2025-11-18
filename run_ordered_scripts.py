###
### Runs a ordered and not parallel sequence of python scripts to build and
### update the Deribit's BTC options database.
###

import subprocess
import os


# Defines the main paths.
r_path = '/Users/dradicchi/Documents/Projects/python_work/deribit_btc_options/'
env_path = os.path.join(r_path, 'deribit_btc_options_env/')
scripts_path = os.path.join(r_path, 'v1')

# Sets the execution base commands.
activate_env = os.path.join(env_path, 'bin/activate')
python_exec = 'python3'

# Gets all database building scripts in the scripts folder.
# scripts = [os.path.join(scripts_path, f) 
#            for f in os.listdir(scripts_path) 
#            if f.startswith('build_') and f.endswith('.py')]

# Sets an ordered (FIFO) list of scripts:
scripts = [
           # Build scripts - To create and update databases.
           'build_hist_btc_index_price_5min.py',
           'build_hist_btc_iv_implied_volatility_5min.py',
           'build_hist_btc_delivery_price_daily.py',
           'build_hist_btc_daily_avg_index_price.py',
           'build_hist_btc_hourly_avg_index_price.py',
           'build_hist_btc_inverse_options_offering.py',
           'build_hist_btc_options_trades_5min.py',

           # Calculation scripts - To calculate params and updates databases.
           'calc_daily_mov_avg_btc_index_prices.py',
           'calc_hourly_mov_avg_btc_index_prices.py',
           'calc_e1e2_daily_btc_avg_index_price.py',
           'calc_e1e2_hourly_btc_avg_index_price.py',
           'calc_inv_t_btc_options_trades.py',
           'calc_zs_btc_options_trades.py',

           # Filling script.
           'fill_instr_data_btc_options_trades.py',
           'fill_e1e2_data_btc_options_trades.py',

           # Modeling script.
           'model_k_per_instrument_trades.py',
          ]

# Tries to execute each script in the defined order.
for script in scripts:

    command = (f"source {activate_env} && {python_exec}"+ 
               f" {os.path.join(scripts_path, script)}")

    try:
        completed_process = subprocess.run(command, 
                                           shell=True, 
                                           check=True, 
                                           text=True, 
                                           capture_output=True)

        print(f"Success! ENV: {env_path}, SCR: {script}.")
        print(f"Output:\n{completed_process.stdout}")

    except FileNotFoundError:
        print(f"Executable script not found! SCR: {script}.")

    except subprocess.CalledProcessError as e:
        print(f"Runtime error! SCR: {script}.")
        print(f"Error output: \nSCR: {script}; \nERR: {e.stderr}.")





