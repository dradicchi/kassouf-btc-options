###
### Completes / Fills non-coleted instrument data.
###

## NOTES:
## The 'datetime' fields from 'btc_inverse_options_offering' collection are 
## relative to 'São Paulo -3 GMT local time'. As the Deribit's BTC options 
## (daily, weekly and monthly) contracts expires at 8:00 AM GMT (5:00 AM at 
## São Paulo -3 GMT local time). Unix time are in miliseconds.

from datetime import datetime
import requests
import json
from pymongo import MongoClient, DESCENDING, ASCENDING


# API definitions.
hist_base_url = "https://history.deribit.com/api/v2/public/" # Expired options.
endpoint = f"get_instrument"

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
instr_collection = db['btc_inverse_options_offering']
trades_collection = db['btc_trade_history_5min']

instruments = trades_collection.distinct("instrument_name")

if instruments:

    for instr in instruments:

        instr_check = instr_collection.find_one(
                                                {"instrument_name": instr}, 
                                                {"instrument_id": 1},
                                               )
        if instr_check is None:

            api_params = {
                           'instrument_name': instr,
                         }

            api_response = requests.get(f"{hist_base_url}{endpoint}", params=api_params)

            if api_response.status_code == 200:
                api_data = api_response.json()
                item = api_data['result']

                creation_dt = datetime.fromtimestamp(
                                    (int(item['creation_timestamp']) / 1000.0))
                expiration_dt = datetime.fromtimestamp(
                                    (int(item['expiration_timestamp']) / 1000.0))

                document = {
                    'instrument_id': int(item['instrument_id']),
                    'instrument_name': item['instrument_name'],
                    'is_active': item['is_active'],
                    'strike': float(item['strike']),
                    'tick_size': float(item['tick_size']),
                    'settlement_period': item['settlement_period'],
                    'creation_datetime': creation_dt,
                    'expiration_datetime': expiration_dt,
                    'creation_unix_timestamp': int(item['creation_timestamp']),
                    'expiration_unix_timestamp': int(item['expiration_timestamp']),
                    'base_currency': item['base_currency'],
                    'counter_currency': item['counter_currency'],
                    'quote_currency': item['quote_currency'],
                    'settlement_currency': item['settlement_currency'],
                    'price_index': item['price_index'],
                    'contract_size': int(item['contract_size']),
                    'min_trade_amount': float(item['min_trade_amount']),
                    'kind': item['kind'],
                    'option_type': item['option_type'],
                    'maker_commission': float(item['maker_commission']),
                    'taker_commission': float(item['taker_commission']),
                    }


                instr_collection.insert_one(document)

                print(item['instrument_id'], 
                      item['instrument_name'], 
                      item['is_active'])

            else:
                print(f"There is an API error!")

        else:
            print(f"{instr_check["instrument_id"]} already stored.")



