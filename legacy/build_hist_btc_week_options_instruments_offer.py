###
### Builds the BTC week inverse options offer database.
###

from datetime import datetime
import requests
import json
from pymongo import MongoClient, DESCENDING, ASCENDING


# API definitions.
hist_base_url = "https://history.deribit.com/api/v2/public/" # Expired options.
prod_base_url = "https://deribit.com/api/v2/public/" # Active options.
endpoint = f"get_instruments"

# DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
collection = db['btc_week_inverse_options_offer']

# Builds the collection
if collection.find_one() is None:
    collection.create_index([('expiration_datetime', DESCENDING)])
    collection.create_index([('expiration_datetime', ASCENDING)])
    collection.create_index([('instrument_id', ASCENDING)])


##
## Handling expired instruments
##

# Defines query parameters to expired instruments.
hist_params = {
               'currency': "BTC",
               'kind': "option",
               'expired': 'true',
               }

# Gets the expired instrument data.
hist_response = requests.get(f"{hist_base_url}{endpoint}", params=hist_params)

# Iterates expired instruments.
if hist_response.status_code == 200:
    hist_data = hist_response.json()
    hist_instrument_list = hist_data['result']
    h_filtered_items = (instrument 
                        for instrument in hist_instrument_list 
                        if (instrument['settlement_period'] == "week" and
                            instrument['creation_timestamp'] >= 1483228800000)) 

    # A simple counter.
    c_exp = 0

    for item in h_filtered_items:

        query = {
                 'instrument_id': int(item['instrument_id']),
                 'is_active': False,
                }

        # To test if the expired instrument already was processed.
        is_exp_doc_stored = collection.find_one(query)

        if is_exp_doc_stored is None:

            creation_dt = datetime.fromtimestamp(
                                (int(item['creation_timestamp']) / 1000.0))
            expiration_dt = datetime.fromtimestamp(
                                (int(item['expiration_timestamp']) / 1000.0))

            new_data = {
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

            # Updates or stores the instrument data.
            filter = {'instrument_id': int(item['instrument_id']),}
            collection.update_one(filter, {'$set': new_data}, upsert=True)

            print(item['instrument_id'], 
                  item['instrument_name'], 
                  item['is_active'])
        
            c_exp += 1


##
## Handling active instruments
##

# Defines query parameters to active instruments.
prod_params = {
               'currency': "BTC",
               'kind': "option",
               'expired': 'false',
               }

# Gets the active instrument data.
prod_response = requests.get(f"{prod_base_url}{endpoint}", params=prod_params)

# Iterates active instruments.
if prod_response.status_code == 200:
    prod_data = prod_response.json()
    prod_instrument_list = prod_data['result']
    p_filtered_items = (instrument 
                        for instrument in prod_instrument_list 
                        if (instrument['settlement_period'] == "week" and
                            instrument['creation_timestamp'] >= 1483228800000))

    # A simple counter.
    c_act = 0

    for item in p_filtered_items:

        query = {
                 'instrument_id': int(item['instrument_id']),
                }

        # To test if the active instrument already was processed.
        is_act_doc_stored = collection.find_one(query)

        if is_act_doc_stored is None:

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

            # Stores the new active instrument data.
            collection.insert_one(document)

            print(item['instrument_id'], 
                  item['instrument_name'], 
                  item['is_active'])
        
            c_act += 1

print(f"Updated/Created expired docs: {c_exp} | Created active docs: {c_act}.")







