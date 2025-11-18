###
### Evaluates some hedging strategies - ed2.
###

## IMPORTANT: 
## Is recommended to update all the historic databases, at least once time a 
## day (running "run_build_scripts.py"), before to execute this script. Is 
## especially important update the expired instruments status and the delivery
## price database.

import datetime
import time
import requests
import json
from pymongo import MongoClient, DESCENDING, ASCENDING


# API definitions.
hist_base_url = "https://history.deribit.com/api/v2/public/"
endpoint = f"get_last_trades_by_instrument_and_time"


## DB access.
client = MongoClient('mongodb://localhost:27017/')
db = client['deribit_btc_options']
instrument_collection = db['btc_day_inverse_options_offer']
prospec_test_collection = db['btc_day_prospective_test']


##
## Settings to evaluate hedging strategies.
##

delivery_price = 60990.00
instrument_day = "^BTC-29JUN24-"


##
## Getting the best trade opportunity.
##

doc = prospec_test_collection.find(
                            {'instrument': { '$regex': f"{instrument_day}62000-C" },
                             }).sort([('bid', -1), ('vars.YdivY', -1)]).limit(1)

doc_list = list(doc)

if doc_list:
    best_trade = doc_list[0]
else:
    print("The query returned empty.")


###
### 01. Operating without hedging
###

# the option / underlying ratio. 
s_opt_btc_ratio = 1.00

# Starts counters.
count_yield_positive = 0
count_yield_negative = 0

# Starts balance.
yield_balance = 0.00

## Deribit transactions fees (taker).
## Source: https://www.deribit.com/kb/fees
    
# BTC options trading (in BTC) fee.
fee_option_trade = min((best_trade['bid'] * 0.125), 0.0003)

# BTC options delivery fee.
fee_option_delivery = 0.00  # Is zero to day expiration options.

##
## Calculating the trade yield (in BTC).
##

call_premium = (s_opt_btc_ratio * ((best_trade['bid'] - fee_option_trade)))

if delivery_price >= best_trade['vars']['strike']:
    # The option was expired at/in the money.
    delivery_yield = s_opt_btc_ratio * ((best_trade['vars']['strike'] - (delivery_price + (fee_option_delivery * delivery_price))) / delivery_price)
else:
    # The option was expired out the money.
    delivery_yield = 0.00

# The total trade yield.
trade_yield = call_premium + delivery_yield

# Updates counters.
if trade_yield >= 0:
    count_yield_positive += 1
else:
    count_yield_negative += 1

# Updates balances:
yield_balance = yield_balance + trade_yield

# Shows the trade yield summary.
print("| instr: ", best_trade['instrument'], 
      "| bid: ", "{:.4f}".format(best_trade['bid']), 
      "| YdivY: ", "{:.4f}".format(round(float(best_trade['vars']['YdivY']), 4)), 
      "| yld_call: BTC ", round(call_premium, 3),
      f"(USD {round((call_premium * delivery_price), 3)})",
      "| yld_delivery: BTC ",  round(delivery_yield, 3),
      f"(USD {round((delivery_yield * delivery_price), 3)})",
      "| yld_trade: BTC ",  round(trade_yield, 3),
      f"(USD {round((trade_yield * delivery_price), 3)})",)

# Shows the report:
print("\n\n\n============================================")
print("01. Operating without hedging")
print("------------------------------------------")
print(f"Yield positive trades: {count_yield_positive}")
print(f"Yield negative trades: {count_yield_negative}")
print(f"Yield balance - Normalized amount: BTC {yield_balance} (USD {round((yield_balance * delivery_price), 3)})")
print("============================================\n\n\n")
input("Press Enter to continue...")


###
### 02. Operating with a short straddle
###

##
## Gets the "mirrored" put.
##

call = best_trade

params = {
          'instrument_name': f"BTC-29JUN24-{int(call['vars']['strike']) - 2000}-P",                             
          'start_timestamp': (call['unix_datetime']),        
          'count': 1,                         
          }

response = requests.get(f"{hist_base_url}{endpoint}", params=params)

if response.status_code == 200:
    data = response.json()
    put = data['result']['trades'][0]

# the option / underlying ratio. 
c_opt_btc_ratio = 1.00
p_opt_btc_ratio = 1.00

# Starts counters.
count_yield_positive = 0
count_yield_negative = 0

# Starts balance.
yield_balance = 0.00

## Deribit transactions fees (taker).
## Source: https://www.deribit.com/kb/fees

# BTC options trading (in BTC) fee.
fee_call_trade = min((call['bid'] * 0.125), 0.0003)
fee_put_trade = min((put['price'] * 0.125), 0.0003)

# BTC options delivery fee.
fee_option_delivery = 0.00  # Is zero to day expiration options.

##
## Calculating the trade yield (in BTC).
##

call_premium = (c_opt_btc_ratio * ((call['bid'] - fee_call_trade)))
put_premium = (p_opt_btc_ratio * ((put['price'] - fee_call_trade)))

# P&L Call.
if delivery_price >= call['vars']['strike']:
    # The call was expired at/in the money.
    delivery_yld_call = c_opt_btc_ratio * ((call['vars']['strike'] -  (delivery_price + (fee_option_delivery * delivery_price))) / delivery_price)
else:
    # The option was expired out the money.
    delivery_yld_call = 0.00

# P&L Put.
if delivery_price <= call['vars']['strike']:
    # The option was expired at/in the money.
    delivery_yld_put = p_opt_btc_ratio * ((delivery_price - (call['vars']['strike'] + (fee_option_delivery * delivery_price))) / delivery_price)
else:
    # The option was expired out the money.
    delivery_yld_put = 0.00

# The total trade yield.
trade_yield = call_premium + put_premium + delivery_yld_call + delivery_yld_put

# Updates counters.
if trade_yield >= 0:
    count_yield_positive += 1
else:
    count_yield_negative += 1

# Updates balances:
yield_balance = yield_balance + trade_yield

# Shows the trade yield summary.
print("SHORT CALL",
      "| instr: ", call['instrument'], 
      "| bid: ", "{:.4f}".format(call['bid']), 
      "| YdivY: ", "{:.4f}".format(round(float(call['vars']['YdivY']), 4)), 
      "| yld_call: BTC ", round(call_premium, 3),
      f"(USD {round((call_premium * delivery_price), 3)})",
      "| lss_call: BTC ",  round(delivery_yld_call, 3),
      f"(USD {round((delivery_yld_call * delivery_price), 3)})",
      "| lss_put: BTC ",  round(delivery_yld_put, 3),
      f"(USD {round((delivery_yld_put * delivery_price), 3)})",
      "| yld_trade: BTC ",  round(trade_yield, 3),
      f"(USD {round((trade_yield * delivery_price), 3)})",)
print("SHORT PUT",
      "| instr: ", put['instrument_name'], 
      "| bid: ", "{:.4f}".format(put['price']), 
      "| yld_put: BTC ", round(put_premium, 3),
      f"(USD {round((put_premium * delivery_price), 3)})",
      "| lss_put: BTC ",  round(delivery_yld_put, 3),
      f"(USD {round((delivery_yld_put * delivery_price), 3)})",
      "| yld_trade: BTC ",  round(trade_yield, 3),
      f"(USD {round((trade_yield * delivery_price), 3)})",)

# Shows the report:
print("\n\n\n============================================")
print("02. Operating with a short straddle.")
print("------------------------------------------")
print(f"Yield positive trades: {count_yield_positive}")
print(f"Yield negative trades: {count_yield_negative}")
print(f"Yield balance - Normalized amount: BTC {yield_balance} (USD {round((yield_balance * delivery_price), 3)})")
print("============================================\n\n\n")
input("Press Enter to continue...")


###
### 03. Operating with a call spread (short & long calls)
###

##
## Getting the long opportunity.
##

t_short = best_trade

l_instrument = f"{instrument_day}" + f"{int(t_short['vars']['strike']) + 500}-C"

doc_l = prospec_test_collection.find(
                            {'instrument': { '$regex': l_instrument },
                             'unix_datetime': {'$gte': t_short['unix_datetime']},
                             }).sort('bid', ASCENDING).limit(1)

doc_l_list = list(doc_l)

if doc_l_list:
    t_long = doc_l_list[0]
else:
    print("The query returned empty.")


# the option / underlying ratio. 
s_opt_btc_ratio = 1.00
l_opt_btc_ratio = 2.00

# Starts counters.
count_s_yield_positive = 0
count_s_yield_negative = 0
count_l_yield_positive = 0
count_l_yield_negative = 0

# Starts balance.
yield_s_balance = 0.00
yield_l_balance = 0.00

## Deribit transactions fees (taker).
## Source: https://www.deribit.com/kb/fees

# BTC options trading (in BTC) fee.
fee_short_trade = min((t_short['bid'] * 0.125), 0.0003)
fee_long_trade = min((t_long['bid'] * 0.125), 0.0003)

# BTC options delivery fee.
fee_option_delivery = 0.00  # Is zero to day expiration options.

##
## Calculating the trade yield (in BTC).
##

short_premium = (s_opt_btc_ratio * ((t_short['bid'] - fee_short_trade)))
long_cost = -(l_opt_btc_ratio * ((t_long['bid'] + fee_long_trade)))


#P&L Short.
if delivery_price >= t_short['vars']['strike']:
    # The option was expired at/in the money.
    delivery_s_yield = s_opt_btc_ratio * ((t_short['vars']['strike'] - (delivery_price + (fee_option_delivery * delivery_price))) / delivery_price)
else:
    # The option was expired out the money.
    delivery_s_yield = 0.00

#P&L Long.
if delivery_price >= t_long['vars']['strike']:
    # The option was expired at/in the money.
    delivery_l_yield = l_opt_btc_ratio * ((delivery_price - (t_long['vars']['strike'] + (fee_option_delivery * delivery_price))) / delivery_price)
else:
    # The option was expired out the money.
    delivery_l_yield = 0.00


# The total trade yield.
short_yield = short_premium + delivery_s_yield
long_yield = long_cost + delivery_l_yield

# Updates counters.
if short_yield >= 0:
    count_s_yield_positive += 1
else:
    count_s_yield_negative += 1

if long_yield >= 0:
    count_l_yield_positive += 1
else:
    count_l_yield_negative += 1

# Updates balances:
yield_s_balance = yield_s_balance + short_yield
yield_l_balance = yield_l_balance + long_yield

# Shows the trade yield summary.
print("--- SHORT ---",
      "| instr: ",t_short['instrument'], 
      "| bid: ", "{:.4f}".format(t_short['bid']), 
      "| YdivY: ", "{:.4f}".format(round(float(t_short['vars']['YdivY']), 4)), 
      "| yld_call: BTC ", round(short_premium, 3),
      f"(USD {round((short_premium * delivery_price), 3)})",
      "| yld_delivery: BTC ",  round(delivery_s_yield, 3),
      f"(USD {round((delivery_s_yield * delivery_price), 3)})",
      "| yld_trade: BTC ",  round(short_yield, 3),
      f"(USD {round((short_yield * delivery_price), 3)})",)

print("--- LONG ---",
      "| instr: ", t_long['instrument'], 
      "| bid: ", "{:.4f}".format(t_long['bid']), 
      "| YdivY: ", "{:.4f}".format(round(float(t_long['vars']['YdivY']), 4)), 
      "| yld_call: BTC ", round(long_cost, 3),
      f"(USD {round((long_cost * delivery_price), 3)})",
      "| yld_delivery: BTC ",  round(delivery_l_yield, 3),
      f"(USD {round((delivery_l_yield * delivery_price), 3)})",
      "| yld_trade: BTC ",  round(long_yield, 3),
      f"(USD {round((long_yield * delivery_price), 3)})",)



# Shows the report:
print("\n\n\n============================================")
print("03. Operating with a bull call spread (short & long calls)")
print("------------------------------------------")
print(f"Yield positive short: {count_s_yield_positive}")
print(f"Yield negative short: {count_s_yield_negative}")
print(f"Yield positive long: {count_l_yield_positive}")
print(f"Yield negative long: {count_l_yield_negative}")
print(f"Yield short balance: BTC {yield_s_balance} (USD {round((yield_s_balance * delivery_price), 3)})")
print(f"Yield long balance: BTC {yield_l_balance} (USD {round((yield_l_balance * delivery_price), 3)})")
print(f"Yield total: BTC {yield_s_balance + yield_l_balance} (USD {round(((yield_s_balance + yield_l_balance) * delivery_price), 3)})")
print("============================================\n\n\n")
input("Press Enter to continue...")


###
### 04. Operating with a boxed call spread (short & long calls)
###

t_short = best_trade

##
## Getting the long opportunities.
##

# LONG 1
l_instrument_1 = f"{instrument_day}" + f"{int(t_short['vars']['strike']) - 1000}-C"

doc_l_1 = prospec_test_collection.find(
                            {'instrument': { '$regex': l_instrument_1 },
                             #'unix_datetime': {'$gte': t_short['unix_datetime']},
                             }).sort('bid', ASCENDING).limit(1)

doc_l_list_1 = list(doc_l_1)

if doc_l_list_1:
    t_long_1 = doc_l_list_1[0]
else:
    print("The query to long1 returned empty.")

# LONG 2
l_instrument_2 = f"{instrument_day}" + f"{int(t_short['vars']['strike']) + 1000}-C"

doc_l_2 = prospec_test_collection.find(
                            {'instrument': { '$regex': l_instrument_2 },
                             #'unix_datetime': {'$gte': t_short['unix_datetime']},
                             }).sort('bid', ASCENDING).limit(1)

doc_l_list_2 = list(doc_l_2)

if doc_l_list_2:
    t_long_2 = doc_l_list_2[0]
else:
    print("The query to long2 returned empty.")


# the option / underlying ratio. 
s_opt_btc_ratio = 2.00
l1_opt_btc_ratio = 1.00
l2_opt_btc_ratio = 1.00

# Starts balance.
yield_s_balance = 0.00
yield_l1_balance = 0.00
yield_l2_balance = 0.00

## Deribit transactions fees (taker).
## Source: https://www.deribit.com/kb/fees

# BTC options trading (in BTC) fee.
fee_short_trade = min((t_short['bid'] * 0.125), 0.0003)
fee_long1_trade = min((t_long_1['bid'] * 0.125), 0.0003)
fee_long2_trade = min((t_long_2['bid'] * 0.125), 0.0003)

# BTC options delivery fee.
fee_option_delivery = 0.00  # Is zero to day expiration options.

##
## Calculating the trade yield (in BTC).
##

short_premium = (s_opt_btc_ratio * ((t_short['bid'] - fee_short_trade)))
long_cost_1 = -(l1_opt_btc_ratio * ((t_long_1['bid'] + fee_long1_trade)))
long_cost_2 = -(l2_opt_btc_ratio * ((t_long_2['bid'] + fee_long2_trade)))


#P&L Short.
if delivery_price >= t_short['vars']['strike']:
    # The option was expired at/in the money.
    delivery_s_yield = s_opt_btc_ratio * ((t_short['vars']['strike'] - (delivery_price + (fee_option_delivery * delivery_price))) / delivery_price)
else:
    # The option was expired out the money.
    delivery_s_yield = 0.00

#P&L Long 1.
if delivery_price >= t_long_1['vars']['strike']:
    # The option was expired at/in the money.
    delivery_l1_yield = l1_opt_btc_ratio * ((delivery_price - (t_long_1['vars']['strike'] + (fee_option_delivery * delivery_price))) / delivery_price)
else:
    # The option was expired out the money.
    delivery_l1_yield = 0.00

#P&L Long 2.
if delivery_price >= t_long_2['vars']['strike']:
    # The option was expired at/in the money.
    delivery_l2_yield = l2_opt_btc_ratio * ((delivery_price - (t_long_2['vars']['strike'] + (fee_option_delivery * delivery_price))) / delivery_price)
else:
    # The option was expired out the money.
    delivery_l2_yield = 0.00


# The total trade yield.
short_yield = short_premium + delivery_s_yield
long1_yield = long_cost_1 + delivery_l1_yield
long2_yield = long_cost_2 + delivery_l2_yield


# Updates balances:
yield_s_balance = yield_s_balance + short_yield
yield_l1_balance = yield_l1_balance + long1_yield
yield_l2_balance = yield_l2_balance + long2_yield

# Shows the trade yield summary.
print("--- SHORT ---",
      "| instr: ",t_short['instrument'], 
      "| bid: ", "{:.4f}".format(t_short['bid']), 
      "| YdivY: ", "{:.4f}".format(round(float(t_short['vars']['YdivY']), 4)), 
      "| yld_call: BTC ", round(short_premium, 3),
      f"(USD {round((short_premium * delivery_price), 3)})",
      "| yld_delivery: BTC ",  round(delivery_s_yield, 3),
      f"(USD {round((delivery_s_yield * delivery_price), 3)})",
      "| yld_trade: BTC ",  round(short_yield, 3),
      f"(USD {round((short_yield * delivery_price), 3)})",)

print("--- LONG #1 ---",
      "| instr: ", t_long_1['instrument'], 
      "| bid: ", "{:.4f}".format(t_long_1['bid']), 
      "| YdivY: ", "{:.4f}".format(round(float(t_long_1['vars']['YdivY']), 4)), 
      "| yld_call: BTC ", round(long_cost_1, 3),
      f"(USD {round((long_cost_1 * delivery_price), 3)})",
      "| yld_delivery: BTC ",  round(delivery_l1_yield, 3),
      f"(USD {round((delivery_l1_yield * delivery_price), 3)})",
      "| yld_trade: BTC ",  round(long1_yield, 3),
      f"(USD {round((long1_yield * delivery_price), 3)})",)

print("--- LONG #2 ---",
      "| instr: ", t_long_2['instrument'], 
      "| bid: ", "{:.4f}".format(t_long_2['bid']), 
      "| YdivY: ", "{:.4f}".format(round(float(t_long_2['vars']['YdivY']), 4)), 
      "| yld_call: BTC ", round(long_cost_2, 3),
      f"(USD {round((long_cost_2 * delivery_price), 3)})",
      "| yld_delivery: BTC ",  round(delivery_l2_yield, 3),
      f"(USD {round((delivery_l2_yield * delivery_price), 3)})",
      "| yld_trade: BTC ",  round(long2_yield, 3),
      f"(USD {round((long2_yield * delivery_price), 3)})",)


# Shows the report:
print("\n\n\n============================================")
print("04. Operating with a boxed call spread (short & long calls)")
print("------------------------------------------")
print(f"Yield short balance: BTC {yield_s_balance} (USD {round((yield_s_balance * delivery_price), 3)})")
print(f"Yield long balance: BTC {yield_l1_balance} (USD {round((yield_l1_balance * delivery_price), 3)})")
print(f"Yield long balance: BTC {yield_l2_balance} (USD {round((yield_l2_balance * delivery_price), 3)})")
print(f"Yield total: BTC {yield_s_balance + yield_l1_balance + yield_l2_balance} (USD {round(((yield_s_balance + yield_l1_balance + yield_l2_balance) * delivery_price), 3)})")
print("============================================\n\n\n")
input("Press Enter to continue...")
