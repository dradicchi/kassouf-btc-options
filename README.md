# kassouf-btc

**Experimental application of Thorp-Kassouf option models to Bitcoin data**

---

## Introduction

Since early 2024, I have been developing a fair-price model for European call and put options on Bitcoin traded on Deribit. I plan to build distinct models for calls and puts across daily, weekly, and monthly expiration cycles, totaling six separate models. Currently, I am focusing on daily call contracts, the most liquid product. The goal is to use these models in arbitrage operations (mainly short straddles), identifying moments of significant overpricing.

The project is inspired by Edward Thorp's *Beat the Market* (1967) and Sheen Kassouf's paper *An Econometric Model for Option Price with Implications for Investors Expectations and Audacity* (1969). While dated and focused on warrant arbitrage, I find this approach practical for initial exploration, limiting the mathematical requirements to basic statistics and linear regression. Thorp admired G. Polya and the principles of plausible reasoning, which guide my pursuit of simple but powerful solutions to complex problems.

---

## Data Collection from Deribit

Deribit provides European call and put options on Bitcoin with daily, weekly, and monthly expirations, with high liquidity. Contracts are priced in BTC (units of 0.0001 BTC) and can be traded fractionally (0.1).  

Deribit offers a real-time API (HTTP/WebSocket) and a legacy historical API. Historical data collection scripts are prefixed with `build_`, and each script targets a specific data type. MongoDB is used for storage, with one collection per object. Historical data collection generally starts from **2017-01-01**.

---

## Constructed Data Series

### BTC Reference Price

- Interval: 5 min  
- Unit: USD  
- Script: `build_hist_btc_index_price_5min.py`  
- Collection: `btc_index_price_5min`  

### Implied Volatility Index (IV)

- Interval: 5 min  
- Unit: n/a  
- Script: `build_hist_btc_iv_implied_volatility_5min.py`  
- Collection: `btc_iv_implied_volatility_5min`  

### BTC Delivery Price

- Interval: 1 day  
- Unit: USD  
- Script: `build_hist_btc_delivery_price_daily.py`  
- Collection: `btc_delivery_price_daily`  

### Daily Average BTC Price

- Interval: 1 day  
- Unit: USD  
- Script: `build_hist_btc_daily_avg_index_price.py`  
- Collection: `btc_avg_index_price_daily`  

### Hourly Average BTC Price

- Interval: 1 hour  
- Unit: USD  
- Script: `build_hist_btc_hourly_avg_index_price.py`  
- Collection: `btc_avg_index_price_hourly`  

---

## Other Primary Data

### Historical Supply of Inverse BTC Option Contracts

- Start: 2017-01-01, 03:00 GMT  
- Script: `build_hist_btc_inverse_options_offering.py`  
- Collection: `btc_inverse_options_offering`  

### BTC Option Trade History

- Start: 2017-01-01, 03:00 GMT  
- Script: `build_hist_btc_options_trades_5min.py`  
- Collection: `btc_trade_history_5min`  

---

## Notes

- Python is used for numerical processing with libraries such as NumPy, Pandas, SciPy, and Statsmodels.  
- MongoDB stores all processed data for ease of computation.  
- The code is primarily imperative, with specialized scripts, and can be refactored for efficiency.  
- This is a work in progress (WIP), focused on modeling and generating insights for short-straddle arbitrage strategies.

---

## Original Kassouf Paper

[Kassouf, “An Econometric Model for Option Price with Implications for Investors Expectations and Audacity,” 1969](https://www.jstor.org/stable/1910443)

---

## Next Steps

- Explore strategies to anticipate when a contract will fit the model.  
- Improve code structure and optimization.  
- Extend testing and backtesting.
