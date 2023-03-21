@author: Victor

[disclaimer] These are some templates of Kaiko REST API that the auther uses for himself. Kaiko does not provide any 
templates for
REST API. Please modify them to your needs. If you have any questions, please contact Kaiko support. All the potential
problems caused by using these scripts are the responsibility of the user, including but not limited to the loss of
precision in using Pandas etc. 

# Kaiko API documentation:
https://docs.kaiko.com/

# Templates

Here 3 basic templates are shown: 1. OHLCV Data, 2. Trade Data, 3. All Instruments List

# Steps

1. In credentials.py, either add your API key directly into the file, or set the environment variable KAIKO_API_KEY
   in your system.
2. Open the scripts and try to modify the parameters at the end to get the data you want - by default they will create
   a csv file in the same folder to store the result.

# Recommendations
1. Install Python 3.11 or 3.10
2. Install the requirements: pip install -r requirements.txt
3. For OHLCV data, there is usually no need to split the dates into smaller chunks because the data is not that large (
   rarely beyond 1GB), but for trade data, it is recommended to split the dates into smaller chunks (in one hour for
   example), and the number of exchanges and pairs should not be too large.
4. Be careful of the computer loading. Using concurrent usually requires more memory and CPU usage.


# Estimated Time

Usually: 
1. 2 months OHLCV data for Coinbase and Kraken for btc-usd and eth-usd with 1 minute interval takes about
   4 seconds to download. (36 MB). By splitting the dates into smaller chunks and use multi-threading, it can be even
   much faster.
2. 1 month Trade data for Coinbase and Kraken for btc-usd and eth-usd takes about 100 seconds to download. (1.5 GB).
