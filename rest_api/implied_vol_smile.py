import requests
import pandas as pd
import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from itertools import repeat
from tqdm import tqdm

from credentials import api_key
from general_tools import time_convert


url = 'https://us.market-api.kaiko.io/v2/data/analytics.v2/implied_volatility_smile?base=btc&quote=usd&exchange=drbt' \
      '&time=2022-09-20T16:15:00.000Z&expiry=2022-12-30T00:00:00.000Z&strikes=10000,15000,20000,25000,30000,35000,40000'

res = requests.get(url, headers={'X-Api-Key': api_key}).json()
res = pd.DataFrame(res['implied_volatilities'])
res = res.sort_values('strike')

import matplotlib.pyplot as plt

plt.plot(res['strike'], res['implied_volatility'])

plt.show()

if __name__ == '__main__':
    ...
