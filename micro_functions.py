import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import requests
import os
import pandas as pd
import seaborn as sb
sb.set_theme()

START_DATE = '2019-01-01'
END_DATE = '2022-07-12'
DEFAULT_DATE = dt.date.today() - dt.timedelta(396)
TODAY = dt.date.today()

class Stock:
    def __init__(self, ticker, key, adjusted=True, start=START_DATE, end=END_DATE, path=None):
        self.ticker = ticker
        self.key = key
        self.adjusted = adjusted
        self.start = start
        self.end = end
        self.path = path
        self.data = self.get_data()


    def get_data(self):
        available_data = [filename[:-4] for filename in os.listdir(self.path)
                          if not filename.startswith('0')]
        if self.ticker in available_data:
            data = pd.read_csv(f"{self.path}/{self.ticker}", index_col='t').round(2)
        else:
            endpoint = f"https://api.polygon.io/v2/aggs/ticker/{self.ticker}/range/1/day/{self.start}/{self.end}?adjusted={self.adjusted}&sort=asc&limit=50000&apiKey={self.key}"
            call = requests.get(endpoint).json()
            data = pd.DataFrame(call["results"]).round(2)
            data.index = pd.to_datetime(data['t'], unit='ms').dt.date
            data.drop(columns=['t'], inplace=True)
            self.calc_vol(data)

        return data

    def calc_vol(self, df):
        df['returns'] = np.log(df.c).diff().round(4)
        df['volatility'] = df.returns.rolling(21).std().round(4)
        df['change'] = df['c'].diff()
        df['hi_low_spread'] = ((df['h'] - df['l']) / df['o']).round(2)
        df['exp_change'] = (df.volatility * df.c.shift(1)).round(2)
        df['magnitude'] = (df.change / df.exp_change).round(2)
        df['abs_magnitude'] = np.abs(df.magnitude)
        df.dropna(inplace=True)

    def plot_return_data(self):
        start = self.data.index[0]
        end = self.data.index[-1]
        plt.hist(self.data['returns'], bins=20, edgecolor='w')
        plt.suptitle(f"Distribution of returns for {self.ticker}", fontsize=14)
        plt.title(f"From {start} to {end}", fontsize=12)
        plt.show()



def main():
    key = 'NexrgAqzDgn0PINe8qadOI_6ERpEG8wc'
    test = Stock(ticker='GOOG', key=key)
    print(test.data)
    test.plot_return_data()

if __name__ == '__main__':
    main()