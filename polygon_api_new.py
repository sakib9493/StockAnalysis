import time
import datetime as dt
import os
import math
import requests
import numpy as np
import pandas as pd
import openpyxl
import json
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick


START_DATE = '2019-01-01'
END_DATE = '2022-07-12'
DEFAULT_DATE = dt.date.today() - dt.timedelta(396)
TODAY = dt.date.today()

def get_tickers(key, market="stocks", type="CS", exchange='NYSE'):
    """
    returns metadeta for a specific exchange
    """
    endpoint = f"https://api.polygon.io/v3/reference/tickers?active=true&sort=ticker&order=asc&limit=1000&"
    endpoint += f"primary_exchange={exchange}&market={market}&type={type}&apiKey={key}"
    print("Downloading data...")

    call = requests.get(endpoint).json()
    tickers = pd.DataFrame(call["results"])

    count = 1
    #print(type(df))
    try:
        while call["next_url"]:
            #print(call["next_url"]+f"&apiKey={key}")
            call = requests.get(call["next_url"]+f"&apiKey={key}").json()
            temp = pd.DataFrame(call["results"])
            tickers = pd.concat([tickers, temp], ignore_index=True)
            count += 1
            print(count)
            print(tickers.tail(n=5))
            print(tickers.shape[0])
            time.sleep(15)
    except Exception:
        pass

    tickers.to_csv(f"Data/Tickers/{exchange}_{market}_{type}.csv")

    print("Completed")
    return tickers

def get_ticker_types(key):
    """
    Returns all the types of financial instruments supported by polygon.io
    with its tickers and other details
    """
    endpoint = f"https://api.polygon.io/v3/reference/tickers/types?apiKey={key}"
    call = requests.get(endpoint).json()
    ticker_types = pd.DataFrame(call["results"])
    ticker_types.to_csv(f"Data/Ticker_Types/ticker_types.csv")
    return ticker_types

def get_ticker_details(*tickers, key, path="Data/Ticker_Details"):

    isExist = os.path.exists(path)

    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)
        print("Path didn't exist. A new directory is created!")

    downloaded = 0
    skipped = 0
    tickers_skipped = []

    for ticker in tickers:
        try:
            print(f"Downloading {ticker}")
            endpoint = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={key}"
            call = requests.get(endpoint).json()
            ticker_details = pd.DataFrame(call["results"])
            ticker_details.to_csv(f"{path}/{ticker}_details.csv")
            downloaded += 1

        except Exception as e:
            print(f"{ticker} has a problem: {e}, skipping...")
            skipped += 1
            tickers_skipped.append(ticker)

        time.sleep(12)

    print("Download completed")
    print(f"Data downloaded for {downloaded} securities")
    print(f"{skipped} tickers skipped")
    if tickers_skipped:
        print(" Tickers skipped ".center(30, "="))
        for ticker in tickers_skipped:
            print(ticker)

def get_ticker_news(*tickers, key, start_date=START_DATE, path="Data/Ticker_News"):

    isExist = os.path.exists(path)

    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)
        print("Path didn't exist. A new directory is created!")

    downloaded = 0
    skipped = 0
    tickers_skipped = []

    for ticker in tickers:
        try:
            print(f"Downloading {ticker}")
            endpoint = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&published_utc.gte={start_date}&order=asc&limit=1000&sort=published_utc&apiKey={key}"
            call = requests.get(endpoint).json()
            ticker_news = pd.DataFrame(call["results"])
            downloaded += 1
            count = 1
            try:
                while call["next_url"]:
                    #print(call["next_url"]+f"&apiKey={key}")
                    call = requests.get(call["next_url"] + f"&apiKey={key}").json()
                    temp = pd.DataFrame(call["results"])
                    ticker_news = pd.concat([ticker_news, temp], ignore_index=True)
                    count += 1
                    print(count)
                    print(ticker_news.shape[0])
                    time.sleep(15)

            except Exception:
                pass
            ticker_news.to_csv(f"{path}/{ticker}_news.csv")

        except Exception as e:
            print(f"{ticker} has a problem: {e}, skipping...")
            skipped += 1
            tickers_skipped.append(ticker)

        time.sleep(12)

    print("Download completed")
    print(f"Data downloaded for {downloaded} securities")
    print(f"{skipped} tickers skipped")
    if tickers_skipped:
        print(" Tickers skipped ".center(30, "="))
        for ticker in tickers_skipped:
            print(ticker)


def get_sp(symbols=True, sector=False):
    """
    Returns S&P 500 stocks metadata
    Sectors: Communication Services, Consumer Discretionary, Consumer Staples,
    Energy, Financials, Health Care, Industrials, Information Technology, Materials,
    Real Estate, Utilities
    """
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', flavor='html5lib')
    sp = table[0]
    if sector:
        sp = sp[sp["GICS Sector"] == sector]
    if symbols:
        return sp['Symbol']
    else:
        return sp

def get_sic_code(path="Data/SIC Code List"):
    """
    Returns Standard Industrial Classification (SIC) Code List
    """
    isExist = os.path.exists(path)

    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)
        print("Path didn't exist. A new directory is created!")

    table = pd.read_html('https://www.sec.gov/corpfin/division-of-corporation-finance-standard-industrial-classification-sic-code-list', flavor='html5lib')
    sic = table[0]
    sic.to_csv(f"{path}/sic_code_list.csv")
    return sic


def get_price_data(*tickers, key, path='Data/Price_Data/Energy_S&P500', start=START_DATE, end=END_DATE, adjusted=True):
    """
    downloads and stores as csv price data for selected securities
    """
    isExist = os.path.exists(path)

    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)
        print("Path didn't exist. A new directory is created!")

    downloaded = 0
    skipped = 0
    tickers_skipped = []

    for ticker in tickers:
        try:
            print(f"Downloading {ticker}")
            endpoint = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted={adjusted}&sort=asc&limit=50000&apiKey={key}"
            call = requests.get(endpoint).json()
            price = pd.DataFrame(call["results"])
            price['t'] = pd.to_datetime(price['t'], unit='ms').dt.date
            price.to_csv(f"{path}/{ticker}.csv")
            downloaded += 1

        except Exception as e:
            print(f"{ticker} has a problem: {e}, skipping...")
            skipped += 1
            tickers_skipped.append(ticker)

        time.sleep(12)

    print("Download completed")
    print(f"Data downloaded for {downloaded} securities")
    print(f"{skipped} tickers skipped")
    if tickers_skipped:
        print(" Tickers skipped ".center(30, "="))
        for ticker in tickers_skipped:
            print(ticker)


def get_closing_prices(path='Data/Price_Data/Energy_S&P500'):
    """
    Returns file with closing prices for selected securities
    """
    files = [file for file in os.listdir(path) if not file.startswith('0')]

    closes = pd.DataFrame()

    for file in files:
        df = pd.DataFrame(pd.read_csv(f"{path}/{file}", index_col='t')['c'])
        df.rename(columns={'c': file[:-4]}, inplace=True)

        if closes.empty:
            closes = df
        else:
            closes = pd.concat([closes, df], axis=1)

    closes.to_csv(f"{path}/0-closes.csv")
    print(closes)
    return closes

def returns_from_closes(path='Data/Price_Data/Energy_S&P500', filename='0-closes.csv'):
    """
    Returns instantaneous returns for selected securities
    """
    try:
        data = pd.read_csv(f"{path}/{filename}", index_col='t')

    except Exception as e:
        print(f"There was a problem: {e}")

    return np.log(data).diff().dropna()

def get_corr(data):
    """
    Returns correlations between security returns
    """
    return data.corr()

def plot_closes(closes, relative=False):
    """
    Plot absolute or relative closes for securities
    """
    if closes.endswith('.csv'):
        closes = pd.read_csv(closes, index_col='t')
    else:
        closes = pd.read_excel(closes, index_col='t')
    if relative:
        relative_change = closes / closes.iloc[0] - 1
        relative_change.plot()
        plt.axhline(0, c='r', ls='--')
        plt.grid(axis='y')
        plt.show()
    else:
        closes.plot()
        plt.grid(axis='y')
        plt.show()

def get_return_data(*tickers, key, path='Data/Price_Data/Energy_S&P500', start=START_DATE, end=END_DATE, adjusted=True):
    """
    Saves closes and returns out to excel file named 0-returns.xlsx
    """
    isExist = os.path.exists(path)

    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)
        print("Path didn't exist. A new directory is created!")

    downloaded = 0
    skipped = 0
    tickers_skipped = []

    temp = pd.DataFrame()

    for ticker in tickers:
        try:
            print(f"Downloading {ticker}")
            endpoint = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted={adjusted}&sort=asc&limit=50000&apiKey={key}"
            temp[['t', ticker]] = pd.DataFrame(requests.get(endpoint).json()["results"])[['t', 'c']]
            temp['t'] = pd.to_datetime(temp['t'], unit='ms').dt.date
            temp.set_index('t', inplace=True)
            downloaded += 1

        except Exception as e:
            print(f"{ticker} has a problem: {e}, skipping...")
            skipped += 1
            tickers_skipped.append(ticker)

        time.sleep(12)

    data = temp
    data_instantaneous = np.log(data).diff().dropna()
    data_pct = data.pct_change()

    with pd.ExcelWriter(f'{path}/0-returns.xlsx') as writer:
        data.to_excel(writer, sheet_name='closes')
        data_instantaneous.to_excel(writer, sheet_name='returns')
        data_pct.to_excel(writer, sheet_name='pct change')

    print(f"Data retrieved and saved to 0-returns.xlsx in {path}")
    print(f"Data downloaded for {downloaded} securities")
    print(f"{skipped} tickers skipped")
    if tickers_skipped:
        print(" Tickers skipped ".center(30, "="))
        for ticker in tickers_skipped:
            print(ticker)
    return data, data_instantaneous, data_pct

def plot_performance(path='Data/Price_Data/Energy_S&P500'):
    """
    Returns figure containing relative performance of all securities in path
    """
    files = [file for file in os.listdir(path) if not file.startswith('0')]
    fig, ax = plt.subplots(math.ceil(len(files) / 4), 4, figsize=(16, 16))
    count = 0
    print(files[0][:-4])
    for row in range(math.ceil(len(files) / 4)):
        print(row)
        for column in range(4):
            print(column)
            try:
                data = pd.read_csv(f"{path}/{files[count]}", index_col='t')['c']
                data = (data / data[0] - 1) * 100
                print(data.head())
                ax[row, column].plot(data, label=files[count][:-4])
                ax[row, column].legend()
                ax[row, column].yaxis.set_major_formatter(mtick.PercentFormatter())
                ax[row, column].axhline(0, c='r', ls='--')
            except:
                pass
            count += 1
            print(count)
    plt.show()

def get_earnings(key):
    """
    Returns list of tickers for companies reporting in the next week
    """
    client = EodHistoricalData(key)
    eps = pd.DataFrame(client.get_calendar_earnings())
    symbols = []

    for row in range(len(eps)):
        if eps.earnings.iloc[row]['code'].endswith('US'):
            symbols.append(eps.earnings.iloc[row]['code'][:-3])
    print(f"There are {len(symbols)} companies reporting this week")
    return symbols

def get_dividends(*tickers, key, path = 'Data/Dividends_Data/Energy_S&P500', start=START_DATE):
    """
    Returns securities with specific ex-date
    """
    isExist = os.path.exists(path)

    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)
        print("Path didn't exist. A new directory is created!")

    downloaded = 0
    skipped = 0
    tickers_skipped = []

    for ticker in tickers:
        try:
            print(f"Downloading {ticker}")
            endpoint = f"https://api.polygon.io/v3/reference/dividends?ticker={ticker}&ex_dividend_date.gte={start}&order=asc&limit=1000&apiKey={key}"
            call = requests.get(endpoint).json()
            dividends = pd.DataFrame(call["results"])
            downloaded += 1
            count = 1
            try:
                while call["next_url"]:
                    # print(call["next_url"]+f"&apiKey={key}")
                    call = requests.get(call["next_url"] + f"&apiKey={key}").json()
                    temp = pd.DataFrame(call["results"])
                    dividends = pd.concat([dividends, temp], ignore_index=True)
                    count += 1
                    print(count)
                    print(dividends.shape[0])
                    time.sleep(15)

            except Exception:
                pass
            dividends.to_csv(f"{path}/{ticker}_div.csv")

        except Exception as e:
            print(f"{ticker} has a problem: {e}, skipping...")
            skipped += 1
            tickers_skipped.append(ticker)

        time.sleep(12)

    print("Download completed")
    print(f"Data downloaded for {downloaded} securities")
    print(f"{skipped} tickers skipped")
    if tickers_skipped:
        print(" Tickers skipped ".center(30, "="))
        for ticker in tickers_skipped:
            print(ticker)

def main():
    key = 'NexrgAqzDgn0PINe8qadOI_6ERpEG8wc'
    #key = open('api_token.txt').read()
    #print(get_tickers(key))
    #print(get_ticker_types(key))
    energy = get_sp(symbols=True, sector='Energy')
    #print(energy)
    #print(get_sic_code(path="Data/SIC Code List"))
    #print(get_ticker_details(*energy, key=key, path="Data/Ticker_Details"))
    #get_ticker_news(*energy, key=key, start_date=START_DATE, path="Data/Ticker_News")
    #get_price_data(*energy, key=key)
    #get_price_data('AAPL', key=key)
    #get_closing_prices(path='Data/Price_Data/Test')
    #print(returns_from_closes(path='Data/Price_Data/Test', filename='0-closes.csv'))
    #print(get_corr(returns_from_closes(path='Data/Price_Data/Test', filename='0-closes.csv')))
    #plot_closes(closes='Data/Price_Data/Test/0-closes.csv', relative=True)
    #returns = get_return_data(*energy, path='Data/Price_Data/Test', key=key)
    #print(returns[0])
    #plot_performance(path='Data/Price_Data/Test')
    #print(get_earnings(key))
    print(get_dividends(*energy, key=key))

if __name__ == '__main__':
    main()