from psycopg2 import connect

import yfinance as yf
import pandas as pd

import warnings
warnings.filterwarnings('ignore')

"""
set the DB pipeline
"""

def get_target_table() -> list:
    with connect(database="kants", user='postgres', password='kants123!', host='34.64.47.191') as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM target")
            target_stocks = cur.fetchall()        
            cur.close    
    stock_names = []
    for stock in target_stocks:
        stock_names.append(stock[0])    
    return stock_names

def insert_dy_data_table(df_data):
    with connect(database="kants", user='postgres', password='kants123!', host='34.64.47.191') as conn:
        with conn.cursor() as cur:
            for i in range(len(df_data)):
                row = df_data.iloc[i,:].values
                date = row[0]
                ticker = row[1]
                revenue = row[2]
                cur.execute("INSERT INTO dynamic(date, ticker, revenue) VALUES(%s, %s, %s)",
                           (date, ticker, revenue))
            conn.commit()
            cur.close

if __name__ == "__main__":
    # replace this part (load tickers from DB table target)
    tickers = get_target_table()
    tickers = [ticker + '.KS' for ticker in tickers]

    df_dynamic = pd.DataFrame()

    for i, ticker in enumerate(tickers):
        df_dynamic_yf = yf.Ticker(ticker)
        df_dynamic_add = df_dynamic_yf.financials

        if i == 0:
            add_date = df_dynamic_add.columns[0]
            print("Extracting Dynamic data on", add_date.date())

        print(ticker)
        df_dynamic_add = df_dynamic_add[add_date]
        df_dynamic_add.name = ticker[:ticker.find('.')]

        df_dynamic = pd.concat([df_dynamic, df_dynamic_add], axis=1)

    df_dynamic = df_dynamic.T.reset_index()[['index', 'Total Revenue']]
    df_dynamic['date'] = add_date

    df_dynamic = df_dynamic.rename(columns={'index':'ticker', 'Total Revenue':'revenue'})[['date', 'ticker', 'revenue']]
    insert_dy_data_table(df_dynamic)

    print(df_dynamic)
    print('Done')