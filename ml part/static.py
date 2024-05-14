from psycopg2 import connect

import yfinance as yf
import pandas as pd

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

def insert_data_table(df_data):
    with connect(database="kants", user='postgres', password='kants123!', host='34.64.47.191') as conn:
        with conn.cursor() as cur:
            for i in range(len(df_data)):
                row = df_data.iloc[i,:].values
                ticker = str(row[0])
                sector = row[1]
                industry = row[2]
                desc = row[3]
                cur.execute("INSERT INTO static(ticker, sector, industry, descr) VALUES(%s, %s, %s, %s)",
                           (ticker, sector, industry, desc))
            conn.commit()
            cur.close

if __name__ == "__main__":
    # replace this part (load tickers from DB table target)
    tickers = get_target_table()
    tickers = [ticker + '.KS' for ticker in tickers]

    static_columns = ['sector', 'industry', 'longBusinessSummary']

    df_static = pd.DataFrame()

    for ticker in tickers:
        print(ticker)
        df_static_yf = yf.Ticker(ticker)
        df_static_add = pd.DataFrame([dict([(key, value) for key, value in df_static_yf.info.items() if key in static_columns])])
        df_static_add['ticker'] = ticker[:ticker.find('.')]

        df_static = pd.concat([df_static, df_static_add], axis=0)

    df_static.columns = df_static.columns.str.lower()

    df_static = df_static.rename(columns={'longbusinesssummary':'desc'})[['ticker', 'sector', 'industry', 'desc']]
    df_static['ticker'] = df_static['ticker'].astype('str')

    insert_data_table(df_static)
    """
    store the df_static to DB table (kants.static)
    """

    print(df_static)
    print('Done')