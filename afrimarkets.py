import pandas as pd 
import psycopg2
import sqlalchemy
import numpy as np
import statsmodels

db_connection_string = "postgresql://fahad:589Aupgradez2BdfK@localhost:5432/africanfinance_db"
engine = sqlalchemy.create_engine(db_connection_string)

Exchange_tables = {
    "NASE": "nse_ke_daily_ohlcv",
    "BRVM": "brvm_daily_ohlcv",
    "DSE": "dse_tz_daily_ohlcv",
    "JSE": "jse_sa_daily_ohlcv"
}

def get_daily_price(exchange, symbol, start_date, end_date):
    table = Exchange_tables[exchange]
    
    if start_date.lower() == "all" and end_date.lower() == "max":
        sql = f"SELECT * FROM {table} WHERE ticker='{symbol}'"
    elif start_date.lower() == "all":
        sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND trade_date<='{end_date}'"
    elif end_date.lower() == 'max':
        sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND trade_date>='{start_date}'"
    else:
        sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND (trade_date>='{start_date}' AND trade_date<='{end_date}')"
    
    df = pd.read_sql_query(sql, engine)
    
    # Convert to datetime with format specification, then extract date
    if 'trade_date' in df.columns:
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='mixed').dt.date
    df = df.sort_values('trade_date')
    df = df.reset_index(drop=True)
    
    return df

def get_daily_return(exchange, symbol, start_date, end_date,ignore_zero_volume):
    df=get_daily_price(exchange,symbol,start_date,end_date)
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    if ignore_zero_volume==True:
        df=df[df['volume']>0]
    df['daily_return']=df['closing_price'].pct_change()
    df=df[['trade_date','ticker','company_name','daily_return']]
    return df

test=get_daily_return(exchange='NASE',symbol='TOTL',start_date='all',end_date='max',ignore_zero_volume=True)
print(test)