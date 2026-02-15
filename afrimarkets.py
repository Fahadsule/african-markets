import pandas as pd 
import psycopg2
import sqlalchemy
import numpy as np
import statsmodels.api as sm

db_connection_string = "postgresql://fahad:589Aupgradez2BdfK@localhost:5432/africanfinance_db"
engine = sqlalchemy.create_engine(db_connection_string)

Exchange_tables = {
    "NASE": "nse_ke_daily_ohlcv",
    "BRVM": "brvm_daily_ohlcv",
    "DSE": "dse_tz_daily_ohlcv",
    "JSE": "jse_sa_daily_ohlcv"
}

Index_tables={"NASE":"nse_ke_daily_ohlcv",
              "BRVM":"brvm_indices_daily_ohlcv",
              "DSE": '"DSEI"',
              "JSE":"jse_indices_daily_ohlcv"}

Index_symbols={"NASE":"^NASI",
               "BRVM":"BRVMPG",
               "JSE":"^J203.JO"}

distribution_tables={"NASE":"nse_corporate_actions_distributions"}

ticker_changes_tables={"NASE":"nse_ticker_change"}

Dividend_table={"NASE":"nse_corporate_actions_dividends",
                "DSE":"dse_corporate_actions_dividends"}

splits_tables={"NASE":"nse_corporate_actions_splits",
               "DSE":'dse_corporate_actions_splits'}

bonus_tables={"NASE":"nse_corporate_actions_bonus",
              "DSE":"dse_corporate_actions_bonus"}

rights_tables={"NASE":"nse_corporate_actions_rights",
               "DSE":"dse_corporate_actions_rights"}

index_available=['NASE','DSE','JSE','BRVM']

Dividend_available=['NASE','DSE']

distribution_available=['NASE']

bonus_available=['NASE','DSE']

splits_available=['NASE','DSE']

exchanges_available=['NASE','JSE','DSE','BRVM']

rights_available=['NASE','DSE']

ticker_changes=['NASE']

def get_daily_price(exchange, symbol, start_date, end_date):
    if exchange in exchanges_available:
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
    else:
        print("WE CURRENTLY LACK DATA ON THAT EXCHANGE....")
        return None

def get_no_dividend_daily_return(exchange, symbol, start_date, end_date,ignore_zero_volume):
    df=get_daily_price(exchange,symbol,start_date,end_date)
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    if ignore_zero_volume==True:
        df=df[df['volume']>0]
    df['daily_return']=df['closing_price'].pct_change()
    df=df[['trade_date','ticker','company_name','daily_return']]
    return df

def get_log_daily_return(exchange, symbol,start_date,end_date,ignore_zero_volume):
    df=get_daily_price(exchange,symbol,start_date,end_date)
    df['volume']=pd.to_numeric(df['volume'],errors='coerce')
    if ignore_zero_volume==True:
        df=df[df['volume']>0]
    df['log_return']=np.log(df['closing_price']/df['closing_price'].shift(1))
    df=df[['trade_date','ticker','company_name','log_return']]
    return df


def get_index_price(exchange,start_date, end_date):
    if exchange in index_available:
        table = Index_tables[exchange]
        if exchange=="DSE":
            if start_date.lower() == "all" and end_date.lower() == "max":
                sql = f"SELECT * FROM {table}"
            elif start_date.lower() == "all":
                sql = f"SELECT * FROM {table} WHERE trade_date<='{end_date}'"
            elif end_date.lower() == 'max':
                sql = f"SELECT * FROM {table} WHERE trade_date>='{start_date}'"
            else:
                sql = f"SELECT * FROM {table} WHERE trade_date>='{start_date}' AND trade_date<='{end_date}'"
            df=pd.read_sql_query(sql, engine)
            # Convert to datetime with format specification, then extract date
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='mixed').dt.date
            df = df.sort_values('trade_date')
            df['ticker']='DSEI'
            df = df.reset_index(drop=True)
            return df
        else:
            symbol=Index_symbols[exchange]

    
            if start_date.lower() == "all" and end_date.lower() == "max":
                sql = f"SELECT * FROM {table} WHERE ticker='{symbol}'"
            elif start_date.lower() == "all":
                sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND trade_date<='{end_date}'"
            elif end_date.lower() == 'max':
                sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND trade_date>='{start_date}'"
            else:
                sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND (trade_date>='{start_date}' AND trade_date<='{end_date}')"
    
            df = pd.read_sql_query(sql, engine)
            if exchange=='JSE':
                df=df.drop(columns=['volume'])
            elif exchange=='NASE':
                df=df.drop(columns=['volume'])
        
    
            # Convert to datetime with format specification, then extract date
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='mixed').dt.date
            df = df.sort_values('trade_date')
            df = df.reset_index(drop=True)
    
            return df
    else:
        print("WE CURRENTLY LACK INDEX DATA FOR THIS EXCHANGE...")
        return None
    
def get_index_return(exchange,start_date,end_date):
    df=get_index_price(exchange,start_date,end_date)
    df['daily_return']=df['closing_price'].pct_change()
    df=df[['trade_date','ticker','daily_return']]
    return df

def get_log_index_return(exchange,start_date,end_date):
    df=get_index_price(exchange,start_date,end_date)
    df['log_return']=np.log(df['closing_price']).diff()
    df=df[['trade_date','ticker','log_return']]
    return df


def get_dividend_data(exchange,symbol,start_date,end_date):
    
    if exchange in Dividend_available:
        table=Dividend_table[exchange]
        if start_date.lower() == "all" and end_date.lower() == "max":
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}'"
        elif start_date.lower() == "all":
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND pay_date<='{end_date}'"
        elif end_date.lower() == 'max':
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND pay_date>='{start_date}'"
        else:
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND (pay_date>='{start_date}' AND pay_date<='{end_date}')"
    
        df = pd.read_sql_query(sql, engine)
        df = df.sort_values('pay_date')
        df = df.reset_index(drop=True)
    
        return df
    else:
        print("WE CURRENTLY LACK DIVIDEND DATA FOR THIS EXCHANGE,\n\t WE ARE SORRYe🥺")
        return None
    
def get_distribution_data(exchange,symbol,start_date,end_date):
    if exchange in distribution_available:
        table=distribution_tables[exchange]
        if start_date.lower() == "all" and end_date.lower() == "max":
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}'"
        elif start_date.lower() == "all":
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND pay_date<='{end_date}'"
        elif end_date.lower() == 'max':
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND pay_date>='{start_date}'"
        else:
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND (pay_date>='{start_date}' AND pay_date<='{end_date}')"
    
        df = pd.read_sql_query(sql, engine)
        df = df.sort_values('pay_date')
        df = df.reset_index(drop=True)
    
        return df
    else:
        print("WE CURRENTLY LACK DISTRIBUTIONS DATA FOR THIS EXCHANGE,\n\t WE ARE SORRYe🥺")
        return None
    
def get_bonus_issue_data(exchange,symbol,start_date,end_date):
    if exchange in bonus_available:
        table=bonus_tables[exchange]
        if start_date.lower() == "all" and end_date.lower() == "max":
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}'"
        elif start_date.lower() == "all":
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND credit_date<='{end_date}'"
        elif end_date.lower() == 'max':
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND credit_date>='{start_date}'"
        else:
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND (credit_date>='{start_date}' AND credit_date<='{end_date}')"
        df=pd.read_sql_query(sql,engine)
        df=df.sort_values('credit_date')
        df=df.reset_index(drop=True)
        return df
    else:
        print("WE CURRENTLY LACK DISTRIBUTION DATA ON THIS EXCHANGE")
        return None
    
def get_splits_data(exchange,symbol,start_date,end_date):
    if exchange in splits_available:
        table=splits_tables[exchange]
        if start_date.lower() == "all" and end_date.lower() == "max":
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}'"
        elif start_date.lower() == "all":
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND effective_date<='{end_date}'"
        elif end_date.lower() == 'max':
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND effective_date>='{start_date}'"
        else:
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND (effective_date>='{start_date}' AND effective_date<='{end_date}')"
        df=pd.read_sql_query(sql,engine)
        df=df.sort_values('effective_date')
        df=df.reset_index(drop=True)
        return df
    else:
        print("WE CURRENTLY LACK STOCK SPLITS DATA ON THIS EXCHANGE")
        return None


def get_rights_data(exchange,symbol,start_date,end_date):
    if exchange in rights_available:
        table=rights_tables[exchange]
        if start_date.lower() == "all" and end_date.lower() == "max":
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}'"
        elif start_date.lower() == "all":
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND credit_date<='{end_date}'"
        elif end_date.lower() == 'max':
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND credit_date>='{start_date}'"
        else:
            sql = f"SELECT * FROM {table} WHERE ticker='{symbol}' AND (credit_date>='{start_date}' AND credit_date<='{end_date}')"
        df=pd.read_sql_query(sql,engine)
        df=df.sort_values('credit_date')
        df=df.reset_index(drop=True)
        return df
    else:
        print("WE CURRENTLY LACK RIGHTS DATA ON THIS EXCHANGE..........")
        return None

def get_ticker_changes(exchange):
    if exchange in ticker_changes:
        table=ticker_changes_tables[exchange]
        sql=f"SELECT * FROM {table}"
        df = pd.read_sql_query(sql, engine) 
        df = df.reset_index(drop=True)
        return df
    else:
        print("NO TICKER CHANGE RECORDED FOR THIS EXCHANGE.................")
        return None

def get_ticker_list(exchange):
    if exchange in exchanges_available:
        table=Exchange_tables[exchange]
        sql=f"SELECT DISTINCT ticker FROM {table}"
        df=pd.read_sql_query(sql,engine)
        change_df=get_ticker_changes(exchange)
        change_list=change_df['old_ticker'].to_list()
        ticker_list=df['ticker'].to_list()
        ticker_list = list(set(ticker_list) - set(change_list))
        ticker_list.sort()
        return ticker_list
    else:
        print("WE LACK ANY DATA ON THIS EXCHANGE")
        return None

def get_company_list(exchange):
    if exchange in exchanges_available:
        table=Exchange_tables[exchange]
        sql=f"SELECT DISTINCT company_name FROM {table}"
        df=pd.read_sql_query(sql,engine)
        company_list=df['company_name'].to_list()
        return company_list
    else:
        print("WE LACK ANY DATA ON THIS EXCHANGE")
        return None

def get_industry_list(exchange):
    if exchange in exchanges_available:
        table=Exchange_tables[exchange]
        sql=f"SELECT DISTINCT industry FROM {table}"
        df=pd.read_sql_query(sql,engine)
        if 'industry' in df.columns:
            industry_list=df['industry'].to_list()
            return industry_list
        else:
            print("WE ARE STILL WORKING ON GETTING INDUSTRY CATEGORIES FOR THIS EXCHANGE............")
    else:
        print("WE LACK ANY DATA ON THIS EXCHANGE")
        return None

def get_volatility(exchange,symbol):
    if exchange in exchanges_available:
        df=get_no_dividend_daily_return(exchange,symbol,start_date='all',end_date='max',ignore_zero_volume=True)
        #filter for the last 242 days
        df=df.tail(242)
        volatility=df['daily_return'].std()
        yearly_volatility=volatility*np.sqrt(242)
        data={'symbol':symbol,
              'daily_volatility':volatility,
              'annualized_volatility':yearly_volatility}
        vol_df=pd.DataFrame([data])
        return vol_df
    else:
        print("WE CURRENTLY LACK DATA ON THIS EXCHANGE..................")
        return None

def get_unlevered_beta(exchange,symbol):
    if exchange in exchanges_available:
        ticker_df=get_no_dividend_daily_return(exchange,symbol,start_date='all',end_date='max',ignore_zero_volume=True)
        ticker_df=ticker_df.tail(242)
        ticker_df=ticker_df[['trade_date','daily_return']]
        index_df=get_index_return( exchange,start_date='all',end_date='max')
        index_df=index_df[['trade_date','daily_return']]
        df=ticker_df.merge(index_df,on='trade_date',how='inner',suffixes=("_stock","_index"))
        X=sm.add_constant(df['daily_return_index'])
        y=df['daily_return_stock']
        model=sm.OLS(y,X).fit()
        beta=model.params["daily_return_index"]
        alpha=model.params["const"]
        r2=model.rsquared
        t_beta=model.tvalues["daily_return_index"]
        p_beta=model.pvalues["daily_return_index"]
        data={"ticker":symbol,
              "unleverd_beta":beta,
              "alpha":alpha,
              "R2":r2,
              "T Value":t_beta,
              "P Value":p_beta}
        beta_df=pd.DataFrame([data])
        return beta_df
    else:
        print('WE CURRRENTLY LACK DATA ON THIS EXCHANGE')
        return None