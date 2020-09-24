"""
Copyright (c) 2019 by Impulse Innovations Ltd. Private and confidential. Part of the causaLens product.
"""
import requests
import pandas as pd
import numpy as np
import io
import json
from datetime import timedelta
from datetime import datetime
from pandas.tseries.offsets import BDay
import alphalens

FORCE_LIVE = True
GENERAL_FEATURES=['Code', 'CountryName', 'Sector', 'InternationalDomestic', 
                  'Industry', 'GicSector', 'GicIndustry', 'GicSubIndustry', 'HomeCategory']


def get_fundamental_data(symbol= 'AAPL.US', 
                         api_token='5d9220f54d3479.22613861', 
                         session=None, 
                         live = FORCE_LIVE):
    r"""
    get_fundamental_data return a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    if( not live ):
        with open(r'fundamentals/' + symbol + r'.JSON') as json_file:
            data = json.load(json_file)                
        return data
    if session is None:
        session = requests.Session()
    url = 'https://eodhistoricaldata.com/api/fundamentals/%s' % symbol
    params = { 
        api_token: api_token
    }
    r = session.get(url, params=params)
    if r.status_code == requests.codes.ok:
        df = json.load(io.StringIO(r.text))
        with open('fundamentals/' + symbol + ".json", 'w') as outfile:
            json.dump(df, outfile)        
        return df
    else:
        raise Exception(r.status_code, r.reason, url)

        
def get_index_tickers(index = "GSPC", 
                      initdate = '2020-01-01', 
                      country='US'):
    r"""
    get_index_tickers return a dataframe of the index constituents. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/#Index_Constituents_or_Index_Components_API

    :param index: String symbol for the desired index.
    :param initdate: String date in 'YYYY-MM-DD' format. Which date to retrieve index makeup from.
    :param country: Optional string. Appends Exchange code to stock symbols to get tickers if df doesn't have exchange listed.
    """
    if not index.endswith(".INDX"):
        index = index + ".INDX"    
    data = get_fundamental_data( index )
    try:
        df = pd.DataFrame(list(data['HistoricalTickerComponents'].values()))
        try:
            df['ticker'] = df['Code']+"."+df["Exchange"]
        except:
            df['ticker'] = df['Code']+"."+country
        df = df.set_index('ticker')
        df = df[df['StartDate']<initdate]
        df = df[df['EndDate']>initdate]
    except:
        print("No historical ticker data. Returning current ticker data")
        df = pd.DataFrame(list(data['Components'].values()))
        df['ticker'] = df['Code']+"."+df['Exchange']
        df = df.set_index('ticker')
    return df 


def get_exchanges(api_token='5d9220f54d3479.22613861', 
                  session=None):   
    r"""
    get_exchanges return a dataframe of the exchanges for which eod historical data is available.  
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/exchanges-api-list-of-tickers-and-trading-hours/#Get_List_of_Tickers_Exchange_Symbols

    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    """
    if session is None:
        session = requests.Session()
    url = 'https://eodhistoricaldata.com/api/exchanges-list/?api_token={token}'.format(token=api_token)
    r = session.get(url)
    if r.status_code == requests.codes.ok:
        df = pd.DataFrame(json.loads(r.text)).set_index("Code")
        return df
    else:
        raise Exception(r.status_code, r.reason, url)

        
def search_symbol(symbol,
                 api_token='5d9220f54d3479.22613861', 
                 session=None):  
    r"""
    search_symbol return a dataframe of all available symbols with ticker or name matching search string.
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/search-api-for-stocks-etfs-mutual-funds-and-indices/

    :param symbol: search string to look for in ticker or symbol name e.g. AAPL or else Apple.
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    """
    if session is None:
        session = requests.Session()
    url = 'https://eodhistoricaldata.com/api/search/{symbol}?api_token={token}'.format(symbol=symbol,token=api_token)
    r = session.get(url)
    if r.status_code == requests.codes.ok:
        df = pd.DataFrame(json.loads(r.text)).set_index("Code")
        return df
    else:
        raise Exception(r.status_code, r.reason, url)  

        
def sharpe_ratio(rets, 
                 risk_free=0):
    r"""
    sharpe_ratio return the sharpe ratio of a given series of returns. 

    :param rets: pandas series of returns.
    :param risk_free: float to specify a risk free rate for sharpe ratio. Defaults to 0.
    """
    rets = rets.replace(np.inf,np.nan)
    mean_ret = np.nanmean(rets)
    std_ret = np.nanstd(rets)
    return (mean_ret-risk_free)/std_ret
 
    
def get_eod_data(symbol = 'AAPL.US', 
                 freq = 'd', 
                 start_date = '2008-01-01', 
                 end_date = '2020-12-12', 
                 api_token='5d9220f54d3479.22613861', 
                 session=None, 
                 live = FORCE_LIVE):   
    r"""
    get_eod_data return a dataframe of end of day price and volume data of symbol for date period specified. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/api-for-historical-data-and-volumes/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param freq: String use ‘d’ for daily, ‘w’ for weekly, ‘m’ for monthly prices. 
    :start_date: String date format 'YYYY-MM-DD'. First date to retrieve prices from.
    :end_date: String date format 'YYYY-MM-DD'. Last date to retrieve prices up to.
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    if( not live ):
        df = pd.read_csv(r'eod/' + symbol + ".csv", index_col=0,  parse_dates=[0])
        df = df.loc[start_date:end_date]
        return df 
    if session is None:
        session = requests.Session()
    url = 'https://eodhistoricaldata.com/api/eod/{symbol}'.format(symbol=symbol)
    today = datetime.today().strftime('%Y-%m-%d')
    params = { 
        'api_token': api_token,
        'from' : '2001-01-01',
        'to' : today,#end_date,
        'period' : freq,
    }
    r = session.get(url, params=params)
    if r.status_code == requests.codes.ok:
        df = pd.read_csv(io.StringIO(r.text), skipfooter=1, parse_dates=[0], index_col=0,  engine='python')
        df.to_csv(r'eod/' + symbol + ".csv")
        df = df.loc[start_date:end_date]
        return df
    else:
        raise Exception(r.status_code, r.reason, url)

        
def get_intraday_data(symbol = 'AAPL.US', 
                 interval = '5m', 
                 api_token='5d9220f54d3479.22613861', 
                 session=None, 
                 live = FORCE_LIVE):  
    r"""
    get_intraday_data return a dataframe of intraday price and volume data of symbol for last 30 days. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/intraday-historical-data-api/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param interval: String use ‘5m’ for 5-minutes intervals and ‘1m’ for 1-minute intervals of intraday updates. 
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    if( not live ):
        df = pd.read_csv(r'intraday/' + symbol + ".csv", index_col=0,  parse_dates=[0])
        return df 
    if session is None:
        session = requests.Session()
    url = 'https://eodhistoricaldata.com/api/intraday/{symbol}?api_token={token}&interval={interval}&from=1564752900'.format(
        symbol = symbol,
        token = api_token,
        interval = interval)
    r = session.get(url)
    if r.status_code == requests.codes.ok:
        df = pd.read_csv(io.StringIO(r.text), skipfooter=1, parse_dates=[0], index_col=0,  engine='python')
        df.to_csv(r'eod/' + symbol + ".csv")
        return df
    else:
        raise Exception(r.status_code, r.reason, url)        

        
def get_exchange_data(symbol= 'US', 
                      api_token='5d9220f54d3479.22613861', 
                      session=None, 
                      live=FORCE_LIVE):
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    if( not live ):
        df = pd.read_csv(r'eod/' + symbol + ".csv", index_col=0 ) 
        return df
    if session is None:
        session = requests.Session()
    url = 'https://eodhistoricaldata.com/api/exchange-symbol-list/%s' % symbol
    params = { 
        'api_token': api_token
    }
    r = session.get(url, params=params)
    if r.status_code == requests.codes.ok:
        df = pd.read_csv(io.StringIO(r.text), skipfooter=1, index_col=0,  engine='python')
        df.to_csv(r'eod/' + symbol + ".csv")        
        return df
    else:
        raise Exception(r.status_code, r.reason, url)       

        
def get_generalinfo(data):
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    general = data['General']
    generalfeatures = [ 'Code' ] + GENERAL_FEATURES 
    general = { x : general.get(x) for x in generalfeatures}    
    return general        


def get_outstanding_shares(data):
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    try:
        df = pd.DataFrame(list(data['outstandingShares']['quarterly'].values()))
        df['date'] = df['dateFormatted']
    except:
        try:
            df = pd.DataFrame(list(data['outstandingShares']['annual'].values()))
            df['date'] = df['dateFormatted']       
        except:
            raise ValueError('No outstanding Shares data available')
    df = df[['date','shares']] 
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')    
    return( df )


def parse_dates(df, 
                use_filing_date = False):   
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    if df.empty:
        raise ValueError("No data available")
    else:
        df['date'] = pd.to_datetime(df['date'])
        df['filing_date'] = pd.to_datetime(df['filing_date'].replace('0000-00-00',np.nan))
        df['filing_date'] = df['filing_date'].fillna(df['date']+BDay(25) )    
        idx_name = 'date'    
        if( use_filing_date ):
            idx_name = 'filing_date'
        df = df.set_index(idx_name)    
        df = df.sort_index()
        df = df.replace("None",np.nan)
        df = df.fillna(value=np.nan) 
        return df


def financials_postprocess(data, 
                           filing_type, 
                           use_filing_date=True):
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    general = get_generalinfo( data )
    types = ['Balance_Sheet', 'Cash_Flow', 'Income_Statement']
    dfs = [ pd.DataFrame(list(data['Financials'][x][filing_type].values()), dtype = float ) for x in types ]    
    dfs = [ parse_dates( x, use_filing_date = use_filing_date ) for x in dfs ]  
    dfs = [x[~x.index.duplicated(keep='first')] for x in dfs]
    df = pd.concat( dfs, axis=1, join = 'outer' )
    return( df )


def compute_market_cap(history, 
                       shares):
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    shares = shares[['shares']].resample('d').fillna(method='ffill')
    prices = history[['Adjusted_close']].resample('d').fillna(method='ffill')        
    join = pd.concat( [ shares, prices ], axis=1, join='outer').fillna(method='ffill')     
    join.loc[:,'marketCap'] = (join['shares']*join['Adjusted_close']).fillna(method='ffill')
    return(join)


def industry_features():
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    return([ 'CountryName','Sector','Industry','GicSector','GicIndustry','GicSubIndustry'])


def fundamentals_parse():
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    income_statement_df = {
        'Revenue': [
            ('Total Revenue', 'totalRevenue'),
            ('Cost of Revenue', 'costOfRevenue'),
            ('Gross Profit', 'grossProfit')
        ],
        'Operating Expenses': [
            ('Research Development', 'researchDevelopment'),
            ('Selling General and Administrative', ''),
            ('Non Recurring', 'nonRecurring'),
            ('Others', 'otherOperatingExpenses'),
            ('Total Operating Expenses', 'totalOperatingExpenses'),
            ('Operating Income or Loss', 'operatingIncome')
        ],
        'Income from Continuing Operations': [
            ('Total Other Income/Expenses Net', 'totalOtherIncomeExpenseNet'),
            ('Earnings Before Interest and Taxes', 'ebit'),
            ('Interest Expense', 'interestExpense'),
            ('Income Before Tax', 'incomeBeforeTax'),
            ('Income Tax Expense', 'incomeTaxExpense'),
            ('Minority Interest', 'minorityInterest'),
            ('Net Income From Continuing Ops', 'netIncomeFromContinuingOps'),
            ('Net Interest Income', 'netInterestIncome')
        ],
        'Non-recurring Events': [
            ('Discontinued Operations', 'discontinuedOperations'),
            ('Extraordinary Items', 'extraordinaryItems'),
            ('Effect Of Accounting Changes', 'effectOfAccountingCharges'),
            ('Other Items', 'otherItems')
        ],
        'Net Income': [
            ('Net Income', 'netIncome'),
            ('Preferred Stock And Other Adjustments', 'preferredStockAndOtherAdjustments'),
            ('Net Income Applicable To Common Shares', 'netIncomeApplicableToCommonShares')
        ]
    }
    balance_sheet_df =   {
        'Assets': [
            ('Cash And Cash Equivalents', 'cash'),
            ('Short Term Investments', 'shortTermInvestments'),
            ('Net Receivables', 'netReceivables'),
            ('Inventory', 'inventory'),
            ('Other Current Assets', 'otherCurrentAssets'),
            ('Total Current Assets', 'totalCurrentAssets'),
            ('Long Term Investments', 'longTermInvestments'),
            ('Property Plant and Equipment', 'propertyPlantEquipment'),
            ('Goodwill', 'goodWill'),
            ('Intangible Assets', 'intangibleAssets'),
            ('Accumulated Amortization', 'accumulatedAmortization'),
            ('Other Assets', 'otherAssets'),
            ('Deferred Long Term Asset Charges', 'deferredLongTermAssetCharges'),
            ('Total Assets', 'totalAssets')
        ],
        'Liabilities': [
            ('Accounts Payable', 'accountsPayable'),
            ('Short/Current Long Term Debt', 'shortLongTermDebt'),
            ('Other Current Liabilities', 'otherCurrentLiab'),
            ('Total Current Liabilities', 'totalCurrentLiabilities'),
            ('Long Term Debt', 'longTermDebt'),
            ('Other Liabilities', 'otherLiab'),
            ('Deferred Long Term Liability Charges', 'deferredLongTermLiab'),
            ('Minority Interest', 'minorityInterest'),
            ('Negative Goodwill', 'negativeGoodwill'),
            ('Total Liabilities', 'totalLiab')
        ],
        'Equity': [
            ('Misc. Stocks Options Warrants', 'warrants'), #
            ('Redeemable Preferred Stock', 'preferredStockRedeemable'),
            ('Preferred Stock', 'preferredStockTotalEquity'),
            ('Common Stock', 'commonStock'),
            ('Retained Earnings', 'retainedEarnings'),
            ('Treasury Stock', 'treasuryStock'),
            ('Capital Surplus', 'capitalSurpluse'),
            ('Other Stockholder Equity', 'otherStockholderEquity'),
            ('Total Stockholder Equity', 'totalStockholderEquity'),
            ('Net Tangible Assets', 'netTangibleAssets')
        ]
    }    
    cash_flow_df = {
        'Overall': [
            ('Net Income', 'netIncome')
        ],
        'Operating activities': [
            ('Depreciation', 'depreciation'),
            ('Adjustments to net income', 'changeToNetincome'),
            ('Changes in accounts receivable', 'changeToAccountReceivables'),
            ('Changes in liabilities', 'changeToLiabilities'),
            ('Changes in inventory', 'changeToInventory'),
            ('Changes in other operating activities', 'changeToOperatingActivities'),
            ('Total cash flow from operating activities', 'totalCashFromOperatingActivities')
        ],
        'Investment activities': [
            ('Capital expenditure', 'capitalExpenditures'),
            ('Investments', 'investments'),
            ('Other cash flow from investment activities', 'otherCashflowsFromInvestingActivities'),
            ('Total cash flow from investment activities', 'totalCashflowsFromInvestingActivities'),
        ],
        'Financing activities': [
            ('Dividends paid', 'dividendsPaid'),
            ('Sale purchase of stock', 'salePurchaseOfStock'),
            ('Net borrowings', 'netBorrowings'),
            ('Other cash flow from financing activities', 'otherCashflowsFromFinancingActivities'),
            ('Total cash flow from financing activities', 'totalCashFromFinancingActivities')
        ],
        'Changes in Cash': [
            ('Effect of exchange rate changes', 'exchangeRateChanges'),
            ('Change in cash and cash equivalents', 'changeInCash')
        ]
    }    
    agg = { 'Cash-Flow' : cash_flow_df,
            'Balance Sheet' : balance_sheet_df ,
            'Income Statement': income_statement_df }
    return agg


def base_fundamental_ratios():    
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    output = {
        'Assets to Equity' : [ 'totalAssets', 'totalStockholderEquity' ],
        'EBIT to Equity'   : [ 'ebit', 'totalStockholderEquity'],
        'Liability to Assets' : ['totalLiab', 'totalAssets' ],
        'Liability to Equity' : ['totalLiab', 'totalStockholderEquity' ],
        'Short Term Debt to Cash' : [ 'shortLongTermDebt', 'cash'],
        'Earnings per Share' : ['netIncome','shares'],
        'Assets Yield' : ['netIncome','totalAssets'],
        'Investments Yield' : [ 'netIncome', 'investments' ],
        'Profit Margin' : ['grossProfit','totalRevenue'],
        'Book Price': ['marketCap','netIncome'],
        
        'Short Term Debt Ratio' : ['shortLongTermDebt', 'marketCap'],
        'Income Ratio' : ['netIncome', 'marketCap'],
        'Asset Ratio'  : [ 'totalAssets', 'marketCap' ],
        'Stock Sale/Purchase Ratio' : ['salePurchaseOfStock', 'marketCap'],
        'Div Yield': ['dividendsPaid','marketCap'],
        'EBIT ratio'   : [ 'ebit', 'marketCap'],        
        'Equity Ratio' : ['totalStockholderEquity','marketCap'],
        'Cash Ratio'   : [ 'cash', 'marketCap'],
        'Borrowing Ratio' : [ 'netBorrowings', 'marketCap' ]
    }
   
    return( output )


def get_realized_volatility(rets, 
                            days, 
                            meanadj=1, 
                            weekends=False):
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    period = str(days) + 'd'
    if weekends:
        dcf = 252/days
    else:
        dcf = 365/days
    e_x2 = (rets.pow(2)).rolling(period).sum()
    e2_x = 0
    if( meanadj ):
        e2_x = (rets.rolling(period).mean()).pow(2)*rets.rolling(period).count()
    vol = np.sqrt(e_x2-e2_x)*np.sqrt(dcf) #~sqrt(4) to annualize
    return vol    


def get_moving_average(data, 
                       days):
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    ma = data.rolling( str(days) + 'd').mean()
    return( ma )


def get_momentum(data, 
                 days_short=30, 
                 days_long=365):
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    momentum = np.log(get_moving_average( data, days_short ) / get_moving_average( data, days_long ))
    return( momentum )


def get_fund_df(f_history, 
                mktcap_history, 
                ratios, 
                extrapolate_forward=True):
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    df_base = pd.concat( [ f_history, mktcap_history ], axis = 1, join = 'outer')
    if( extrapolate_forward ):
        df_base = df_base.fillna(method='ffill')
    df = pd.DataFrame()
    for elm in ratios:
        over = ratios[ elm ][0]
        under = ratios[ elm ][1]
        over_s = df_base[ over ]
        if( under != 1 ):
            under_s = df_base[ under ]
            ratio = over_s / under_s
        else:
            ratio = over_s
        df[ elm ] = ratio
    df['AsOf'] = df_base['AsOf']
    return( df )
 
    
def get_sheet(df_factors,
              factor, 
              periods, 
              quantiles = 5,
              ticker_key = '__panel'):
    r"""
    get_fundamental_data returns a dictionary of fundamental, financial, and descriptive data. 
    The fields are detailed at https://eodhistoricaldata.com/knowledgebase/stock-etfs-fundamental-data-feeds/

    :param symbol: String symbol for the desired Stock, ETF, index etc, requires Exchange to be appended to ticker e.g. AAPL.US
    :param api_token: String key to connect to EOD api. 
    :param session: requests.session object if existing session in use.
    :param live: Boolean check to request fresh data or load existing from previous request.
    """
    df_factors = df_factors.reset_index().copy()
    df_factors = df_factors.set_index(['index',ticker_key])
    df_factors = df_factors.unstack(level=-1)
    df_factors = df_factors.fillna(method='ffill')        
    factor = df_factors[factor]
    factor.index = factor.index.tz_localize('UTC')
    factor = factor.stack()
    factor.index = factor.index.set_names(['date', 'asset'])
    pricing = df_factors['Adjusted_close']
    pricing.index = pricing.index.tz_localize('UTC')    
    factor_data = alphalens.utils.get_clean_factor_and_forward_returns(factor,
                                                                       pricing,
                                                                       periods=periods,
                                                                       quantiles=quantiles,
                                                                       bins=None)
    alphalens.tears.create_summary_tear_sheet(factor_data) 
    return(factor_data)


class MktObj:
    
    def __init__(self, symbol):
        self.symbol = symbol
    
    def History(self,  
                init_date="2008-01-01", 
                final_date="2030-12-12", 
                resolution = 'd', 
                refresh = True):
        if hasattr(self, 'ticker_history') and not refresh:
            return( self.ticker_history )
        else:
            self.ticker_history = get_eod_data( self.symbol, resolution, init_date, final_date )            
            return( self.ticker_history )

    def IntradayHistory(self, 
                        interval = '5m', 
                        refresh = True):
        if hasattr(self, 'intraday_history') and not refresh:
            return( self.intraday_history )
        else:
            self.intraday_history = get_intraday_data( self.symbol, interval)            
        return( self.intraday_history )

    
class Stock(MktObj):
    
    def __init__(self, 
                 ticker, 
                 exchange):
        self.symbol = ticker + "." + exchange
        self.ticker = ticker
        self.exchange = exchange
        super().__init__( self.symbol )
    
    def Raw_Financial_History(self, 
                              refresh = True):
        if hasattr(self, 'RawFundamentals') and not refresh:
            return( self.RawFinancials )
        else:        
            self.RawFinancials = get_fundamental_data( self.symbol )
        
        return( self.RawFinancials )
    
    def General_Info(self, 
                     item = None):
        financials = self.Raw_Financial_History( refresh = False )
        if item is None:
            return( financials['General'] )
        else:
            if item in financials['General']:
                return( financials['General'][ item ])
            else:
                print( item + " does not exist ")
                return( None )
    
    def Industry_Info(self):
        gen = self.General_Info()
        features = industry_features()
        y = { x : gen[x] for x in features }
        return y
            
    def Full_Financial_History( self, filing_type = 'quarterly' ):
        financials = self.Raw_Financial_History( refresh = False )
        df = financials_postprocess( financials, filing_type )
        df = df.loc[:,~df.columns.duplicated()]
        df['AsOf']=pd.PeriodIndex(df['date'], freq='Q')
        df = df.drop('date',axis=1)
        self.FullFinancials = df
        return df
    
    def Shares_History( self ):
        data = self.Raw_Financial_History( refresh = False )
        return( get_outstanding_shares( data ) )    
    
    def Market_Cap_History( self, init_date="2008-01-01", final_date="2030-12-12", refresh = False ):
        p_history = self.History( init_date, final_date, 'd', refresh = refresh )
        shares = self.Shares_History( )
        join = compute_market_cap( p_history, shares )        
        return( join )
    
    def Merge_History( self, init_date="2008-01-01", final_date="2030-12-12", filing_type = 'quarterly' ):
        full_financial_history = self.Full_Financial_History( filing_type )
        mktCap_history = self.Market_Cap_History( init_date, final_date )
        
        df = pd.concat([ mktCap_history, full_financial_history], axis = 1, join = 'outer')
        return( df )

    
class EqFactors:
    
    def __init__( self, stock_obj ):
        self.stock = stock_obj
        self.fundamental_ratios = base_fundamental_ratios()
        
    def add_fundamental_ratio(self, name, over_under):
        self.fundamental_ratios[ name ] = over_under 
    
    def Factors_df( self, filing_type = 'quarterly', refresh = False, resample = ''):
        ratios = self.fundamental_ratios
        f_history = self.stock.Full_Financial_History( filing_type )

        first_date = f_history.index[0].strftime('%Y-%m-%d')
        last_date = datetime.today().strftime('%Y-%m-%d')

        mktcap_history = self.stock.Market_Cap_History( '1990-01-01', last_date, refresh )
        mktcap_history = self.add_technical_factors( mktcap_history )
        mktcap_history = mktcap_history.loc[first_date:last_date]

        df = get_fund_df( f_history, mktcap_history, ratios )
        df = pd.concat([df,mktcap_history],axis=1,join='outer')

        if( len( resample) ):
            df = df.resample(resample).last()

        industry_info = self.stock.Industry_Info()
        for elm in industry_info:
            df[ elm ] = industry_info[ elm ]  
        df[ 'Ticker' ] = self.stock.ticker
        self.f_df = df
        name = self.stock.symbol
        df.to_csv(r'merged/' + name + ".csv")
        
        return( df )
        
    def GetStoredDf( self ):
        return( self.f_df )
    
    def add_technical_factors( self, data ):
        rets = np.log(data['Adjusted_close']/data['Adjusted_close'].shift(1))
        prices = data['Adjusted_close']
        data['1y vol'] = get_realized_volatility( rets, 365, meanadj = 1 )
        data['1m vol'] = get_realized_volatility( rets, 30, meanadj = 1 )
        data['1y ma'] = get_moving_average( prices, 365 )
        data['1m ma'] = get_moving_average( prices, 30 )
        data['1y1m momentum'] = get_momentum( prices, 30, 365 )
        
        return( data )

    
class Portfolio:
    
    def __init__( self ):
        self.assets = []
        
    def Add_Asset( self, ticker, exchange_code, quantity = 0, currency = 'USD'):
        dic = dict()
        dic['ticker']=ticker
        dic['exchange_code']=exchange_code
        dic['quantity']=quantity
        dic['currency']=currency
        self.assets.append( dic )
        
    def Get_Symbols( self ):
        assets = self.assets
        symbols = [d['ticker'] + "." + d['exchange_code'] for d in assets]
        return( symbols )
        
    def ComputeFactors( self ):
        assets = self.assets
        factors = []
        dfs = []
        for elm in assets:
            stock = Stock( elm['ticker'], elm['exchange_code'] )
            try:
                _ = stock.Raw_Financial_History()
                __ = stock.History('2000','2030')
                fact = EqFactors( stock )
                dfs.append(fact.Factors_df())
                factors.append( fact )  
                print(elm['ticker'] + " successful.")
            except (ValueError,KeyError) as e:
                print( elm['ticker'] + " failed.")
                print(e)
        self.Dfs = dfs    
        self.FactorsArray = factors
        return( self.FactorsArray )
    
    def Factors_df( self ):
        _ = self.ComputeFactors()
        dfs = self.Dfs
        self.Factor_df = pd.concat( dfs, axis = 0, join = 'outer')
        self.Factor_df = self.Factor_df.drop(columns=['AsOf'])
        self.Factor_df = self.Factor_df.sort_index()
        return( self.Factor_df )
    
    def NAV_history( self ):
        self.weights = pd.DataFrame(self.assets).set_index("ticker")
        if hasattr(self, 'Factor_df'):
            df = self.Factor_df
        else:
            _ = self.Factors_df()
            df = self.Factor_df
        navs = df.reset_index().Adjusted_close*self.weights.loc[df.Ticker].reset_index().quantity
        navs.index = df.index
        ind_series = navs.groupby(df.Ticker)
        self.nav_history = pd.concat([ind_series.get_group(x) for x in ind_series.groups],1).fillna(method="ffill").sum(1)
        return self.nav_history

    
class Exchange:
    
    def __init__( self, code, exchanges = [], eqtypes = ['Common Stock'] ):
        self.code = code
        self.exchanges = exchanges
        self.eqtypes = eqtypes
        
    def Tickers( self ):        
        self.all_tickers = get_exchange_data( self.code )
        self.tickers = self.all_tickers
        if( len( self.exchanges )):
            self.tickers = self.all_tickers[self.all_tickers['Exchange'].isin(self.exchanges)]
        
        if( len( self.eqtypes ) ):
            self.tickers = self.tickers[self.tickers['Type'].isin(self.eqtypes)]
        self.tickers_list = list(self.tickers.index)
        
        return( self.tickers_list ) 
    
    def ComputeFactors( self ):
        tickers = self.Tickers()
        factors = []
        dfs = []
        for elm in tickers:
            stock = Stock( elm, self.code )
            try:
                _ = stock.Raw_Financial_History()
                __ = stock.History('2000','2030')
                fact = EqFactors( stock )
                dfs.append(fact.Factors_df())
                factors.append( fact )
                print(elm + " successful.")
            except (ValueError,KeyError) as e:
                print( elm + " failed.")
                print(e)
        self.Dfs = dfs    
        self.FactorsArray = factors
        return( self.FactorsArray )
    
    def Factors_df( self ):
        _ = self.ComputeFactors()
        dfs = self.Dfs
        self.Factor_df = pd.concat( dfs, axis = 0, join = 'outer')
        self.Factor_df = self.Factor_df.drop(columns=['AsOf'])
        self.Factor_df = self.Factor_df.sort_index()
        return( self.Factor_df )        

    
class Index(MktObj):
    
    def __init__( self, index, asof_date, country = 'US' ):
        if not index.endswith(".INDX"):
            index = index + ".INDX"
        
        self.name = index
        self.date = asof_date
        self.country = country
        super().__init__( self.name )
    
    def Tickers( self ):
        self.tickers_details = get_index_tickers( self.name, self.date )
        self.tickers_list = list(self.tickers_details['Code'])
        return( self.tickers_list )
    
    def Tickers_Detailed( self ):
        _ = self.Tickers()
        return( self.tickers_details )
    
    def get_ticker_info(self):
        data = get_fundamental_data( self.name)
        df = pd.DataFrame(list(data['Components'].values()))
        df['ticker']=df['Code']+"."+df['Exchange']
        df = df.set_index('ticker')
        self.ticker_info = df
        return df    
        
    def ComputeFactors( self ):
        tickers = self.Tickers()
        factors = []
        dfs = []
        for elm in tickers:
            stock = Stock( elm, self.country )
            try:
                _ = stock.Raw_Financial_History()
                __ = stock.History('2000','2030')
                fact = EqFactors( stock )
                dfs.append(fact.Factors_df())
                factors.append( fact )
                print(elm + " successful.")
            except (ValueError,KeyError) as e:
                print( elm + " failed.")
                print(e)
        self.Dfs = dfs    
        self.FactorsArray = factors
        return( self.FactorsArray )
    
    def Factors_df( self ):
        _ = self.ComputeFactors()
        dfs = self.Dfs
        self.Factor_df = pd.concat( dfs, axis = 0, join = 'outer')
        self.Factor_df = self.Factor_df.drop(columns=['AsOf'])
        self.Factor_df = self.Factor_df.sort_index()
        return( self.Factor_df )