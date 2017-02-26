'''This script exposes necessary function calls to get the data.
'''

'''
Copyright (c) 2017, WinQuant Information and Technology Co. Ltd.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the <organization> nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''

# built-in modules
import datetime as dt
import functools

# third-party modules
import numpy  as np
import pandas as pd

# customized modules
import data.api.base as base
from data.config import *
from data.driver import mongodb
from data.driver import mysql

import util.calendar as uc

# Get RIC exchange code given the Datayes ones.
RIC_EXCHANGE_CODE     = { 'XSHG': 'SH',
                          'XSHE': 'SZ' }
# Get the Datayes ones given the RIC ones.
DATAYES_EXCHANGE_CODE = { 'SH': 'XSHG',
                          'SZ': 'XSHE' }

DEFAULT_START_DATE = dt.date( 2012, 1, 1 )

TICK_DEFAULT_START_DATE = '20150101'
TICK_DATE_FORMAT        = '%Y%m%d'

WIND_DEFAULT_START_DATE = '20120101'
WIND_DATE_FORMAT        = '%Y%m%d'

# @functools.lru_cache( maxsize=32 )
def _getUniverse( asOfDate, country='CN' ):
    '''Get stock universe as of the given date.

Parameters
----------
asOfDate : datetime.date
    Data date of the stock classification or
    None if not found;
country : str
    Country identifier, currently, only CN supported.

Returns
-------
universeInfo : dict
    Universe information with `Sector` universe sector and `Stocks` stock information.

Exceptions
----------
    raise Exception when duplicated records found.
    '''
    username, password = MONGODB_CRED
    db = mongodb.getAuthenticatedConnection( MONGODB_URL,
                                             MONGODB_PORT,
                                             username,
                                             password,
                                             'universe' )
    universe = db.stocks.find_one( { 'Date': { '$lte': dt.datetime.combine( asOfDate, 
                                       dt.datetime.min.time() ) },
                               'Country': country },
                               sort=[ ( 'Date', mongodb.pymongo.DESCENDING ) ] )

    return universe


def getStockClassification( asOfDate, exch=None, country='CN', alive=True ):
    '''Get stock classification.

Parameters
----------
asOfDate : datetime.date
    Data date to get all available stock names;
exch : str
    Exchange name of the stocks expected or None to get all names in the country;
country : str
    Country of the stocks traded in;
alive : boolean
    An indicator whether return alive stocks only or include all stocks on the exchange.

Returns
-------
industryClassification : pandas.DataFrame
    Industry classification.

Exceptions
----------
    raise Exception when duplicated records found.
    '''
    universe = _getUniverse( asOfDate, country )
    industryClassification = pd.read_json( universe[ 'Sectors' ] )
    industryClassification.sort_index( inplace=True )

    if exch is not None:
        validExch = DATAYES_EXCHANGE_CODE.get( exch, exch )
        industryClassification = industryClassification[
            industryClassification.exchangeCD == validExch ]

    if alive:
        # Get listed stocks
        stockInfo = getStockInformation( asOfDate )
        stockInfo = stockInfo[ stockInfo.listStatusCD == 'L' ]
        industryClassification = industryClassification[
            industryClassification.isNew == 1 ]
        industryClassification = industryClassification[
            industryClassification.secID.isin( stockInfo.secID ) ] 

    return industryClassification


def getStockInformation( asOfDate, exch=None, country='CN' ):
    '''Get stock fundamental information, e.g. listing date, non-rest floting, etc.

Parameters
----------
asOfDate : datetime.date
    Data date to get all available stock names;
exch : str or None
    Exchange name of the stocks expected or None to get all names in the country;
country : str
    Country of the stocks traded in.

Returns
-------
stockInformation : pandas.DataFrame
    Stock information.

Exceptions
----------
    raise Exception when duplicated records found.
    '''
    universe  = _getUniverse( asOfDate, country )
    stockInfo = pd.read_json( universe[ 'Stocks' ] )
    stockInfo.sort_index( inplace=True )

    if exch is not None:
        validExch = DATAYES_EXCHANGE_CODE.get( exch, exch )
        stockInfo = stockInfo[ stockInfo.exchangeCD == validExch ]

    return stockInfo


def getExchangeStockNames( asOfDate, exch=None, country='CN', alive=True ):
    '''Get all stock names in the given exchange.

Parameters
----------
asOfDate : datetime.date
    Data date to get all available stock names;
exch : str
    Exchange name of the stocks expected or None to get all names under the country;
country : str
    Country of the stocks traded on;
alive : boolean
    An indicator whether return alive stocks only or include all stocks on the exchange.

Returns
-------
stocks : list of str
    Names of all stocks traded on the exchange.

Exceptions
----------
    raise Exception when duplicated records found.
    '''
    classification = getStockClassification( asOfDate, exch=exch,
                                             country=country, alive=alive )
    if classification is None:
        stocks = None
    else:
        stocks = list( classification.secID )

    return stocks


def getDailyData( secId, startDate=DEFAULT_START_DATE, endDate=dt.date.today() ):
    '''Get daily data for the given stocks during the date range.

Parameters
----------
secId : str
    Security ID of the stock;
startDate : datetime.date
    Start date of the daily data queried inclusively;
endDate : datetime.date
    End date of the daily data queried inclusively.

Returns
-------
dailyData : pandas.DataFrame
    Requested daily data in pandas.DataFrame.

Exceptions
----------
    raise Exception when duplicated records found on the given stock name.
    '''
    # Get authenticated MongoDB connection
    username, password = MONGODB_CRED
    db = mongodb.getAuthenticatedConnection( MONGODB_URL, MONGODB_PORT,
        username, password, 'dailyData' )

    # Query data
    cursor = db.stocks.find( { 'SecID': secId } )

    # Sanity check
    nRecords = cursor.count()
    if nRecords == 0:
        dailyData = None
    elif nRecords > 1:
        raise Exception( 'Duplicated {n:d} records found for stock {s:s}.'.format(
                         n=nRecords, s=secId ) )
    else:
        data = cursor.next()
        dailyData = pd.read_json( data[ 'Data' ] )
        dailyData.sort_index( inplace=True )

        # Filtered by date
        startDateStr = startDate.strftime( '%Y-%m-%d' )
        endDateStr   = endDate.strftime( '%Y-%m-%d' )

        greaterDates = dailyData.tradeDate >= startDateStr
        smallerDates = dailyData.tradeDate <= endDateStr
        dailyData = dailyData[ np.logical_and( greaterDates, smallerDates ) ]
        
    return dailyData


def getBinData( secId, startDate=DEFAULT_START_DATE, endDate=dt.date.today() ):
    '''Get minute-by-minute data for the given stock during the date range.

Parameters
----------
secId : str
    Security ID of the stock;
startDate : datetime.date
    Start date of the bin data required inclusively,
endDate : datetime.date
    End date of the bin data required inclusively.

Returns
-------
binData : pandas.Panel
    Requested bin data in pandas.Panel.

Exceptoins
----------
    raise Exception when duplicated records found on the given stock name.
    '''
    # Get authenticated MongoDB connection
    username, password = MONGODB_CRED
    db = mongodb.getAuthenticatedConnection( MONGODB_URL, MONGODB_PORT,
        username, password, 'binData' )

    # Query data
    cursor = db.stocks.find( { 'SecID': secId, 'Date': {
        '$gte': { 'Date': dt.datetime.combine( startDate, dt.datetime.min.time() ) },
        '$lte': { 'Date': dt.datetime.combine( endDate, dt.datetime.min.time() ) } } } )

    data = {}
    for item in cursor:
        # Build DataFrame's to convert to a Panel.
        date = item[ 'Date' ].date()
        if date in data:
            raise Exception( 'Duplicated records on {d:s} found.'.format(
                d=str( date ) ) )
        else:
            dayBinData = pd.read_json( item[ 'Data' ] )
            dayBinData.sort_index( inplace=True )
            data[ date ] = dayBinData

    return pd.Panel( data )


class BinDataSource( base.BinDataSource ):
    '''Get stocks bin data from the database.
    '''

    def __init__( self ):
        '''Initialize a BinDataSource for stocks.
        '''
        super( BinDataSource, self ).__init__( 'indicator_stock' )


class WindDataSource( object ):
    '''Get data from Wind.
    '''

    def __init__( self ):
        '''Initialize a WindDataSource object.
        '''
        username, password = MYSQL_WIND_CRED
        dbname = 'wind'
        self.conn = mysql.getAuthenticatedConnection( MYSQL_WIND_URL, MYSQL_WIND_PORT,
                username, password, dbname, encoding='gbk' )


    def getStockDailyData( self, secId=None, startDate=WIND_DEFAULT_START_DATE,
            endDate=dt.date.today().strftime( WIND_DATE_FORMAT ) ):
        '''Get daily data for the given instrument in the specified date range.

Parameters
----------
secId : str
    Wind stock code; if None, get the dailyData on all the stocks;
startDate : str
    start date of the data in the format %Y%m%d;
endDate : str
    end date of the data in the format %Y%m%d.

Returns
-------
dailyData : pandas.DataFrame
    daily data for the specified stock. The order of the rows is not guaranteed.

Exceptions
----------
    raise Exception when error occurs reading the daily data.
        '''
        tableName = 'ashareeodprices'
        sql = mysql.buildSql( tableName, secId, startDate, endDate )
        df  = pd.read_sql( sql, self.conn )

        return df


    def getIndexDailyData( self, secId=None, startDate=WIND_DEFAULT_START_DATE,
            endDate=dt.date.today().strftime( WIND_DATE_FORMAT ) ):
        '''Get daily data for the given index in the specified date range.

Parameters
----------
secId : str
    Wind stock code; if None, get the dailyData on all the stocks;
startDate : str
    start date of the data in the format %Y%m%d;
endDate : str
    end date of the data in the format %Y%m%d.

Returns
-------
dailyData : pandas.DataFrame
    daily data for the specified index. The order of the rows is not guaranteed.

Exceptions
----------
    raise Exception when error occurs reading the daily data.
        '''
        tableName = 'aindexeodprices'
        sql = mysql.buildSql( tableName, secId, startDate, endDate )
        df  = pd.read_sql( sql, self.conn )

        return df


    def getFundamentals( self, tableName, secIds=None, startDate=WIND_DEFAULT_START_DATE,
            endDate=dt.date.today().strftime( WIND_DATE_FORMAT ), dateColName='REPORT_PERIOD' ):
        '''Get fundamentals for the given stocks in the specific date range.

Parameters
----------
tableName : str
    data source table;
secIds : list of str or None
    Wind stock codes whose fundamental data to retrieve; if None, get the
    fundamentals on all the stocks;
startDate : str
    start date of the data in the format %Y%m%d;
endDate : str
    end date of the data in the format %Y%m%d;
dateColName : str
    the name of the date column.

Returns
-------
fundamentals : pandas.DataFrame
    fundamental data for the stocks specified. The order of the rows is not
    guaranteed.

Exceptions
----------
    raise Exception when error occurs reading the data.
        '''
        if secIds is None:
            sql = mysql.buildSql( tableName, secIds, startDate, endDate,
                    dateColumn=dateColName )
            df  = pd.read_sql( sql, self.conn )
        else:
            dfs   = []
            # batch the query 100 per query
            step  = 100
            index = 0
            total = len( secIds )
            while index < total:
                sql = mysql.buildSqlWithSecIds( tableName,
                        secIds[ index : index + step ], startDate=startDate,
                        endDate=endDate, dateColumn=dateColName )
                dfs.append( pd.read_sql( sql, self.conn ) )
                index += step

            df = pd.concat( dfs )

        return df


    def getDailyDataOnDate( self, secIds, dataDate=dt.date.today().strftime( WIND_DATE_FORMAT ) ):
        '''Get daily data for all instruments on the given date.

Parameters
----------
secIds : list of str
    All instruments to read;
dataDate : str
    data date in the format %Y%m%d.

Returns
-------
dailyData : pandas.DataFrame
    daily data for the specific trading date.
        '''
        tableName = 'ashareeodprices'
        dfs   = []
        step  = 100
        index = 0
        total = len( secIds )
        while index < total:
            sql = mysql.buildSqlWithSecIds( tableName, secIds[ index : index + step ],
                    startDate=dataDate, endDate=dataDate )
            dfs.append( pd.read_sql( sql, self.conn ) )
            index += step

        df = pd.concat( dfs ).drop_duplicates( [ 'S_INFO_WINDCODE' ] )
        # sort by securities identifier
        df.set_index( [ 'S_INFO_WINDCODE' ], inplace=True )

        return df


    def getDailyDataWithFields( self, secIds, fields, startDate, endDate, tableName='ashareeodprices' ):
        '''Get selected data on the given date.

Parameters
----------
secIds : list of str
    All securities concerned;
fields : list of str
    fields concerned, if not specified (None), all columns are extracted;
startDate : str
    start data date in the format %Y%m%d;
endDate : str
    end data date in the format %Y%m%d;
tableName : str
    name of the data table, by default, ashareeodprices.

Returns
-------
pivot : pandas.DataFrame
    panel with three dimension
1. data filed;
2. data date, sorted by date ascendingly;
3. sec id.
        '''
        dfs   = []
        step  = 100
        index = 0
        total = len( secIds )
        while index < total:
            sql = mysql.buildSqlWithSecIds( tableName, secIds[ index : index + step ],
                    startDate=startDate, endDate=endDate, dataColumns=fields )
            dfs.append( pd.read_sql( sql, self.conn ) )
            index += step

        df = pd.concat( dfs ).drop_duplicates( [ 'TRADE_DT', 'S_INFO_WINDCODE' ] )
        df.sort_values( 'TRADE_DT', inplace=True, ascending=True )

        return df.pivot( 'TRADE_DT', 'S_INFO_WINDCODE' )


    def getDividendInformation( self, secId=None, startDate=DEFAULT_START_DATE, endDate=dt.date.today(),
            realizedOnly=True ):
        '''Get dividend information from Wind database.

Parameters
----------
secId : str
    Wind stock code;
startDate : datetime.date
    start date of the dividend data;
endDate : datetime.date
    end date of the dividend data.

Returns
-------
dividendInfo : pandas.DataFrame
    All dividend info in pandas DataFrame.
        '''
        tableName = 'asharedividend'
        sql = mysql.buildSql( tableName, secId, startDate, endDate, dateColumn='EX_DT' )
        if realizedOnly:
            # modify the SQL to constraint the row range.
            sql += " AND S_DIV_PROGRESS='3'"

        df = pd.read_sql( sql, self.conn )

        return df


    def getRightIssueInformation( self, secId=None, startDate=DEFAULT_START_DATE, endDate=dt.date.today(),
            realizedOnly=True ):
        '''Get dividend information from Wind database.

Parameters
----------
secId : str
    Wind stock code;
startDate : datetime.date
    start date of the dividend data;
endDate : datetime.date
    end date of the dividend data.

Returns
-------
dividendInfo : pandas.DataFrame
    All dividend info in pandas DataFrame.
        '''
        tableName = 'asharerightissue'
        sql = mysql.buildSql( tableName, secId, startDate, endDate, dateColumn='S_RIGHTSISSUE_EXDIVIDENDDATE' )
        if realizedOnly:
            # modify the SQL to constraint the row range.
            sql += " AND S_RIGHTSISSUE_PROGRESS='3'"

        df = pd.read_sql( sql, self.conn )

        return df


    def getBusinessDates( self, startDate=WIND_DEFAULT_START_DATE,
            endDate=dt.date.today().strftime( WIND_DATE_FORMAT ) ):
        '''Get business dates during the given date range.

Parameters
----------
startDate : str
    Start date of the business date range in the format '%Y%m%d';
endDate : str
    end date of the business dates range in the format '%Y%m%d'.

Returns
-------
businessDates : pandas.Series
    All business dates during the date range.
        '''
        sql = "SELECT * FROM asharecalendar WHERE TRADE_DAYS >= '{sd:s}' AND TRADE_DAYS <= '{ed:s}'".format(
                sd=startDate, ed=endDate )
        df  = pd.read_sql( sql, self.conn )

        # since 2012-01-04, Shanghai stock exchange and Shenzhen stock exchange share the
        # same trading calendar.
        return df[ df.S_INFO_EXCHMARKET == 'SSE' ].TRADE_DAYS.sort_values()


    def getDelistedStocks( self, startDate=WIND_DEFAULT_START_DATE,
            endDate=dt.date.today().strftime( WIND_DATE_FORMAT ) ):
        '''Get delisted stocks from the given start date to now.

Parameters
----------
startDate : str
    Start date of the business date range in the format %Y%m%d;
endDate : datetime.date
    end date of the business dates range in the format %Y%m%d.

Returns
-------
delistedStocks : pandas.Series
    delisted stocks in a pandas Series with S_INFO_DELISTDATE indexed by stock codes.
        '''
        sql = "SELECT S_INFO_WINDCODE, S_INFO_DELISTDATE FROM asharedescription WHERE S_INFO_DELISTDATE >= '{sd:s}' AND S_INFO_DELISTDATE <= '{ed:s}'".format( sd=startDate, ed=endDate )
        df  = pd.read_sql( sql, self.conn )
        delistedStocks = df.S_INFO_DELISTDATE
        delistedStocks.index = df.S_INFO_WINDCODE

        return delistedStocks


    def getSuspensionDates( self, startDate=WIND_DEFAULT_START_DATE,
            endDate=dt.date.today().strftime( WIND_DATE_FORMAT ) ):
        '''Get suspending dates during the given date range.

Parameters
----------
startDate : str
    Start date of the business date range in the format %Y%m%d;
endDate : datetime.date
    end date of the business dates range in the format %Y%m%d.

Returns
-------
df : pandas.DataFrame
    All suspending dates during the date range.
        '''
        sql = "SELECT S_INFO_WINDCODE, S_DQ_SUSPENDDATE FROM asharetradingsuspension WHERE S_DQ_SUSPENDDATE >= '{sd:s}' AND S_DQ_SUSPENDDATE <= '{ed:s}'".format(
                sd=startDate, ed=endDate )
        df  = pd.read_sql( sql, self.conn )

        return df


class CachedWindSource( WindDataSource ):
    '''In memory Wind source, which loads the necessary Wind data in batch and stores in memory.
    '''

    def __init__( self, secIds, startDate, endDate ):
        '''Initialize a in-memory data source.

Parameters
----------
secIds : list of str
    All securities concerned;
startDate : str
    backtest start date in the format %Y%m%d;
endDate : str
    backtest end date in the format %Y%m%d.
        '''
        super( CachedWindSource, self ).__init__()

        # calculate data start date and end date based on the backtest date
        stockCalendar = uc.AShareTradingCalendar( self,
                startDate=uc.DEFAULT_TS_START_DATE )
        # to calculate the data start date, preceed backtest start date by 100-day.
        try:
            dataStartDate = stockCalendar.prevTradingDate( startDate, n=100 )
        except Exception:
            dataStartDate = startDate
        try:
            dataEndDate   = stockCalendar.nextTradingDate( endDate )
        except Exception:
            dataEndDate   = endDate

        # load all data into memory
        dfs   = []
        step  = 100
        index = 0
        total = len( secIds )
        tableName = 'ashareeodprices'
        while index < total:
            sql = mysql.buildSqlWithSecIds( tableName, secIds[ index : index + step ],
                    startDate=dataStartDate, endDate=dataEndDate )
            df = pd.read_sql( sql, self.conn )
            dfs.append( df )
            index += step

        df = pd.concat( dfs )
        df.sort_values( 'TRADE_DT', inplace=True, ascending=True )

        self.data = df


    def getStockDailyData( self, secId=None, startDate=WIND_DEFAULT_START_DATE,
            endDate=dt.date.today().strftime( WIND_DATE_FORMAT ) ):
        '''Get daily data for the given instrument in the specified date range.

Parameters
----------
secId : str
    Wind stock code; if None, get the dailyData on all the stocks;
startDate : str
    start date of the data in the format %Y%m%d inclusively;
endDate : str
    end date of the data in the format %Y%m%d inclusively.

Returns
-------
dailyData : pandas.DataFrame
    daily data for the specified index. The order of the rows is not guaranteed.

Exceptions
----------
    raise Exception when error occurs reading the daily data.
        '''
        dataFilter = ( self.data.TRADE_DT >= startDate ) & ( self.data.TRADE_DT <= endDate )
        dailyData  = self.data[ dataFilter ]
        if secId is not None:
            dailyData = dailyData[ dailyData.S_INFO_WINDCODE == secId ]

        return dailyData


    def getDailyDataOnDate( self, secIds, dataDate=dt.date.today().strftime( WIND_DATE_FORMAT ) ):
        '''Get daily data for all instruments on the given date.

Parameters
----------
secIds : list of str
    All instruments to read;
dataDate : str
    data date in the format %Y%m%d.

Returns
-------
dailyData : pandas.DataFrame
    daily data for the specific trading date.
        '''
        dataFilter = self.data.S_INFO_WINDCODE.isin( secIds ) & \
                ( self.data.TRADE_DT == dataDate )

        df = self.data[ dataFilter ]
        # sort by securities identifier
        df.set_index( [ 'S_INFO_WINDCODE' ], inplace=True )

        return df


    def getDailyDataWithFields( self, secIds, fields, startDate, endDate, tableName='ashareeodprices' ):
        '''Get selected data on the given date.

Parameters
----------
secIds : list of str
    All securities concerned;
fields : list of str
    fields concerned, if not specified (None), all columns are extracted;
startDate : str
    start data date in the format %Y%m%d;
endDate : str
    end data date in the format %Y%m%d;
tableName : str
    name of the data table, by default, ashareeodprices.

Returns
-------
data : pandas.DataFrame.pivot
    requested data in pivoted DataFrame.
        '''
        dataWithField = self.data[ fields + [ 'S_INFO_WINDCODE', 'TRADE_DT' ] ]
        dataFilter    = ( dataWithField.TRADE_DT >= startDate ) & \
                ( dataWithField.TRADE_DT <= endDate )

        dataWithField = dataWithField[ dataFilter ]
        dataWithField = dataWithField[ dataWithField.S_INFO_WINDCODE.isin( secIds ) ]

        return dataWithField.pivot( 'TRADE_DT', 'S_INFO_WINDCODE' )
