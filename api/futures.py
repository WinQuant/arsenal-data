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
import logging
import re

# third-party modules
import numpy  as np
import pandas as pd

# customized modules
import data.api.base as base
import data.config   as config
import data.driver.mongodb as dMongodb
import data.driver.mysql   as mysql

# customize logging configure
logging.basicConfig( format='[%(levelname)s] %(message)s', level=logging.INFO )

TICK_DEFAULT_START_DATE = '20150101'
TICK_DATE_FORMAT        = '%Y%m%d'

WIND_DEFAULT_START_DATE = '20120101'
WIND_DATE_FORMAT        = '%Y%m%d'

FUTURES_PRODUCTS = [ 'AG', 'AL', 'AU', 'BU', 'CU', 'FU', 'HC', 'NI',
                     'PB', 'RB', 'RU', 'SN', 'WR', 'ZN',  # XSGE
                     'CF', 'RI', 'FG', 'JR', 'LR', 'MA', 'OI', 'PM',
                     'RM', 'RS', 'SF', 'SM', 'SR', 'TA', 'TC', 'WH',
                                                          # XZCE
                     'IC', 'IF', 'IH', 'T', 'TF',         # CCFX
                     'A',  'B',  'BB', 'C', 'CS', 'FB', 'I', 'J',
                     'JD', 'JM', 'L',  'M', 'P',  'PP', 'V', 'Y' ]
                                                          # XDCE

@functools.lru_cache( maxsize=32 )
def getFuturesInformation( asOfDate, ticker=None, listed=True, country='CN' ):
    '''Get all futures contract information.

Parameters
----------
asOfDate : datetime.date
    Data date of the futures information or
    None if not found;
ticker : str
    Ticker name of the futures, if not given, return all futures;
listed : bool or None
    Contract listed status, True for listed contracts by the asOfDate, otherwise return all available
    contracts;
country : str
    Country identifier, currently, only CN supported.

Returns
-------
futuresInfo : pandas.DataFrame
    Futures information.

Exceptions
----------
    raise Exception when duplicated records found.
    '''
    username, password = config.MONGODB_CRED
    db = dMongodb.getAuthenticatedConnection( config.MONGODB_URL,
            config.MONGODB_PORT, username, password, 'universe' )

    # find the latest records
    validDate = dt.datetime( 2020, 1, 1 )
    data = db.futures.find_one( { 'Date': { '$lte': validDate }, 'Country': 'CN' },
        sort=[ ( 'Date', dMongodb.pymongo.DESCENDING ) ] )

    futuresInfo = pd.read_json( data[ 'Data' ] )
    futuresInfo.sort_index( inplace=True )

    if ticker is not None:
        futuresInfo = futuresInfo[ futures.ticker == ticker ]
        futuresInfo.reset_index( drop=True, inplace=True )

    if listed:
        sAsOfDate = str( asOfDate )
        isListed  = np.logical_and( futuresInfo.listDate <= sAsOfDate,
                                       futuresInfo.lastTradeDate >= sAsOfDate )
        futuresInfo = futuresInfo[ isListed ]
        futuresInfo.reset_index( drop=True, inplace=True )

    return futuresInfo


def getFuturesProducts():
    '''Get all futures product.

Returns
futuresProduct : list of str
    All futures product supported.
    '''
    return FUTURES_PRODUCTS


def getTradingDates():
    '''Get futures trading dates.

Returns
-------
tradingDates : list of datetime.date
    return list of trading dates in datetime.date.

Exceptions
----------
    raise Exception when duplicated records found.
    '''
    username, password = config.MONGODB_CRED
    db = dMongodb.getAuthenticatedConnection( config.MONGODB_URL,
            config.MONGODB_PORT, username, password, 'universe' )

    cursor = db.refdata.find( { 'Name': 'TradingDates', 'Country': 'CN' } )
    nRecords = cursor.count()
    if nRecords == 0:
        tradingDates = None
    elif nRecords > 1:
        raise Exception( 'Duplicated {n:d} records found.'.format(
                         n=nRecords ) )
    else:
        data = cursor.next()
        tradingDates = [ d.date() for d in data[ 'Data' ] ]

    return tradingDates


def getDailyData( secId, startDate=dt.date( 2012, 1, 1 ),
        endDate=dt.date.today() ):
    '''Get daily data for the given futures during the date range.

Parameters
----------
secId : str
    Security ID of the futures;
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
    raise Exception when duplicated records found on the given futures name.
    '''
    # Get authenticated MongoDB connection
    username, password = config.MONGODB_CRED
    db = dMongodb.getAuthenticatedConnection( config.MONGODB_URL, config.MONGODB_PORT,
        username, password, 'dailyData' )

    # Query data
    cursor = db.futures.find( { 'SecID': secId } )

    # Sanity check
    nRecords = cursor.count()
    if nRecords == 0:
        dailyData = None
    elif nRecords > 1:
        raise Exception( 'Duplicated {n:d} records found for futures {s:s}.'.format(
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
        # reset index from 0 onwards
        dailyData.reset_index( drop=True, inplace=True )
        
    return dailyData



def getMainContractDailyData( product, startDate=dt.date( 2012, 1, 1 ),
        endDate=dt.date.today() ):
    '''Get daily data for the given product main futures contract during the date range.

Parameters
----------
product : str
    Product ID;
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
    username, password = config.MONGODB_CRED
    db = dMongodb.getAuthenticatedConnection( config.MONGODB_URL, config.MONGODB_PORT,
        username, password, 'dailyData' )

    # Query data
    cursor = db.futures.find( { 'Product': product, 'MainContract': 1 } )

    # Sanity check
    nRecords = cursor.count()
    if nRecords == 0:
        dailyData = None
    elif nRecords > 1:
        raise Exception( 'Duplicated {n:d} records found for futures {s:s}.'.format(
                         n=nRecords, s=product ) )
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


def getBinData( secId, startDate=dt.date( 2012, 1, 1 ), endDate=dt.date.today() ):
    '''Get minute-by-minute data for the given futures during the date range.

Parameters
----------
secId : str
    Security ID of the futures;
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
    raise Exception when duplicated records found on the given futures name.
    '''
    # Get authenticated MongoDB connection
    username, password = config.MONGODB_CRED
    db = dMongodb.getAuthenticatedConnection( config.MONGODB_URL,
            config.MONGODB_PORT, username, password, 'binData' )

    # Query data
    cursor = db.futures.find( { 'SecID': secId, 'Date': {
        '$gte': dt.datetime.combine( startDate, dt.datetime.min.time() ),
        '$lte': dt.datetime.combine( endDate, dt.datetime.min.time() ) } } )

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
    '''Get futures bin data from the database.
    '''

    def __init__( self ):
        '''Initialize a BinDataSource for futures.
        '''
        super( BinDataSource, self ).__init__( 'indicator_future' )


class WindDataSource( object ):
    '''Get data from Wind.
    '''

    def __init__( self ):
        '''Initialize a WindDataSource object.
        '''
        username, password = config.MYSQL_WIND_CRED
        dbname = 'wind'
        self.conn = mysql.getAuthenticatedConnection( config.MYSQL_WIND_URL,
                config.MYSQL_WIND_PORT, username, password, dbname )


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
        sql = "SELECT * FROM cfuturescalendar WHERE TRADE_DAYS >= '{sd:s}' AND TRADE_DAYS <= '{ed:s}' ORDER BY TRADE_DAYS ASC".format(
                sd=startDate, ed=endDate )
        df  = pd.read_sql( sql, self.conn )

        # since 20070101, Shanghai Futures Exchange, Dalian Commodities Exchange, Zhengzhou Commodities Exchange,
        # and China Financial Futures Exchange share the same business days.
        return df[ df.S_INFO_EXCHMARKET == 'SHFE' ].TRADE_DAYS


    def getUpDownLimit( self, secId=None, startDate=WIND_DEFAULT_START_DATE,
            endDate=dt.date.today().strftime( WIND_DATE_FORMAT ) ):
        '''Get up and down limit data for the given instrument in the specified date range.

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
upDownLimit : pandas.DataFrame
    up-down limit data for the specified stock. The order of the rows is not guaranteed.

Exceptions
----------
    raise Exception when error occurs reading the daily data.
        '''
        tableName = 'cfuturespricechangelimit'
        sql = mysql.buildSql( tableName, secId, startDate, endDate, dateColumn='CHANGE_DT' )
        df  = pd.read_sql( sql, self.conn )
        df.S_INFO_WINDCODE = df.S_INFO_WINDCODE.apply( lambda secId: secId.split( '.' )[ 0 ] )

        return df


    def getFuturesInfo( self, secId=None, tableName='cfuturescontpro' ):
        '''Get futures fundemental information.

Parameters
----------
secId : str
    Wind stock code; if None, get the dailyData on all the stocks.

Returns
-------
fundamentalData : pandas.DataFrame
    fundamental data for the specified futures instrument. The order of the rows is not guaranteed;
tableName : str
    name of the table to read futures fundamental data.

Exceptions
----------
    raise Exception when error occurs reading the daily data.
        '''
        sql = 'SELECT * FROM {tn:s}'.format( tn=tableName )
        df  = pd.read_sql( sql, self.conn )

        if tableName == 'cfuturescontpro':
            df.S_INFO_MFPRICE  = df.S_INFO_MFPRICE.apply( self._extractTick )
        df.S_INFO_WINDCODE = df.S_INFO_WINDCODE.apply( lambda secId: secId.split( '.' )[ 0 ] )

        return df


    def getMarginInfo( self, secId=None, startDate=WIND_DEFAULT_START_DATE,
            endDate=dt.date.today().strftime( WIND_DATE_FORMAT ) ):
        '''Get futures margin information.

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
marginInfo : pandas.DataFrame
    margin information for the specified stock. The order of the rows is not guaranteed.

Exceptions
----------
    raise Exception when error occurs reading the daily data.
        '''
        sql = 'SELECT * FROM cfuturesmarginratio'
        df  = pd.read_sql( sql, self.conn )
        # convert to percentage
        df.MARGINRATIO     = df.MARGINRATIO.apply( float ) / 100.0
        df.S_INFO_WINDCODE = df.S_INFO_WINDCODE.apply( lambda secId: secId.split( '.' )[ 0 ] )

        return df


    def _extractTick( self, s ):
        '''Extract tick size for the contract.

Parameters
----------
s : str
    Tick description for the contract.

Returns
-------
tick : float
    Tick size.
        '''
        pattern = re.compile( '^[0-9]*(\.[0-9]*)?' )
        pobj    = pattern.match( s )

        tick    = s[ pobj.start() : pobj.end() ]
        return float( tick ) if len( tick ) else np.nan
