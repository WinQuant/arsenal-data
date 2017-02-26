'''This script holds the way to extract stock universe.
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

# third-party modules
import pandas as pd

# customized modules
import data.config.mysql as mConfig
import data.driver.mysql as mDriver


class Universe( object ):
    '''Abstraction of the way to get universe.
    '''

    def __init__( self ):
        '''Initialize a universe.
        '''
        super( Universe, self ).__init__()


    def getUniverse( self, asOfDate ):
        '''Get universe on the given date.

Parameters
----------
asOfDate : str
    Data date.

Returns
-------
universe : set of str
    A set of stock names in the universe.
        '''


    def getWholeUniverse( self ):
        '''Get all stocks once appeared in the universe.

Returns
-------
wholeUniverse : set of str
    A set of stock names once appear in the universe.
        '''


class FixedUniverse( Universe ):
    '''Fixed universe.
    '''

    def __init__( self, universe ):
        '''Initialize a fixed universe.

Parameters
----------
universe : set of str
    A set of stock names in the universe.
        '''
        super( FixedUniverse, self ).__init__()

        self.universe = universe


    def getUniverse( self, asOfDate ):
        '''Get universe on the given date.

Parameters
----------
asOfDate : str
    Data date.

Returns
-------
universe : set of str
    A set of stock names in the universe.
        '''
        return self.universe


class StockUniverse( Universe ):
    '''Stock universe.
    '''

    INDEX_DATA_PATH = YOUR_INDEX_FILE_PATH_IF_A_FILE_BASED_DATA_SOURCE

    def __init__( self, universeShortName ):
        '''Inititalize stock universe.

Parameters
----------
universeShortName : str
    Short name of the universe, could be
    * 上证综指
    * 上证50
    * 上证180
    * 沪深300
    * 中证500
        '''
        self.shortName = universeShortName
        indexWeights   = pd.read_csv( self.INDEX_DATA_PATH )
        self.universe  = indexWeights[ indexWeights.sec_short_name == self.shortName ]
        # surpress the SettingWithCopyWarning warning
        self.universe.is_copy = False
        self.universe[ 'SYMBOL' ] = self.universe.CONS_TICKER_SYMBOL.apply(
                lambda s: '.'.join( [ '%06d' % s, 'SH' if s >= 600000 else 'SZ' ] ) )

        self.wholeUniverse = set( self.universe.SYMBOL )


    def getUniverse( self, asOfDate ):
        '''Get universe on the given date.

Parameters
----------
asOfDate : str
    Data date in the format %Y-%m-%d.

Returns
-------
universe : set of str
    A set of stock symbols in the universe, SH for Shanghai exchange
and SZ for Shenzhen exchange.
        '''
        selectedUniverse = self.universe[ self.universe.EFF_DATE == asOfDate ]

        universe = set( selectedUniverse.SYMBOL )
        return universe


    def getWholeUniverse( self ):
        '''Get all stocks once appear in the universe.


Returns
-------
wholeUniverse : set of str
    A set of stock names once appear in the universe.
        '''
        return self.wholeUniverse


class WindStockUniverse( Universe ):
    '''Stock universe backed by Wind database.
    '''

    INDEX_NAME_MAPPING = { u'上证综指': '000001.SH', u'上证50': '000016.SH',
            u'上证180': '000010.SH', u'沪深300': '399300.SZ',
            u'中证500': '000905.SH' }

    def __init__( self, universeShortName ):
        '''Initialize the Wind stock universe.

Parameters
----------
universeShortName : str
    Short name of the universe, could be one of
    * 上证综指
    * 上证50
    * 上证180
    * 沪深300
    * 中证500

Exceptions
----------
raise Exception when the given short name is not recognized.
        '''
        super( WindStockUniverse, self ).__init__()
        if universeShortName in self.INDEX_NAME_MAPPING:
            indexCode = self.INDEX_NAME_MAPPING[ universeShortName ]
            username, password = mConfig.MYSQL_WIND_CRED
            dbname = 'wind'

            conn = mDriver.getAuthenticatedConnection( mConfig.MYSQL_WIND_URL,
                    mConfig.MYSQL_WIND_PORT, username, password, dbname )

            sql  = "SELECT * FROM aindexhs300freeweight WHERE S_INFO_WINDCODE='{ic:s}'".format(
                    ic=indexCode )
            self.indexWeights  = pd.read_sql( sql, conn )
            self.tradeDates    = self.indexWeights.TRADE_DT.sort_values().unique()
            self.wholeUniverse = set( self.indexWeights.S_CON_WINDCODE )
        else:
            raise Exception( u'Unrecognized universe name {un:s}.'.format( un=shortUniverseName ) )


    def getUniverse( self, asOfDate ):
        '''Get universe on the given date.

Parameters
----------
asOfDate : str
    Data date in the format %Y-%m-%d or %Y%m%d.

Returns
-------
universe : set of str
    A set of stock symbols in the universe, SH for Shanghai exchange and
SZ for Shenzhen exchange.
        '''
        # drop all '-'s if available
        expectedDate = asOfDate.replace( '-', '' )
        indexDate = self.tradeDates[ self.tradeDates <= expectedDate ][ -1 ]

        universe  = self.indexWeights[ self.indexWeights.TRADE_DT == indexDate ]

        return set( universe.S_CON_WINDCODE )


    def getWholeUniverse( self ):
        '''Get all stocks once appear in the universe.

Returns
-------
wholeUniverse : set of str
    A set of stock names once appear in the universe.
        '''
        return self.wholeUniverse


    def getCompositeWeights( self, asOfDate ):
        '''Get weights of the universe composites on the given date.

Parameters
----------
asOfDate : str
    Data date in the format %Y-%m-%d or %Y%m%d.

Returns
-------
weights : pandas.Series
    A weight series in the universe indexed by RIC name, SH for Shanghai
exchange and SZ for Shenzhen exchange.
        '''
        # drop all '-'s if available
        expectedDate = asOfDate.replace( '-', '' )
        indexDate = self.tradeDates[ self.tradeDates <= expectedDate ][ -1 ]

        universe = self.indexWeights[ self.indexWeights.TRADE_DT == indexDate ]
        weights  = pd.Series( universe.I_WEIGHT / 100, index=universe.S_CON_WINDCODE )

        return weights


class WindStockWholeAUniverse( Universe ):
    '''Get the whole-A stock universe.
    '''

    def __init__( self ):
        '''Initialize a stock whole-A universe backed by the Wind database.
        '''
        super( WindStockWholeAUniverse, self ).__init__()

        username, password = mConfig.MYSQL_WIND_CRED
        dbname = 'wind'

        conn = mDriver.getAuthenticatedConnection( mConfig.MYSQL_WIND_URL,
                mConfig.MYSQL_WIND_PORT, username, password, dbname )

        sql = 'SELECT * FROM asharedescription'
        self.universe = pd.read_sql( sql, conn )
        self.wholeUniverse = set( self.universe.S_INFO_WINDCODE )


    def getUniverse( self, asOfDate ):
        '''Get the whole A stock universe as of the given date.

Parameters
----------
asOfDate : str
    Data date in the format %Y-%m-%d or %Y%m%d.

Returns
-------
universe : set of str
    A set of stock symbols in the universe as of the given date, SH for
Shanghai exchange and SZ for Shenzhen exchange.
        '''
        expectedDate = asOfDate.replace( '-', '' )
        listDate   = self.universe.S_INFO_LISTDATE
        delistDate = self.universe.S_INFO_DELISTDATE
        dateIndex  = ( listDate <= expectedDate ) & \
                     ( delistDate.isnull() | ( delistDate > expectedDate ) )

        return set( self.universe[ dateIndex ].S_INFO_WINDCODE )


    def getWholeUniverse( self ):
        '''Get all stocks once appeared in the universe.

Returns
-------
universe : set of str
    A set of stock names once appeared in the universe.
        '''
        return self.wholeUniverse
