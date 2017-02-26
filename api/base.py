'''Base class of the data API.
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

# third-party modules
import numpy  as np
import pandas as pd

# customized modules
import data.config as config
import data.driver.mysql as mysql


TICK_DEFAULT_START_DATE = '20150101'
TICK_DATE_FORMAT        = '%Y%m%d'


class BinDataSource( object ):
    '''Get bin data from the database.
    '''

    def __init__( self, databaseName ):
        '''Initialize a BinDataSource object.

Parameters
----------
databaseName : str
    Initialize the data source with the given 
        '''
        super( BinDataSource, self ).__init__()

        username, password = config.MYSQL_TICK_CRED
        # connection to tick data database
        dbname = databaseName
        self.binConn = mysql.getAuthenticatedConnection( config.MYSQL_TICK_URL, config.MYSQL_TICK_PORT,
                username, password, dbname )
        # connection to reference database
        dbname = 'indicator_ref'
        self.refConn = mysql.getAuthenticatedConnection( config.MYSQL_TICK_URL, config.MYSQL_TICK_PORT,
                username, password, dbname )
        refDataSql   = mysql.getRefDataSql( 'dict_market_code' )
        self.refData = pd.read_sql( refDataSql, self.refConn )
        # indexed by security identifier
        self.refData.set_index( 'sec_id', inplace=True )
        self.refConn.dispose()


    def getBinData( self, secIds, startDate=TICK_DEFAULT_START_DATE,
            endDate=dt.date.today().strftime( TICK_DATE_FORMAT ), binSize=1 ):
        '''Get bin data for the given instrument

Parameters
----------
secIds : set of str
    All instrument identifiers concerned;
startDate : str
    start data date in the format %Y%m%d inclusively;
endDate : str
    end data date in the format %Y%m%d inclusively.

Returns
-------
fullDf : pandas.DataFrame
    Data in pandas DataFrame with column
* tradeDate -- str
    trade date in the format %Y%m%d;
* timestamp -- str
    trade timestamp in the format %Y%m%d%HH%MM%ss
* seqNo -- int
    K-line sequence number;
* periodId -- int
    K-line peroid identifier;
* exchange -- str
    exchange identifier;
* mainFlag -- int
    TODO unknown indicator;
* openPrice -- int
    open price * 10000;
* closePrice -- int
    close price * 10000;
* highPrice -- int
    high price * 10000;
* lowPrice -- int
    low price * 10000;
* volume -- int
    volume traded;
* turnover -- float
    monetary volume;
* volumeSum -- int
    total volume;
* turnoverSum -- float
    total monetary volume;
* lastModified -- str
    last modification time.
indexed by secId, which is a str representation of the securities identifier;
        '''
        dfs = []
        for secId in secIds:
            tableName = self.refData.ix[ secId ]
            if len( tableName ) < 1:
                raise Exception( 'Cannot get data for {secId:s}.'.format( secId=secId ) )
            else:
                tableName = tableName.table_name

            tableName = '_'.join( [ 'kline', tableName ] )
            sql = mysql.buildBinDataSql( tableName, secId, startDate, endDate, binSize )
            df  = pd.read_sql( sql, self.binConn )
            df.sort_values( by=[ 'kl_score' ], inplace=True )

            dfs.append( df )

        fullDf = pd.concat( dfs )

        fullDf.columns = [ 'tradeDate', 'timestamp', 'seqNo', 'periodId', 'exchange', 'secId',
                'mainFlag', 'openPrice', 'closePrice', 'highPrice', 'lowPrice', 'volume',
                'turnover', 'volumeSum', 'turnoverSum', 'lastModified' ]
        # normalize the price and volume
        fullDf.secId = fullDf.secId.apply( lambda s: s.upper() )
        fullDf.openPrice  /= 10000
        fullDf.closePrice /= 10000
        fullDf.highPrice  /= 10000
        fullDf.lowPrice   /= 10000

        # set trade date, timestamp string
        fullDf.tradeDate = fullDf.tradeDate.apply( lambda d: str( int( d ) ) )
        fullDf.timestamp = fullDf.timestamp.apply( lambda d: str( int( d ) ) )

        fullDf.set_index( 'secId', inplace=True )

        return fullDf
