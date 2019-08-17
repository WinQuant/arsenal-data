'''This script encapsulates all the utility functions related to SQLite3 operation.
'''

'''
Copyright (c) 2019, WinQuant Information and Technology Co. Ltd.
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
import sqlalchemy

# customized modules
import data.config.sqlite3 as sqlite3Config

def getAuthenticatedConnection( sqliteUrl, port=None, username=None,
        password=None, dbname=None, encoding='utf8' ):
    '''Get MySQL connection and authenticate the connection.

Parameters
----------
sqliteUrl : str
    SQLite3 db file address;
port : int
    None;
username : str
    None;
password : str
    None;
dbname : str
    None;
encoding : str
    encoding of the connection.

Returns
-------
db : sqlalchemy.engine.base.Engine
    SQLite3 engine connection.

Exceptions
----------
    raise Exception when authentication failed or cannot connect to server.
    '''
    conn = sqlalchemy.create_engine( 'sqlite:///{dburl:s}'.format(
            dburl=sqliteUrl ) )

    return conn

def buildBinDataSql( tableName, secId, startDate, endDate, binSize,
        instColumn='sec_id', dateColumn='tradedate', binColumn='kl_period_id' ):
    '''Build an SQL query for bin data to be executable.

Parameters
----------
tableName : str
    name of the data table in the SQL database;
secId : str
    stock code;
startDate : str
    start date of the data series in the format %Y%m%d;
endDate : str
    end date of the data series in the format %Y%m%d;
binSize : int
    number of minute in a bin;
instColumn : str
    column name of the instrument identifier;
dateColumn : str
    column name of the date;
binColumn : str
    period to distinguish bin size.

Returns
-------
sql : str
    SQL to query.
    '''
    sql = "SELECT * FROM {tablename:s} WHERE {dc:s}>='{sd:s}' AND {dc:s}<='{ed:s}'".format(
            tablename=tableName, dc=dateColumn, sd=startDate, ed=endDate )

    if secId is not None:
        sql += " AND {sn:s}='{sid:s}'".format( sn=instColumn, sid=secId )

    return sql

def buildSql( tableName, secId, startDate, endDate,
        stockColumn='S_INFO_WINDCODE', dateColumn='TRADE_DT' ):
    '''Build an SQL query to be executable.

Parameters
----------
tableName : str
    name of the data table in the SQL database;
secId : str
    stock code;
startDate : str
    start date of the data series in the format %Y%m%d;
endDate : str
    end date of the data series in the format %Y%m%d;
stockColumn : str
    column name of the stock identifier;
dateColumn : str
    column name of the date;
dateFormat : str
    date format.

Returns
-------
sql : str
    SQL to query.
    '''
    raise Exception( 'Method `buildSql` not implemented' )

def buildSqlWithSecIds( tableName, secIds, startDate, endDate,
        stockColumn='S_INFO_WINDCODE', dateColumn='TRADE_DT', dataColumns=None ):
    '''Build an SQL query to be executable.

Parameters
----------
tableName : str
    name of the data table in the SQL database;
secIds : list of str
    all securities identifiers to be constrained;
startDate : str
    start date in the format %Y%m%d inclusively;
endDate : str
    end date in the format %Y%m%d inclusively;
stockColumn : str
    column name of the stock identifier;
dateColumn : str
    column name of the date.
dataColumns : list of str
    data column to get, by default all columns with None.

Returns
-------
sql : str
    SQL to query.
    '''
    raise Exception( 'Method `buildSqlWithSecIds` not implemented' )
