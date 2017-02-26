'''This script encapsulates all the utility functions related to
MySQL operation.
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
import sqlalchemy

# customized modules
import data.config.mysql as mysqlConfig


def getAuthenticatedConnection( mysqlUrl, port, username, password, dbname,
            driver=mysqlConfig.MYSQL_DRIVER, encoding='utf8' ):
    '''Get MySQL connection and authenticate the connection.

Parameters
----------
mysqlUrl : str
    MySQL hostname;
port : int
    MySQL port;
username : str
    MySQL username to login;
password : str
    MySQL password to authenticate the user;
dbname : str
    MySQL database name to connect;
encoding : str
    encoding of the connection.

Returns
-------
db : sqlalchemy.engine.base.Engine
    MySQL engine connection.

Exceptions
----------
    raise Exception when authentication failed or cannot connect to server.
    '''
    conn = sqlalchemy.create_engine( '{driver:s}://{username:s}:{password:s}@{url:s}:{port:d}/{dbname:s}?charset={encoding:s}'.format(
            driver='mysql' if driver is None else 'mysql+{d:s}'.format( d=driver ),
            username=username, password=password, url=mysqlUrl, port=port, dbname=dbname,
            encoding=encoding ) )

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
    sql = "SELECT * FROM {tablename:s} WHERE {dc:s}>='{sd:s}' AND {dc:s}<='{ed:s}' AND {pc:s}={p:d}".format(
            tablename=tableName, dc=dateColumn, sd=startDate, ed=endDate, pc=binColumn, p=binSize )

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
    sql = "SELECT * FROM {tablename:s} WHERE {dc:s}>='{sd:s}' AND {dc:s}<='{ed:s}'".format(
            tablename=tableName, dc=dateColumn, sd=startDate, ed=endDate )

    if secId is not None:
        sql += " AND {sn:s}='{sid:s}'".format( sn=stockColumn, sid=secId )

    sql += ' ORDER BY {dc:s}'.format( dc=dateColumn )

    return sql


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
    sql  = "SELECT {cols:s} FROM {tablename:s} WHERE {sc:s} IN ('{secIds:s}') AND {dc:s}>='{sd:s}' AND {dc:s} <= '{ed:s}'".format(
            cols='*' if dataColumns is None else ', '.join( [ stockColumn, dateColumn ] + dataColumns ),
            tablename=tableName, sc=stockColumn, secIds="', '".join( secIds ), dc=dateColumn,
            sd=startDate, ed=endDate )

    return sql


def getRefDataSql( tableName ):
    '''Get the query to tick data database to extrat the reference data table.

Parameters
----------
tableName : str
    Name of the table to get the bin data.

Returns
-------
sql : str
    SQL to market ref data table.
    '''
    return 'SELECT * FROM {tn:s}'.format( tn=tableName )


def getTableNameSql( secId ):
    '''Get table name from the reference table.

Parameters
----------
secId : str
    security identifier.

Returns
-------
tableNameSql : str
    SQL to query the name of the table holding the data for the securities.
    '''
    return "SELECT * FROM dict_market_code WHERE sec_id='{secId:s}'".format( secId=secId )
