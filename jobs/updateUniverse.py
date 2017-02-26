'''This script updates universe data.
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
import logging
import pandas   as pd

# third-party modules

# customized modules
from data.config   import *
from data.driver   import mongodb
from data.universe import stocks
from data.universe import futures

# customize logging configure
logging.basicConfig( format='[%(levelname)s] %(message)s', level=logging.INFO )

DB_NAME = 'universe'

def updateStockUniverse( asOfDate ):
    '''Update stock universe data.

Parameters
----------
asOfDate : datetime.date
    Data date.

Returns
-------
None
    '''
    # get the universe data.
    industryClassification = stocks.getIndustryClassification()
    stockInfo              = stocks.getStocks()

    # extract MongoDB credential
    username, password = MONGODB_CRED
    db = mongodb.getAuthenticatedConnection( MONGODB_URL, MONGODB_PORT,
                                             username, password, DB_NAME )
    # write to MongoDB
    # the data is enhanced with an asOfDate date and the market identifier
    mongoDate = dt.datetime.combine( asOfDate,
                           dt.datetime.min.time() )
    record = { 'Date': mongoDate,
               'Stocks':  stockInfo.to_json(),
               'Sectors': industryClassification.to_json(),
               'Country': 'CN' }

    db.stocks.update( { 'Date': mongoDate, 'Country': 'CN' }, record, upsert=True )


def updateFuturesUniverse( asOfDate ):
    '''Update futures universe data.

Parameters
----------
asOfDate : datetime.date
    Data date.

Returns
-------
None
    '''
    futData = []

    for exch in futures.FUTURES_EXCHANGES:
        logging.info( 'Get futures contracts in {e:s}...'.format( e=exch ) )
        exchFutures = futures.getFuturesContracts( exch )
        futData.append( exchFutures )

    futData = pd.concat( futData )
    futData.reset_index( drop=True, inplace=True )

    # write to MongoDB
    username, password = MONGODB_CRED
    db = mongodb.getAuthenticatedConnection( MONGODB_URL, MONGODB_PORT,
                                              username, password, DB_NAME )

    mongoDate = dt.datetime.combine( asOfDate,
                           dt.datetime.min.time() )
    record = { 'Date': mongoDate,
               'Data': futData.to_json(),
               'Country': 'CN' }

    db.futures.update( { 'Date': mongoDate, 'Country': 'CN' }, record, upsert=True )


def main():
    '''Entry point of the job.
    '''
    # runtime date
    asOfDate = dt.date.today()

    logging.info( 'Update stock universe asof {d:s}...'.format( d=str( asOfDate ) ) )
    # update stock universe
    updateStockUniverse( asOfDate )

    logging.info( 'Update futures universe asof {d:s}...'.format( d=str( asOfDate ) ) )
    # update futures universe
    updateFuturesUniverse( asOfDate )

if __name__ == '__main__':
    main()
