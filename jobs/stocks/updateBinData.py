'''This job updates the minute-by-minute trading data for the whole stock universe.
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

# third-party modules

# customized modules
import data.api.stocks as stockApi
import data.config     as config
import data.driver.mongodb as mongodb
import data.instrument.trading.stocks as stockTrading

# customize logging configure
logging.basicConfig( format='[%(levelname)s] %(message)s', level=logging.INFO )

def main():
    '''Entry point of the job.
    '''
    # runtime
    asOfDate = dt.date.today()
    logging.info( 'Updating minute bin data for stocks on date {d:s}...'.format( d=str( asOfDate ) ) )

    # get all stocks in the universe
    universe = stockApi.getExchangeStockNames( asOfDate )

    # initialize MongoDB connection
    username, password = config.MONGODB_CRED
    db = mongodb.getAuthenticatedConnection( config.MONGODB_URL,
        config.MONGODB_PORT, username, password, 'binData' )

    nStocks = len( universe )
    logging.info( 'Minute bin volume for {ns:d} stocks in total to be updated...'.format( ns=nStocks ) )

    # for bin data, stocks are updated one-by-one
    for i, stock in enumerate( universe ):
        logging.info( 'Updating minute bin data for {s:s} ({idx:d}/{n:d})...'.format( s=stock, idx=i + 1, n=nStocks ) )

        data = stockTrading.getBinData( stock, dataDate=asOfDate )

        mongoDate = dt.datetime.combine( asOfDate, dt.datetime.min.time() )
        record    = { 'SecID': stock,
                      'Date':  mongoDate,
                      'Data':  data.to_json(),
                      'Country': 'CN' }

        db.stocks.update( { 'SecID': stock, 'Date': mongoDate, 'Country': 'CN' }, record, upsert=True )

    logging.info( 'All stocks updated.' )


if __name__ == '__main__':
    # let's kick off the job
    main()
