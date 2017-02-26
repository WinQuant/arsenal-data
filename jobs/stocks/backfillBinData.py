'''This job backfills the minute-by-minute bin data for the whole stock universe.
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

DATE_RANGE = [ ( dt.date( 2012, 1, 1 ),  dt.date( 2012, 1, 31 ) ),
               ( dt.date( 2012, 2, 1 ),  dt.date( 2012, 2, 29 ) ),
               ( dt.date( 2012, 3, 1 ),  dt.date( 2012, 3, 31 ) ),
               ( dt.date( 2012, 4, 1 ),  dt.date( 2012, 4, 30 ) ),
               ( dt.date( 2012, 5, 1 ),  dt.date( 2012, 5, 31 ) ),
               ( dt.date( 2012, 6, 1 ),  dt.date( 2012, 6, 30 ) ),
               ( dt.date( 2012, 7, 1 ),  dt.date( 2012, 7, 31 ) ),
               ( dt.date( 2012, 8, 1 ),  dt.date( 2012, 8, 31 ) ),
               ( dt.date( 2012, 9, 1 ),  dt.date( 2012, 9, 30 ) ),
               ( dt.date( 2012, 10, 1 ), dt.date( 2012, 10, 31 ) ),
               ( dt.date( 2012, 11, 1 ), dt.date( 2012, 11, 30 ) ),
               ( dt.date( 2012, 12, 1 ), dt.date( 2012, 12, 31 ) ),
               ( dt.date( 2013, 1, 1 ),  dt.date( 2013, 1, 31 ) ),
               ( dt.date( 2013, 2, 1 ),  dt.date( 2013, 2, 28 ) ),
               ( dt.date( 2013, 3, 1 ),  dt.date( 2013, 3, 31 ) ),
               ( dt.date( 2013, 4, 1 ),  dt.date( 2013, 4, 30 ) ),
               ( dt.date( 2013, 5, 1 ),  dt.date( 2013, 5, 31 ) ),
               ( dt.date( 2013, 6, 1 ),  dt.date( 2013, 6, 30 ) ),
               ( dt.date( 2013, 7, 1 ),  dt.date( 2013, 7, 31 ) ),
               ( dt.date( 2013, 8, 1 ),  dt.date( 2013, 8, 31 ) ),
               ( dt.date( 2013, 9, 1 ),  dt.date( 2013, 9, 30 ) ),
               ( dt.date( 2013, 10, 1 ), dt.date( 2013, 10, 31 ) ),
               ( dt.date( 2013, 11, 1 ), dt.date( 2013, 11, 30 ) ),
               ( dt.date( 2013, 12, 1 ), dt.date( 2013, 12, 31 ) ),
               ( dt.date( 2014, 1, 1 ),  dt.date( 2014, 1, 31 ) ),
               ( dt.date( 2014, 2, 1 ),  dt.date( 2014, 2, 28 ) ),
               ( dt.date( 2014, 3, 1 ),  dt.date( 2014, 3, 31 ) ),
               ( dt.date( 2014, 4, 1 ),  dt.date( 2014, 4, 30 ) ),
               ( dt.date( 2014, 5, 1 ),  dt.date( 2014, 5, 31 ) ),
               ( dt.date( 2014, 6, 1 ),  dt.date( 2014, 6, 30 ) ),
               ( dt.date( 2014, 7, 1 ),  dt.date( 2014, 7, 31 ) ),
               ( dt.date( 2014, 8, 1 ),  dt.date( 2014, 8, 31 ) ),
               ( dt.date( 2014, 9, 1 ),  dt.date( 2014, 9, 30 ) ),
               ( dt.date( 2014, 10, 1 ), dt.date( 2014, 10, 31 ) ),
               ( dt.date( 2014, 11, 1 ), dt.date( 2014, 11, 30 ) ),
               ( dt.date( 2014, 12, 1 ), dt.date( 2014, 12, 31 ) ),
               ( dt.date( 2015, 1, 1 ),  dt.date( 2015, 1, 31 ) ),
               ( dt.date( 2015, 2, 1 ),  dt.date( 2015, 2, 28 ) ),
               ( dt.date( 2015, 3, 1 ),  dt.date( 2015, 3, 31 ) ),
               ( dt.date( 2015, 4, 1 ),  dt.date( 2015, 4, 30 ) ),
               ( dt.date( 2015, 5, 1 ),  dt.date( 2015, 5, 31 ) ),
               ( dt.date( 2015, 6, 1 ),  dt.date( 2015, 6, 30 ) ),
               ( dt.date( 2015, 7, 1 ),  dt.date( 2015, 7, 31 ) ),
               ( dt.date( 2015, 8, 1 ),  dt.date( 2015, 8, 31 ) ),
               ( dt.date( 2015, 9, 1 ),  dt.date( 2015, 9, 30 ) ),
               ( dt.date( 2015, 10, 1 ), dt.date( 2015, 10, 31 ) ),
               ( dt.date( 2015, 11, 1 ), dt.date( 2015, 11, 30 ) ),
               ( dt.date( 2015, 12, 1 ), dt.date( 2015, 12, 31 ) ),
               ( dt.date( 2016, 1, 1 ),  dt.date( 2016, 1, 31 ) ),
               ( dt.date( 2016, 2, 1 ),  dt.date( 2016, 2, 29 ) ),
               ( dt.date( 2016, 3, 1 ),  dt.date( 2016, 3, 31 ) ) ]

def main():
    '''Entry point of the job.
    '''
    # runtime date
    asOfDate = dt.date.today()

    # get all stocks in the universe
    universe = stockApi.getExchangeStockNames( asOfDate )
    universe.sort()
    # idx = universe.index( '603085.XSHG' )
    universe = universe[ idx : ]

    # initialize MongoDB connection
    username, password = config.MONGODB_CRED
    db = mongodb.getAuthenticatedConnection( config.MONGODB_URL,
        config.MONGODB_PORT, username, password, 'binData' )

    nStocks = len( universe )
    logging.info( 'Backfill bin volume for {ns:d} stocks in total...'.format( ns=nStocks ) )

    # for bin data backfill, stocks are updated one-by-one and by month
    for i, s in enumerate( universe ):
        for startDate, endDate in DATE_RANGE:
            logging.info( 'Backfilling bin volume for {sec:s} ({idx:d}/{n:d}) from {sd:s} to {ed:s}...'.format( sec=s,
                idx=i + 1, n=nStocks, sd=str( startDate ), ed=str( endDate ) ) )
            data = stockTrading.getHistoryBinData( s, startDate=startDate,
                endDate=endDate )

            try:
                groupedData = data.groupby( [ 'dataDate' ] )
                records = []

                for dataDate, tData in groupedData:
                    record = { 'SecID': s,
                               'Date':  dt.datetime.strptime( dataDate, '%Y-%m-%d' ),
                               'Data':  tData.to_json(),
                               'Country': 'CN' }
                    records.append( record )
     
                db.stocks.insert_many( records )
            except KeyError as e:
                logging.warning( 'Error when updating {sec:s} from {sd:s} to {ed:s}: {msg:s}.'.format(
                    sec=s, sd=str( startDate ), ed=str( endDate ), msg=str( e ) ) )

    logging.info( 'Bin data backfill done.' )


if __name__ == '__main__':
    # let's kick off the job
    main()
