'''This job updates the minute-by-minute trading data for the whole available
futures universe.
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
import data.api.futures as futuresApi
import data.config      as config
import data.driver.mongodb as mongodb
import data.instrument.trading.futures as futuresTrading

# customize logging config
logging.basicConfig( format='[%(levelname)s] %(message)s', level=logging.INFO )

def main():
    '''Entry point of the job.
    '''
    # runtime
    asOfDate = dt.date.today()
    logging.info( 'Updating minute bin data for futures on date {d:s}...'.format(
            d=str( asOfDate ) ) )

    # get all futures in the universe
    futuresInfo = futuresApi.getFuturesInformation( asOfDate )
    universe = dict( zip( futuresInfo.ticker,
                          futuresInfo.secID ) )

    # initialize MongoDB connection
    username, password = config.MONGODB_CRED
    db = mongodb.getAuthenticatedConnection( config.MONGODB_URL,
            config.MONGODB_PORT, username, password, 'binData' )

    nFutures = len( universe )
    logging.info( 'Minute bin volume for {ns:d} futures in total to be updated...' )

    # for bin data, futures are updated one-by-one
    for i, ids in enumerate( universe.items() ):
        futures, secId = ids
        futures = futures.upper()
        logging.info( 'Updating minute bin data for {s:s} ({idx:d}/{n:d})...'.format(
                s=secId, idx=i + 1, n=nFutures ) )

        data = futuresTrading.getBinData( futures, dataDate=asOfDate )

        if len( data ) > 0:
            mongoDate = dt.datetime.combine( asOfDate, dt.datetime.min.time() )
            record    = { 'SecID': secId,
                          'Date':  mongoDate,
                          'Data':  data.to_json(),
                          'Country': 'CN' }
     
            db.futures.update( { 'SecID': secId, 'Date': mongoDate, 'Country': 'CN' },
                    record, upsert=True )
        else:
            logging.warning( 'Empty data for {secId:s}'.format( secId=secId ) )

    logging.info( 'All futures updated.' )


if __name__ == '__main__':
    # let's kick off the job
    main()
