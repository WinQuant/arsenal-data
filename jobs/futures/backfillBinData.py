'''This job backfills the minute-by-minute bin data for the whole futures universe.
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

# customize logging configure
logging.basicConfig( format='[%(levelname)s] %(message)s', level=logging.INFO )

def main():
    '''Main body of the job.
    '''
    asOfDate = dt.date( 2016, 4, 25 )
    endDate  = dt.datetime.combine( dt.date( 2016, 4, 22 ),
            dt.datetime.min.time() )

    # get all available futures since the very beginning.
    futuresInfo = futuresApi.getFuturesInformation( asOfDate, listed=False )

    # initialize MongoDB connection
    username, password = config.MONGODB_CRED
    db = mongodb.getAuthenticatedConnection( config.MONGODB_URL,
        config.MONGODB_PORT, username, password, 'binData' )


    # contract metadata
    contractMeta = list( zip( futuresInfo.ticker, futuresInfo.secID,
            futuresInfo.listDate, futuresInfo.lastTradeDate ) )
    # contractMeta = contractMeta[ 23 : ]
    nContracts   = len( contractMeta )

    for i, meta in enumerate( contractMeta ):
        instId, secId, listedDate, tradeDate = meta
        logging.info( 'Processing futures {f:s} ({i:d}/{n:d})...'.format(
                f=secId, i=i + 1, n=nContracts ) )
        if listedDate <= '2012-01-01':
            logging.warning( 'Too senior contract for {sec:s}.'.format(
                sec=secId ) )
            continue

        # normalize instrument ID
        instId = instId.upper()
        startDate = dt.datetime.strptime( listedDate, '%Y-%m-%d' )
        endDate   = min( endDate, dt.datetime.strptime( tradeDate, '%Y-%m-%d' ) )

        curDate = startDate
        records = []
        while curDate <= endDate:
            logging.info( 'Processing {iid:s} ({i:d}/{n:d}) on {sd:s}...'.format(
                    iid=instId, sd=str( curDate ), i=i, n=nContracts ) )
            try:
                dailyBin = futuresTrading.getBinData( instId, dataDate=curDate )
                if len( dailyBin ) > 0:
                    record = { 'SecID': secId,
                               'Date':  curDate,
                               'Data':  dailyBin.to_json(),
                               'Country': 'CN' }
                    records.append( record )
                else:
                    logging.warning( 'Empty data for {sec:s} on {sd:s}.'.format(
                        sec=secId, sd=str( curDate ) ) )
            except KeyError as e:
                logging.warning( 'Error when updating {sec:s} on {sd:s}.'.format(
                    sec=secId, sd=str( curDate ) ) )

            curDate += dt.timedelta( 1 )

        if len( records ) > 0:
            db.futures.insert_many( records )
        else:
            logging.warning( 'Empty data to be inserted into database for {sec:s}'.format(
                    sec=secId ) )
    
    logging.info( 'Bin data backfill done.' )


if __name__ == '__main__':
    # let's kick off the job
    main()
