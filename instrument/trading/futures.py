'''This script reads trading data for futures from Datayes.

Current supported data type includes
* daily data;
* bin data (1-min bin);
* tick data (to be added);
* ...

All these are on main contract.
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
import pandas as pd

# customized modules
from   data.driver import datayes
import data.api.futures as futuresApi

# initialize logger
logging.basicConfig( format='[%(levelname)s] %(message)s', level=logging.INFO )

def getDailyData( startDate=dt.date( 2012, 1, 1 ),
                  endDate=dt.date.today() ):
    '''Get continuous daily trading data for the given product
in the given date range.

Parameters
----------
product : str
    product to check.

Returns
-------
dailyData : pandas.DataFrame
    Datayes futures daily data.

Exceptions
----------
    raise Exception when connection errors.
    '''
    # read data from Datayes API in a .csv file
    dataUrl = 'api/market/getMktFutd.csv'

    data = []
    tradingDates = set( futuresApi.getTradingDates() )
    iterDate = startDate
    while iterDate <= endDate:
        if iterDate in tradingDates:
            strD = iterDate.strftime( '%Y%m%d' )
            logging.info( 'Processing {d:s}...'.format( d=strD ) )
            params = { 'tradeDate' : strD }
            futuresDailyData = datayes.getDataFrame( dataUrl, params )
            if len( futuresDailyData ) > 0:
                data.append( futuresDailyData )
            else:
                logging.warning( 'Empty data on {d:s}.'.format( d=strD ) )
        iterDate = iterDate + dt.timedelta( 1 )

    dailyData = pd.concat( data )
    dailyData.reset_index( drop=True, inplace=True )

    return dailyData

def getBinData( instId, dataDate=dt.date.today() ):
    '''Get historical bin data for the given instrument during the given
date.

Parameters
----------
instId : str
    instrument ID (instead of SecID with no exchange info);
dataDate : datetime.date
    Data date to read.

Returns
-------
histBinData : pandas.DataFrame
    Datayes futures historical bin data.

Exceptions
----------
    raise Exception when connection errors.
    '''
    # read data from Datayes API in a .csv file
    dataUrl = 'api/market/getFutureBarHistDateRange.csv'

    # compose request payload
    strDataDate = dataDate.strftime( '%Y%m%d' )
    params = { 'instrumentID': instId, 'startDate': strDataDate, 'endDate': strDataDate }

    binData = datayes.getDataFrame( dataUrl, params )

    return binData
