'''This script reads trading data for stocks from Datayes.

Current supported data type includes
* daily data (former complex right);
* bin data (1-min bin);
* tick data (to be added);
* ...
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

# customized modules
from data.driver import datayes

def getAdjustedDailyData( secId, startDate=dt.date( 2012, 1, 1 ),
                          endDate=dt.date.today() ):
    '''Get adjusted (former complex right) daily information for
the given stock during the given date range.

Parameters
----------
secId : str
    ticker name in secID;
startDate : datetime.date
    data begin date inclusively;
endDate : datetime.date
    data end date inclusively.

Returns
-------
adjustedDailyData : pandas.DataFrame
    Datayes adjusted daily trading volume in pandas DataFrame.

Exceptions
----------
    raise Exception when connection errors.
    '''
    # read data from Datayes API in a .csv file
    dataUrl = 'api/market/getMktEqudAdj.csv'

    # compose request payload
    params  = { 'secID': secId, 'beginDate': startDate.strftime( '%Y%m%d' ),
                'endDate': endDate.strftime( '%Y%m%d' ) }

    adjustedDailyData = datayes.getDataFrame( dataUrl, params )

    return adjustedDailyData


def getHistoryBinData( secId, startDate=dt.date( 2012, 1, 1 ),
                endDate=dt.date.today() ):
    '''Get historical bin data for the given stock during the given
date range.

Parameters
----------
secId : str
    ticker name in secID;
startDate : datetime.date
    data begin date inclusively;
endDate : datetime.date
    data end date inclusively.

Notes
-----
If too many dates requested, the API may raise error.
Limit the date range to one month a time and the `startDate` should be in the
same month as `endDate`.

Returns
-------
histBinData: pandas.DataFrame
    Datayes stock historical bin data in pandas DataFrame.

Exceptions
----------
    raise Exception when connection errors.
    '''
    # read data from Datayes API in a .csv file
    dataUrl = 'api/market/getBarHistDateRange.csv'

    # compose request payload
    params  = { 'securityID': secId, 'startDate': startDate.strftime( '%Y%m%d' ),
                'endDate': endDate.strftime( '%Y%m%d' ) }

    histBinData = datayes.getDataFrame( dataUrl, params )
    return histBinData


def getBinData( secId, dataDate=dt.date.today() ):
    '''Get historical bin data for the given stocks during the given
date.

Parameters
----------
secIds : str
    the ticker name in secID's;
dataDate : datetime.date
    Data date to read.

Notes
-----
The difference between `getBinData` and `getHistoryBinData` is that
* `getBinData` queries in the tickers dimension;
* `getHistoryBinData` queries in the date dimension.

Returns
-------
histBinData: pandas.DataFrame
    Datayes stock historical bin data in pandas DataFrame.

Exceptions
----------
    raise Exception when connection errors.
    '''
    # read data from Datayes API in a .csv file
    dataUrl = 'api/market/getBarHistDateRange.csv'

    # compose request payload
    params  = { 'securityID': secId, 'startDate': dataDate.strftime( '%Y%m%d' ),
                'endDate': dataDate.strftime( '%Y%m%d' ) }

    binData = datayes.getDataFrame( dataUrl, params )

    return binData
