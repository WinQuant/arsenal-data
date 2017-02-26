'''This script reads futures contract information from Datayes and saved to local MongoDB for later use.
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
import pandas as pd

# customized modules
from ..driver import datayes


FUTURES_EXCHANGES = [ 'XZCE',    # Zhengzhou Commodity Exchange
                      'CCFX',    # China Financial Futures Exchange
                      'XSGE',    # China Shanghai Futures Exchange
                      'XDCE' ]   # Dalian Commodity Exchange

def getFuturesContracts( exch ):
    '''Get futures contract informaiton from Datayes.

Parameters
----------
exch : str
    Exchange name. Should be one of XZCE, CCFX, XSGE, and XDCE.

Returns
-------
futures : pandas.DataFrame
    Datayes futures contracts in the given exchanges.

Exceptions
----------
    raise Exception when connection errors.
    '''
    # read data from the Datayes API in a .csv file
    futUrl  = 'api/future/getFutu.csv'

    # compose request payload
    params  = { 'exchangeCD': exch }

    futures = datayes.getDataFrame( futUrl, params )

    return futures
