# -*- coding: UTF-8 -*-
'''This script reads industry sector data from Datayes and saved to local MongoDB session for later use.
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

def getIndustryClassification():
    '''Get industy classification from Datayes.

Returns
-------
industryClassification : pandas.DataFrame or None
    Datayes industry classification in pandas DataFrame or
    None if error happends.

Exceptions
----------
    raise Exception when connection errors.
    '''
    # read data from Datayes API in a .csv file
    indUrl = 'api/equity/getEquIndustry.csv'

    # compose request payload
    params  = { 'industry': u'申万行业分类' }

    industryClassification = datayes.getDataFrame( indUrl, params )

    return industryClassification


def getStocks():
    '''Get stock fundamental information from Datayes.

Returns
-------
stockInfo : pandasDataFrame or None
    Datayes stock information in pandas DataFrame or
    None if error happens.

Exceptions
----------
    raise Exception when connection errors.
    '''
    # read data from Datayes API in a .csv file
    stockUrl = 'api/equity/getEqu.csv'

    # compose request payload
    params = { 'equTypeCD': 'A' }

    stockInfo = datayes.getDataFrame( stockUrl, params )

    return stockInfo
