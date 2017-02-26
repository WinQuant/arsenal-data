'''This script reads data for stocks from Wind.

Current supported data type includes
* P/E_ttm
* P/E_lyr
* P/B_mrq
* P/S_ttm
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
import pandas as pd
import WindPy as w

# customized modules

def getStockPePb( windCodes, asOfDate ):
    '''Get P/E and P/B from Wind.

Parameters
----------
windCodes : list of str
    name of the stock Wind codes;
asOfDate : datetime.date
    trade date.
    
Returns
-------
pePb : pandas.DataFrame
    P/E and P/B for the requested stocks in a pandas DataFrame. WINDCODE, SEC_NAME,
    PE_TTM, PE_LYR, PB_MRQ, and PS_TTM are included.
    
Exceptions
----------
    raise Exception when Wind errors.
    '''
    response = w.w.wss( ','.join( windCodes ), 'windcode, sec_name, pe_ttm, pe_lyr, pb_mrq, ps_ttm',
            'TRADE_DATE={d:s}'.format( d=asOfDate.strftime( '%Y%m%d' ) ) )

    if response.ErrorCode != 0:
        raise Exception( 'Error when reading from Wind with code {c:s}.'.format( c=response.ErrorCode ) )
    else:
        fields = [ 'WindCode', 'SecName', 'PE_ttm', 'PE_lyr', 'PB_mrq', 'PS_ttm' ]
        data   = response.Data
        transposedData = zip( data )

        return pd.DataFrame( dict( zip( fields, data ) ) )
