'''This script acts as a unified interface to Wind.
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

# cusotmized modules
import WindPy as w

def processWindReturn( response, colNames=None ):
    '''Processing the Wind return from the API call.

Parameters
----------
response : WindPy.w.WindData
    Response data from Wind API;
colNames : list of str or None
    column names to be used in the pandas.DataFrame returned, if
    None given, use the default fields from Wind.

Returns
-------
data : pandas.DataFrame
    Processed pandas.DataFrame

Exceptions
----------
    raise Exception when error returned.
    '''
    if response.ErrorCode != 0:
        raise Exception( 'Error when reading from Wind with code {c:d}.'.format( c=response.ErrorCode ) )
    else:
        fields = response.Fields if colNames is None else colNames
        data   = zip( response.Data )

    return pd.DataFrame( dict( zip( fields, data ) ) )
