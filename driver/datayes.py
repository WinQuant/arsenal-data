'''This script acts as a unified interface to Datayes.
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
import logging
import io

# third-party modules
import pandas as pd
import requests

# customized modules
from data.config import *

# set logging level
logging.getLogger( 'requests' ).setLevel( logging.WARNING )

def getDataFrame( apiUrl, params ):
    '''Get data from Datayes in pandas DataFrame.

Parameters
----------
apiUrl : str
    API to Datayes;
params : dict
    parameters to feed the API.

Returns
-------
data: pandas.DataFrame or None
    Datayes data in pandas DataFrame or
    None if error happends.

Exceptions
----------
    raise Exception when connection errors.
    '''
    data = None

    # compose request header and payload
    headers = { 'Authorization': 'Bearer ' + DATAYES_TOKEN }

    # get response
    response = requests.get( url='/'.join( [ DATAYES_API_URL,
                                             DATAYES_VERSION,
                                             apiUrl ] ), headers=headers,
                                           params=params )
    if response.status_code != DATAYES_STATUS_OK:
        raise Exception( 'Request failed with status code {sc:d}.'.format(
                         sc=response.status_code ) )
    else:
        bufferData = io.StringIO( response.text )
        data       = pd.read_csv( bufferData )

    return data
