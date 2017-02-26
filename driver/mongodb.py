'''This script encapsulates all the utility functions related to
MongoDB operation.
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
import pymongo

# customized modules

def getAuthenticatedConnection( mongoUrl, port, username, password, dbname, source='admin' ):
    '''Get MongoDB connection and authenticate the connection.

Parameters
----------
mongoUrl : str
    MongoDB hostname;
port : int
    MongoDB listening port;
username : str
    MongoDB username to login;
password : str
    MongoDB password to authenticate the user;
dbname : str
    MongoDB database name to connect;
source : str
    Source of the authentication database.

Returns
-------
db : pymongon.database.Database
    authenticated MongoDB connection.

Exceptions
----------
    raise Exception when authentication failed or cannont connect to server.
    '''
    client = pymongo.MongoClient( mongoUrl, port )
    db     = client[ dbname ]
    if not db.authenticate( username, password, source=source ):
        raise Exception( 'Fail to authenticate {u:s} against {db:s}.'.format(
                         u=username, db=dbname ) )

    return db
