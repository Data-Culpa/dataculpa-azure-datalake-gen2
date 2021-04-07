#
# create-tree.py
# Data Culpa Python Client
#
# Copyright (c) 2020 Data Culpa, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to 
# deal in the Software without restriction, including without limitation the 
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
# DEALINGS IN THE SOFTWARE.
#

import os
import uuid
import sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

service_client = None


def ConnectByAccountKey(storage_account_name, storage_account_key):
    try:  
        global service_client
        
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)

    return


def RunTest(path):
    # connect.
    api_key = os.environ.get('AZURE_API_KEY')
    storage_account = os.environ.get('AZURE_STORAGE_ACCOUNT')

    assert api_key is not None
    assert storage_account is not None
    ConnectByAccountKey(storage_account, api_key)

    fs_client = service_client.get_file_system_client(file_system="dctest1")

    if path is None:
        path = ""
    paths = fs_client.get_paths(path=path)

    for path in paths:
        print(path.name)

    print("--done--")
    return


if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv()

    path = None
    if len(sys.argv) > 1:
        path = sys.argv[1]
    RunTest(path)
