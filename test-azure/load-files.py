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

import argparse
import dotenv
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


def RunTest(target_dir, local_dir):
    # connect.
    api_key = os.environ.get('AZURE_API_KEY')
    storage_account = os.environ.get('AZURE_STORAGE_ACCOUNT')

    assert api_key is not None
    assert storage_account is not None
    ConnectByAccountKey(storage_account, api_key)

    fs_client = service_client.get_file_system_client(file_system="dctest1")

    # create an example tree.
    fs_client.create_directory(target_dir)
    dir_client = fs_client.get_directory_client(target_dir)

    files = os.listdir(local_dir)
    for fname in files:
        if not fname.endswith(".csv"):
            print("@@ skipping %s" % fname)
            continue

        fp = local_dir + "/" + fname

        # and create a test file inside each.
        #dir_client = fs_client.get_directory_client(sub_name)
        file_client = dir_client.create_file(fname)

        with open(fp, "r") as fh:
            b = fh.read()
            file_client.append_data(data=b, offset=0, length=len(b))
            file_client.flush_data(len(b))
        print("done with %s" % fp)

    # endfor

    print("--done--")
    return


if __name__ == "__main__":
    dotenv.load_dotenv()

    ap = argparse.ArgumentParser()
    ap.add_argument("-t", "--targetdir",        help="target directory name to create")
    ap.add_argument("--load",    help="load .csv files from this local directory")
    args = ap.parse_args()
    
    if args.targetdir is None:
        print("missing -t targetdir");
        os._exit(2)
    if args.load is None:
        print("missing --load");
        os._exit(3)

    RunTest(args.targetdir, args.load)
