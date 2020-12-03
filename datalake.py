#
# datalake.py
# Data Culpa Azure Data Lake Gen2 Connector
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

#import dateutil
import os
import uuid
import sqlite3
import sys
from dateutil.parser import parse as DateUtilParse
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


def ConnectByAzureAD():
    print("not implemented")
    return


class Config:
    def __init__(self):
        self.file_ext         = os.environ.get('AZURE_FILE_EXT')
        self.storage_cache_db = os.environ.get('AZURE_STORAGE_CACHE')
        self.error_log        = os.environ.get('AZURE_ERROR_LOG', "error.log")

fcache = {} 
new_cache = {}

def LoadCache():
    global gConfig
    global fcache

    fn = gConfig.storage_cache_db
    assert fn is not None

    # try to load from sqlite.
    
    needs_tables = False
    if not os.path.exists(fn):
        # we will need to create tables.
        needs_tables = True
    # endif

    c = sqlite3.connect(fn)
    if needs_tables:
        # FIXME: Well, my original idea for this was that we'd just keep the Azure modified 
        # FIXME: timestamp as a string, but sqlite helpfully changes the strings to a date object
        # FIXME: So... we could keep a hash or something, I suppose, or we can wait til datetime
        # FIXME: parsing bites us, which you know is going to happen.
        c.execute("create table cache (filename text unique, last_mod_str text)")
    # endif

    r = c.execute("select filename, last_mod_str from cache")
    for row in r:
        (filename, last_mod_str) = row
        last_mod_dt = DateUtilParse(last_mod_str)
        fcache[filename] = last_mod_dt
    # endfor

    return

def FlushNewCache():
    global gConfig

    fn = gConfig.storage_cache_db
    assert fn is not None
    c = sqlite3.connect(fn)

    for file_name, last_mod_str in new_cache.items():
        # Note that this might be dangerous if we add new fields later and we don't set them all...
        c.execute("insert or replace into cache (filename, last_mod_str) values (?,?)", (file_name, last_mod_str))

        # Sadly, the "upsert" functionality was added to SQLite in 3.24 in 2018; 
        # Ubuntu 18 comes with 3.22, which is too old. 
        #        c.execute("""
        #insert 
        #    into cache (filename, last_mod_str) 
        #    values (?, ?) 
        #    on conflict(filename) do 
        #        update set last_mod_str=?""",
        #        (file_name, last_mod_str, last_mod_str))

    c.commit()
    return

def ProcessDateFile(fs_client, file_path):
    print(">> %s is new and needs processing, here we go!" % file_path)
    return

def WalkPaths(fs_client, path):
    # Crazy, no recursion needed--this is very handy.
    paths = fs_client.get_paths(path=path) # iterate over the root.
    for p in paths:
        if p.is_directory:
            continue
#dateutil.parser.
        lm_time = DateUtilParse(p.last_modified)
        #print(p.name, p.last_modified, lm_time) #type(p.last_modified))

        # does it exist in the old cache?
        existing = fcache.get(p.name)
        if existing is not None:
            if lm_time == existing:
                #print("skipping for %s" % p.name)
                continue
            #else:
                #print("__%s__ != __%s__" % (p.last_modified, existing))

        if existing is None:
            print("new file %s" % p.name)

        if existing != p.last_modified:
            print("%s has changed; reprocessing..." % p.name)

        ProcessDateFile(fs_client, p.name)
        new_cache[p.name] = p.last_modified
    # endfor

    # flush out the new_cache entries.
    FlushNewCache()

    return


def RunTest():
    api_key = os.environ.get('AZURE_API_KEY')
    storage_account = os.environ.get('AZURE_STORAGE_ACCOUNT')

    global gConfig
    gConfig = Config()
    LoadCache()

    assert api_key is not None
    assert storage_account is not None
    ConnectByAccountKey(storage_account, api_key)

    file_system_client = service_client.get_file_system_client(file_system="dctest1")
    #print(file_system_client)
    
    WalkPaths(file_system_client, "")


    return




if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv()

    RunTest()
