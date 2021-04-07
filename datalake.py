#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

import argparse
import dotenv
import logging
import os
import uuid
import sqlite3
import sys
#import tempfile


from dateutil.parser import parse as DateUtilParse

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

from dataculpa import DataCulpaValidator

#for k,v in  logging.Logger.manager.loggerDict.items():
#    if k.find(".") > 0:
#        continue      
#    print(k)  

for logger_name in ['urllib3', 'azure']:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.WARN)


# Need to remember how to disable this for other packages... e.g., Azure 
# stuff leaks a lot of crud.
#logging.basicConfig(format='%(asctime)s %(message)s', level=logging.WARN)

service_client = None


def ConnectByAccountKey(storage_account_name, storage_account_key):
    try:  
        global service_client
        
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)

    return

class Config:
    def __init__(self):
        self.file_ext         = os.environ.get('AZURE_FILE_EXT')
        self.storage_cache_db = os.environ.get('AZURE_STORAGE_CACHE')
#        self.error_log        = os.environ.get('AZURE_ERROR_LOG', "error.log")

        # Data Culpa parameters
        self.pipeline_name      = os.environ.get('DC_PIPELINE_NAME')
        self.pipeline_env       = os.environ.get('DC_PIPELINE_ENV', 'default')
        self.pipeline_stage     = os.environ.get('DC_PIPELINE_STAGE', 'default')
        self.pipeline_version   = os.environ.get('DC_PIPELINE_VERSION', 'default')
        self.dc_host            = os.environ.get('DC_HOST')
        self.dc_port            = os.environ.get('DC_PORT')
        self.dc_protocol        = os.environ.get('DC_PROTOCOL')
        self.dc_secret          = os.environ.get('DC_SECRET')


# FIXME: support this

# Turn on this parameter to make each top-level directory in the Data Lake go 
# to its own 'stage' step in the pipeline.  We might actually want the directory 
# to map into Data Culpa as root/<pipeline>/<stage> or some other mapping provided
# by the user somehow... not sure how people organize this stuff or how much 'fan out'
# of this pipeline importer people will wind up with.
#DC_DIR_IS_STAGE = 

#        self.directory_is_stage = os.environ.get('DC_DIR_IS_STAGE', False)
#
#        if self.directory_is_stage:
#            assert self.pipeline_stage is None, "cannot set both DC_PIPELINE_STAGE and DC_DIR_IS_STAGE"

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


def NewDataCulpaHandle(pipeline_stage=None):
    if pipeline_stage is None:
        pipeline_stage = gConfig.pipeline_stage

    dc = DataCulpaValidator(gConfig.pipeline_name,
                            pipeline_environment=gConfig.pipeline_env,
                            pipeline_stage=pipeline_stage,
                            pipeline_version=gConfig.pipeline_version,
                            protocol=gConfig.dc_protocol, 
                            dc_host=gConfig.dc_host, 
                            dc_port=gConfig.dc_port)
    return dc

def ProcessDateFile(fs_client, file_path):
    if gConfig.file_ext is not None:
        if not file_path.endswith(gConfig.file_ext):
            print(">> %s does not match configured file_ext %s; skipping" % (file_path, gConfig.file_ext))
            return
    print(">> %s is new and needs processing, here we go!" % file_path)

    # Assume a CSV blob, which we will push up.

    # FIXME: tease out the top-level directory for pipeline_stage.

    # FIXME: add short-circuit approach; if we're on the same 
    # machine as the DC controller, no need to move the data twice; we just
    # need to fetch it... but that's for another day.

    # seems we need the directory handle... hopefully Azure is a forward slash environment
    dir_path = os.path.dirname(file_path)
    basename = os.path.basename(file_path)
    d_client = fs_client.get_directory_client(dir_path)
    f_client = d_client.get_file_client(basename)
    download = f_client.download_file()
    downloaded_bytes = download.readall()
    
    tmp_name = "tmp-" + basename
    fp = open(tmp_name, 'wb')
    fp.write(downloaded_bytes)
    fp.close()

    try:
        if tmp_name.endswith(".csv"):
            dc = NewDataCulpaHandle()
            worked = dc.load_csv_file(tmp_name)
            print("worked:", worked)
            dc.queue_commit()
        elif tmp_name.endswith(".json"):
            # load it and send it?
            sys.stderr.write("We don't handle json files right now.\n");
            os._exit(2)
            pass
    except Exception as e:
        # try to remove the file
        os.unlink(tmp_name)
        # pass the exception up
        raise e

    # remove the file
    os.unlink(tmp_name)

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

gConfig = None

def main():

    ap = argparse.ArgumentParser()
    ap.add_argument("-e", "--env",
                    help="Use provided env file instead of default .env")

    args = ap.parse_args()

    env_path = ".env"
    if args.env:
        env_path = args.env
    if not os.path.exists(env_path):
        sys.stderr.write("Error: missing env file at %s\n" % env_path)
        os._exit(1)
        return
    # endif

    dotenv.load_dotenv()

    api_key = os.environ.get('AZURE_API_KEY')
    storage_account = os.environ.get('AZURE_STORAGE_ACCOUNT')

    fs_name = os.environ.get('AZURE_FILESYSTEM_NAME', None)
        
    assert api_key is not None
    assert storage_account is not None

    if fs_name is None:
        sys.stderr.write("Error: missing AZURE_FILESYSTEM_NAME")
        os._exit(2)

    global gConfig
    gConfig = Config()
    LoadCache()

    ConnectByAccountKey(storage_account, api_key)

    file_system_client = service_client.get_file_system_client(file_system=fs_name)
    
    root_path = os.environ.get('AZURE_ROOT_PATH', "")

    WalkPaths(file_system_client, root_path)
    return


if __name__ == "__main__":
    main()

