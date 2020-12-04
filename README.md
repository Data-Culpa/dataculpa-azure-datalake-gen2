# dataculpa-azure-datalake-gen2
Azure Data Lake Gen2 storage connectors for Data Culpa

This connector bridges files storage in an Azure Data Lake to Data Culpa Validator.

## Getting Started

0. Identify the host you want to run the pipeline on; this can be run on the same host as a Data Culpa Validator controller, or it can be run on some other host. The host will need to be able to access the Azure Data Lake and the Data Culpa Validator controller.
1. Clone the repo (or just datalake.py)
2. Install python dependencies (python3):

```
pip install python-dotenv azure-storage-file-datalake
```
3. Install the [Data Culpa python client](https://github.com/Data-Culpa/openclients) and configure your ```PYTHONPATH``` to include the ```dataculpa/``` directory.  (Coming soon: pip package.)
4. Create a .env file with the following keys:

```
# API key to access the storage.
AZURE_API_KEY = stuff_here
AZURE_STORAGE_ACCOUNT = name_here
 
# Set to blank to not cache any metadata locally with the pipeline.
# The question here is whether the data lake is taking in new data (appending)
# or when this pipeline runs, is it always fresh data?
# The default example is to put the metadata into a sqlite cache.
# 
# In a future version of the pipeline, we could store our cached data in the data lake or 
# somewhere else in Azure...I guess all we really need is the last time that we ran per
# object successfully.
AZURE_STORAGE_CACHE = cache.db

# Error log file (path)
AZURE_ERROR_LOG=error.log

# Data Culpa Server
DC_HOST = 192.168.1.13
DC_PORT = 7777
DC_PROTOCOL = 'http'
DC_SECRET = your_secret_here

# Data Culpa Behavior
DC_PIPELINE_NAME = 'azure-dl-test'
DC_PIPELINE_VERSION = '1.0'

# Turn on this parameter to make each top-level directory in the Data Lake go 
# to its own 'stage' step in the pipeline.  We might actually want the directory 
# to map into Data Culpa as root/<pipeline>/<stage> or some other mapping provided
# by the user somehow... not sure how people organize this stuff or how much 'fan out'
# of this pipeline importer people will wind up with.
#DC_DIR_IS_STAGE = 
```

Use multiple environment files to support multiple data lakes and pass in the environment file:

```
datalake.py -e <env file>
```

## Operation

datalake.py can be kicked off from cron or any similar orchestration utility of your choosing.

## Known Limitations

1. API key-only authentication (i.e., needs some work for Azure AD).
2. CSV files only (Data Culpa supports more formats; this is just the library).
3. Some assumptions about how to organize the data in Data Culpa that may not apply to all users.
