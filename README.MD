# Slack Analytics

* This was built for the ausinnovation slack community to measure the interactions of the community.
* This was built with the consent of Chad Renando to help measure.

## History (these numbers are arbitrary, there is no sprint planning - yet)

**v0.1**
* Built a NodeJS version of th SlackAPIs v2 to create CSV content of channels, users, messages, threads, reactions 

**v0.2**
* Built a Python version of th SlackAPIs v2 to create CSV content of channels, users, messages, threads, reactions, files
* This version supercedes the NodeJS version (though will maintain the old code if anyone needs it)
* Captures the CSV and raw JSON data
* Captures the file hosted uploads
* Added exception logic for socket TIMEOUT

**v0.3**
* Further testing on clean environment (so we can run backups and as part of a cron job)

**v0.4**
* Testing worked with cron
* Split reference.py (to be removed at some stage), creating extract script and transform / load script
* Extract shell zip has been created to zip up all of the content (json / csv data)
* NOTE: even though it is JSON like, the string output is Python objects using ast to evaluate back into the Python objects

**v0.5**
* Cleaned up pandas / spark / slack imports (reduced the amount of spark execution to timestamp calculations and adding columns)
* Further testing of the automated execution __SUCCESS__
* Added zip execution into the output. (all API and CSV data is captured).

**v0.6**
* Reduced dependency on spark further (not required for extract).
* Increased usage pandas vs spark for processing.
* Added stages to the transform / load (where Stage 1 is Transform from JSON and Load into CSV, Stage 2 is Post-Processing for Output files eg conversation_data, node_data).

**v0.7**
* Executed further refinement and testing of the processing of conversation_data.
* Added the node_data and edge_data (for gephi processing).
* Added the aggregations for channels and users.

**v0.8**
* Removed spark altogether and using pandas to manipulate the datasets.
* Added in json output translation - raw /api has been saved as Python object format (which is not JSON).
* Added channel mapping to unique users into aggregations

**v0.9**
* Added merge into a master dataset (to cover how slack restricts the number of message hence losing data).
* Refined structure around metrics and raw data
* Refined zip structure to separate the raw data (from the APIs, Files and JSON representations) from the metrics (conversations, aggregations, statistics)

**v1.0**
* Added tag references from the text (and extracting users, channels and links)
* Added timediff for user interactions.

**v1.1**
* Created new script to load data into postgres database.
* Existing scripts do not include bootstrapping the database.

## TODO

**Future things to consider**
* ~~Clean-up the data processing (do we need RDD / Pandas / PySpark to do what needs to be done)~~
* ~~Determine how to use Pandas to do column insertion (for id and foreign relationships)~~ ~~(Use merge) still need to determine timestamp format transformations.~~
* ~~Look into the statistics cubes (max counts)~~
* Look into alternate storage into a DB (as well as CSV) - ~~potential representations in postgres,~~ neojs, graphql
* ~~look into separating the pipe (into discrete stages) - Extract (from Slack) / Transform (into Output Forms) / Load (into CSV, DB, other)~~ though Transform & Load are in the same process.
* ~~Remove any linkages to slack and http in the transform / load script.~~

## Setup (from what I can tell - need to validate on clean environment):

**Python**
* version 3.7.7 (minimum for SlackAPI and do not use python 3.8.0)

**NodeJS**
* version (latest LTS)

**Postgres**
* version 12

**Python Packages (installed using pip3)**
* slackclient
* certifi
* urllib3
* panda
* numpy 
* nltk
* used pyspark (for loading into postgres)

## Slack OAuth Permissions

**Bot Token Scopes**
* chat:write

**User Token Scopes**
* channel:history
* channel:read
* files:read
* groups:history (optional)
* groups:read (optional)
* im:history (optional)
* im:read (optional)
* mpim:history (optional)
* mpim:read (optional)
* reactions:read
* users:read
* users:email

## Configuration Requirements (maintain securely)

These configuration requirements are used as part of the scripted environment
* OAuth Access Token (SLACK_ACCESS_TOKEN)
* Bot User OAuth Access Token (SLACK_USER_TOKEN)
* Client Secret (SLACK_CLIENT_TOKEN)
* Signing Secret (SLACK_SIGN_TOKEN)

## Solution Overview

The process has three steps - extract, transform, analyse. The current solution has two files:
* extract_slack.py - calls the Slack APIs to collect the data and saves the data directly to file. The files are raw Python objects (combination of dictionary and list objects.
* transform_load.py - transforms the raw objects into a model that we can use to aggregate. There are currently two forms - CSV which is in the model, JSON which is a replica of the Python objects.

### Information about extract_slack.py

* This script requires the Slack API OAuth tokens configured.
* The implementation assumes that all user token scopes have been approved.
* groups, im and mpim can be excluded by removing them from the client.conversation_list method call and restricting the types of conversations.
* Even though (and something to fix) the output is not JSON - it is Python objects stringified.
* The data output is created in the ./data directory with a batch number based upon the current timestamp.
* API data will be captured in the /data/<batch>/api directory.
* Each API request is captured in separate files.
* Response payloads do not include the search keys hence the payload does not have the foreign keys. The main one to be be concerned about is the relationship from channel to message to reply (in a thread).
* The IDs have been encoded in the file name.
* A note about the batch information was created in the /data/<batch>/metadata/metadata.csv file.

### Information about transform_load.py

* This script has two modes - 1. transform into model, merge into master dataset and aggregate individual dataset (if a batch id is passed as an argument which is the <batch> directory name. and 2. aggregate master dataset. (if no batch id was provided).
* Based upon the file naming convention and ids encoded in the files, the model is created to form JSON objects and CSV datasets.
* The CSV datasets combine the information from the APIs into CSV format.
* The CSV datasets is then used to aggregate into summarised datasets.
* The CSV datasets from the batch is merged into a master dataset in the ./data/master/csv directory.
* To merge - add new records, update existing records.
* The decision not to delete records is such that the dataset covers the lifespan of the workspace. There is a limit of 10k messages in the workspace and hence using this as a backup as well.
* The master dataset can also be aggregated into summarised datasets.
* As part of the summarised datasets, node and edge files are created that can be imported into a gephi.
