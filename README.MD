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

## TODO
**Future things to consider**
* Clean-up the data processing (do we need RDD / Pandas / PySpark (to do what needs to be done)
* Look into the statistics cubes (max counts)
* Look into alternate storage into a DB (as well as CSV)
* Look into separating the pipe (into discrete stages) - Extract (from Slack) / Transform (into Output Forms) / Load (into CSV, DB, other)

## Setup (from what I can tell - need to validate on clean environment):
**Python**
* version 3.7.7 (minimum for SlackAPI and do not use python 3.8.0)
**NodeJS**
* version (latest LTS)

**Python Packages (installed using pip3)**
* slackclient
* certifi
* urllib3
* panda
* numpy 
* pyspark

## Slack OAuth Permissions__
**Bot Token Scopes**
* chat:write
**User Token Scopes**
* channel:history
* channel:read
* files:read
* groups:history
* groups:read
* im:history (optional)
* im:read (optional)
* mpim:history (optional)
* mpim:read (optional)
* reactions:read
* users:read

## Configuration Requirements (maintain securely)
These configuration requirements are used as part of the scripted environment
* OAuth Access Token (SLACK_ACCESS_TOKEN)
* Bot User OAuth Access Token (SLACK_USER_TOKEN)
* Client Secret (SLACK_CLIENT_TOKEN)
* Signing Secret (SLACK_SIGN_TOKEN)
