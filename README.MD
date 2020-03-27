# Slack Analytics

* This was built for the ausinnovation slack community to measure the interactions of the community.
* This was built with the consent of Chad Renando to help measure.

History:
v0.1
* Built a NodeJS version of th SlackAPIs v2 to create CSV content of channels, users, messages, threads, reactions 
v0.2
* Built a Python version of th SlackAPIs v2 to create CSV content of channels, users, messages, threads, reactions, files
* This version supercedes the NodeJS version
* Captures the CSV and raw JSON data
* Captures the file hosted uploads

Setup (from what I can tell - need to validate on clean environment):

Python
* version 3.6 (minimum for SlackAPI)

Python Packages (installed using pip)
* SlackAPI - version 2
* certifi
* urllib3
* panda
* spark
* numpy 

Slack OAuth Permissions
__Bot Token Scopes__
* chat:write
__User Token Scopes__
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

Configuration Requirements (maintain securely)
* OAuth Access Token (SLACK_ACCESS_TOKEN)
* Bot User OAuth Access Token (SLACK_USER_TOKEN)
* Client Secret (SLACK_CLIENT_TOKEN)
* Signing Secret (SLACK_SIGN_TOKEN)
