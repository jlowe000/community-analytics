import os
import slack

client = slack.WebClient(token=os.environ['SLACK_ACCESS_TOKEN'])

response = client.chat_postMessage(
    channel='#random',
    text="Hello world!")
assert response["ok"]
assert response["message"]["text"] == "Hello world!"
