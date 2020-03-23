const { WebClient } = require('@slack/web-api');
const { createEventAdapter } = require('@slack/events-api');

// Read a token from the environment variables
const sign_token = process.env.SLACK_SIGN_TOKEN;
const access_token = process.env.SLACK_ACCESS_TOKEN;

console.log(sign_token);
console.log(access_token);

const events = createEventAdapter(sign_token, { includeBody: true, includeHeaders: true });

const port = process.env.PORT || 8888;

events.on('reaction_removed', (event) => {
  console.log(`Received a message @ reaction_removed`);
  console.dir(event, {depth:null});
});

events.on('reaction_added', (event) => {
  console.log(`Received a message @ reaction_added`);
  console.dir(event, {depth:null});
});

events.on('message', (event) => {
  console.log(`Received a message @ message`);
  console.dir(event, {depth:null});
});

(async() => {
  const server = await events.start(port);
  console.log(`Listening on ${server.address().port}`);
})();

