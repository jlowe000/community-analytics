const { WebClient } = require('@slack/web-api');
const { createEventAdapter } = require('@slack/events-api');

// Read a token from the environment variables
const sign_token = process.env.SLACK_SIGN_TOKEN;
const access_token = process.env.SLACK_ACCESS_TOKEN;
const user_token = process.env.SLACK_USER_TOKEN;

const web = new WebClient(access_token);

console.log(sign_token);
console.log(access_token);

(async() => {
  try {
    // const result = await web.chat.postMessage({ text: 'Hello world!', channel: 'C1TLW9J01' });
    loop = false;
    req = { token: user_token };
    do { 
      result = await web.conversations.list({ token: user_token });
      console.log('Posted');
      console.dir(result, {depth:null});
      console.log(result.response_metadata.next_cursor);
      req = { token: user_token, cursor: result.response_metadata.next_cursor };
      loop = result.has_more;
    } while (loop);

    loop = false;
    req = { token: user_token };
    do { 
      result = await web.users.list(req);
      console.log('Posted');
      console.dir(result, {depth:null});
      console.log(result.response_metadata.next_cursor);
      req = { token: user_token, cursor: result.response_metadata.next_cursor };
      loop = result.has_more;
    } while(loop);

    // loop = false;
    // req = { token: user_token, channel: 'C1TLW9J01' };
    // do { 
    //   result = await web.conversations.history(req);
    //   console.log('Posted');
    //   console.dir(result, {depth:null});
    //   console.log(result.response_metadata.next_cursor);
    //   req = { token: user_token, channel: 'C1TLW9J01', cursor: result.response_metadata.next_cursor };
    //   loop = result.has_more;
    // } while(loop);
  } catch (error) {
    console.log('Error');
    console.log(error.data);
  }
})();

