const { WebClient } = require('@slack/web-api');
const { createEventAdapter } = require('@slack/events-api');
const fs = require('fs');
const dateFormat = require('dateformat');

// Read a token from the environment variables
const sign_token = process.env.SLACK_SIGN_TOKEN;
const access_token = process.env.SLACK_ACCESS_TOKEN;
const user_token = process.env.SLACK_USER_TOKEN;
const web = new WebClient(access_token);

function dd_reactiondata(ts, reactions) {
  const filename = "reaction_data.csv";

  fs.access(filename, fs.constants.F_OK, (err) => {
    if (err) { try { fs.writeFileSync(filename, 'ts,reaction,user,count\n', 'utf8'); } catch (err) {} }
  });
  // console.dir(reactions, {depth:null});
  reactions.forEach(function(value){
    value.users.forEach(function(user){
      fs.appendFile(filename, ts+','+value.name+','+user+','+value.count+'\n', 'utf8', (err) => { if (err) { console.log(err); } });
    });
  });
}

function retrieveThreads(channel, thread_ts) {
  (async() => {
    try {
      loop = false;
      req = { token: user_token, channel: channel, ts: thread_ts };
      do { 
        result = await web.conversations.replies(req);
        console.log('Posted thread '+thread_ts);
        dd_threaddata(channel,thread_ts,result);
        req = { token: user_token, channel: channel, ts: thread_ts, cursor: result.response_metadata.next_cursor };
        loop = result.has_more;
      } while (loop);
    } catch (error) {
      console.log('Error');
      console.log(error.data);
    }
  })();
}

function retrieveMessages(channel) {
  (async() => {
    try {
      loop = false;
      req = { token: user_token, channel: channel };
      do { 
        result = await web.conversations.history(req);
        console.log('Posted channel '+channel);
        dd_messagedata(channel,result);
        if (result.reactions) {
          dd_reactiondata(ts,result.reactions);
        }
        req = { token: user_token, channel: channel, cursor: result.response_metadata.next_cursor };
        loop = result.has_more;
      } while (loop);
    } catch (error) {
      console.log('Error');
      console.log(error.data);
    }
  })();
}

function convertts(ts) {
  cts = undefined;
  try {
    cts = dateFormat(new Date(Math.floor(parseFloat(ts,10)*1000)),'yyyy-mm-dd"T"HH:MM:ss.000');
  } catch (err) {
  }
  // console.log('convert:'+ts+','+cts);
  return cts;
}

function dd_threaddata(channel,thread_ts,result) {
  const filename = "threads_data.csv";

  fs.access(filename, fs.constants.F_OK, (err) => {
    if (err) { try { fs.writeFileSync(filename, 'channel,type,subtype,ts,thread_ts,time,user,text\n', 'utf8'); } catch (err) {} }
  });
  // console.dir(result, {depth:null});
  // console.log("cursor = "+result.response_metadata.next_cursor);
  result.messages.forEach(function(value){
    fs.appendFile(filename, channel+','+value.type+','+value.subtype+','+value.ts+','+value.thread_ts+','+convertts(value.ts)+','+value.user+',"'+value.text.replace(/"/g,'""')+'"\n', 'utf8', (err) => { if (err) { console.log(err); } });
    if (value.reactions) {
      dd_reactiondata(value.ts,value.reactions);
    }
  });
}

function dd_userdata(result) {
  const filename = "user_data.csv";

  fs.access(filename, fs.constants.F_OK, (err) => {
    if (err) { try { fs.writeFileSync(filename, 'id,name,tz\n', 'utf8'); } catch (err) {} }
  });
  // console.dir(result, {depth:null});
  // console.log("cursor = "+result.response_metadata.next_cursor);
  result.members.forEach(function(value){
    fs.appendFile(filename, value.id+','+value.name+','+value.tz+'\n', 'utf8', (err) => { if (err) { console.log(err); } } );
  });
}

function dd_channeldata(result) {
  const filename = "channel_data.csv";

  fs.access(filename, fs.constants.F_OK, (err) => {
    if (err) { try { fs.writeFileSync(filename, 'id,name\n', 'utf8'); } catch (err) {} }
  });
  // console.dir(result, {depth:null});
  // console.log("cursor = "+result.response_metadata.next_cursor);
  result.channels.forEach(function(value){
    retrieveMessages(value.id);
    fs.appendFile(filename, value.id+','+value.name+'\n', 'utf8', (err) => { if (err) { console.log(err); } } ); 
  });
}

function dd_messagedata(channel,result) {
  const filename = "message_data.csv";

  fs.access(filename, fs.constants.F_OK, (err) => {
    if (err) { try { fs.writeFileSync(filename, 'channel,type,subtype,ts,thread_ts,time,user,text\n', 'utf8'); } catch (err) {} } 
  });
  // console.dir(result, {depth:null});
  // console.log("cursor = "+result.response_metadata.next_cursor);
  result.messages.forEach(function(value){
    fs.appendFile(filename, channel+','+value.type+','+value.subtype+','+value.ts+','+value.thread_ts+','+convertts(value.ts)+','+value.user+',"'+value.text.replace(/"/g,'""')+'"\n', 'utf8', (err) => { if (err) { console.log(err); } });
    if (value.reactions) {
      dd_reactiondata(value.ts,value.reactions);
    }
    if (value.thread_ts) {
      retrieveThreads(channel,value.thread_ts);
    }
  });
}

function dd_metadata() {
  const filename = "meta_data.csv";

  fs.access(filename, fs.constants.F_OK, (err) => {
    if (err) { try { fs.writeFileSync(filename, 'time\n', 'utf8'); } catch (err) {} } 
  });
  fs.appendFile(filename, dateFormat(new Date(),'yyyy-mm-dd"T"HH:MM:ss.000')+'"\n', 'utf8', (err) => { if (err) { console.log(err); } });
}

(async() => {
  try {
    loop = false;
    req = { token: user_token };
    do { 
      result = await web.users.list(req);
      console.log('Posted users');
      dd_userdata(result);
      req = { token: user_token, cursor: result.response_metadata.next_cursor };
      loop = result.has_more;
    } while(loop);

    loop = false;
    req = { token: user_token };
    do { 
      result = await web.conversations.list(req);
      console.log('Posted channels');
      dd_channeldata(result);
      req = { token: user_token, cursor: result.response_metadata.next_cursor };
      loop = result.has_more;
    } while (loop);
    dd_metadata();
  } catch (error) {
    console.log('Error');
    console.log(error.data);
  }
})();

