import {writeEventBuffer} from './lib/event-buffer.mjs'
// Execute the full original event buffer with actual helpers and a fake clock.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-buffer-'))
try {
 const protoSource=(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href));await writeFile(join(temp,'proto.mjs'),protoSource)
 const {makeEventBuffer}=await writeEventBuffer(upstream,temp),emit=(event,data,conditions)=>({op:'emit',event,data,conditions}),key=(id='M',fromMe=false)=>({id,remoteJid:'200@s.whatsapp.net',fromMe}),msg=(id='M',extra={})=>({key:key(id),message:{conversation:'hello'},messageTimestamp:10,...extra}),hist=(chats=[],messages=[],contacts=[],extra={})=>({chats,messages,contacts,...extra});
 const streams=[
  [emit('chats.update',[{id:'200@s.whatsapp.net',archived:true}],[{kind:'chat',last:10}]),{op:'flush'},{op:'buffer'},emit('messaging-history.set',hist([{id:'200@s.whatsapp.net',lastMessageRecvTimestamp:5}]))],
  [emit('chats.update',[{id:'200@s.whatsapp.net',archived:true}],[{kind:'chat',last:10}]),emit('chats.update',[{id:'200@s.whatsapp.net',pinned:9}]),{op:'flush'},{op:'buffer'},emit('chats.upsert',[{id:'200@s.whatsapp.net',lastMessageRecvTimestamp:50}])],
  [emit('chats.delete',['200@s.whatsapp.net']),emit('chats.update',[{id:'200@s.whatsapp.net',archived:true}],[{kind:'false'}])],
  [emit('messages.upsert',{messages:[msg()],type:'append',requestId:'ignored'}),emit('messages.upsert',{messages:[msg('N')],type:'notify'})],
  [emit('contacts.update',[{id:'200@s.whatsapp.net',name:'lost',notify:'old'}]),emit('contacts.upsert',[{id:'200@s.whatsapp.net',name:'new'}])],
  [emit('groups.update',[{id:'g@g.us',subject:'first'},{id:'g@g.us',subject:'ignored'}])],
  [emit('chats.update',[{id:'200@s.whatsapp.net',unreadCount:-1}]),emit('chats.upsert',[{id:'200@s.whatsapp.net',unreadCount:null}])],
  [emit('chats.upsert',[{id:'200@s.whatsapp.net',unreadCount:2}]),emit('messages.upsert',{messages:[msg()],type:'notify'}),emit('messages.update',[{key:key(),update:{status:4}}])],
  [emit('messaging-history.set',hist([],[],[],{pastParticipants:[{groupJid:'g@g.us',pastParticipants:[{userJid:'x',leaveTs:0,leaveReason:0}]}]})),emit('messaging-history.set',hist([],[],[],{pastParticipants:[{groupJid:'g@g.us',pastParticipants:[{userJid:'x',leaveTs:0,leaveReason:0},{userJid:'y',leaveTs:1}]}]}))],
  [emit('messaging-history.set',hist([{id:'chat'}],[msg()],[{id:'contact'}],{isLatest:true})),{op:'flush'},{op:'buffer'},emit('messaging-history.set',hist([{id:'chat'}],[msg()],[{id:'contact'}],{isLatest:false}))],
  [emit('chats.upsert',[{id:'2'},{id:'1'},{id:'a'}]),emit('chats.update',[{id:'z',unreadCount:2},{id:'y',unreadCount:3},{id:'z',unreadCount:4}])],
  [{op:'flush'},{op:'begin',id:'f'},{op:'end',id:'f',success:true},{op:'tick',now:99},{op:'tick',now:100}],
  [{op:'flush'},{op:'begin',id:'f'},{op:'begin',id:'g'},{op:'end',id:'f',success:false},{op:'end',id:'g',success:true},{op:'tick',now:100}],
  [{op:'flush'},{op:'begin',id:'f'},{op:'end',id:'f',success:true},{op:'destroy'},{op:'buffer'},{op:'tick',now:100}],
  [{op:'tick',now:30000}],
  [emit('messages.upsert',{messages:[msg()],type:'append'}),emit('messages.upsert',{messages:[{key:null}],type:'notify'})],
  [emit('chats.update',[{id:'200@s.whatsapp.net',archived:true}],[{kind:'throw'}])],
  [emit('chats.upsert',[{id:'200@s.whatsapp.net',lastMessageRecvTimestamp:'bad'}]),emit('chats.update',[{id:'200@s.whatsapp.net',archived:true}],[{kind:'chat',last:10}])],
  ...[{ephemeralMessage:{},conversation:'ignored'},{senderKeyDistributionMessage:{}},{ephemeralMessage:{message:{conversation:'real'}}}].map(message=>[emit('chats.upsert',[{id:'200@s.whatsapp.net',unreadCount:2}]),emit('messages.upsert',{messages:[msg('M',{message})],type:'notify'}),emit('messages.update',[{key:key(),update:{status:4}}])]),
 ];
 let seed=7;const random=n=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed%n};
 const reactions=[{key:{remoteJid:'300@s.whatsapp.net',fromMe:false},text:'👍'},{key:{fromMe:true},text:''},{key:{participantAlt:'301:2@lid'},text:'❤️'},{}];
 for(let i=0;i<240;i++){const ops=[];for(let j=0;j<24;j++){const chat=['200@s.whatsapp.net','201@lid','g@g.us'][random(3)],id=['M','N','P'][random(3)],k={...key(id),remoteJid:chat,fromMe:!!random(2)};switch(random(15)){
  case 0:ops.push(emit('messaging-history.set',hist([{id:chat,unreadCount:random(4),lastMessageRecvTimestamp:random(20)}],[{...msg(id),key:k}],[{id:chat,name:random(2)?'Name':undefined}],{syncType:random(7),progress:random(100),isLatest:!!random(2)})));break;
  case 1:ops.push(emit('chats.upsert',[{id:chat,unreadCount:random(5)-1,archived:!!random(2)}]));break;
  case 2:ops.push(emit('chats.update',[{id:chat,unreadCount:random(3)?random(5)-1:null,archived:!!random(2)}],random(2)?[{kind:'chat',last:random(20)}]:undefined));break;
  case 3:ops.push(emit('chats.delete',[chat]));break;
  case 4:ops.push(emit('contacts.upsert',[{id:chat,name:'Name'+random(4)}]));break;
  case 5:ops.push(emit('contacts.update',[{id:chat,imgUrl:random(2)?'changed':null}]));break;
  case 6:ops.push(emit('messages.upsert',{messages:[{...msg(id),key:k,messageTimestamp:random(100)}],type:random(2)?'notify':'append'}));break;
  case 7:ops.push(emit('messages.update',[{key:k,update:{status:random(6),starred:!!random(2)}}]));break;
  case 8:ops.push(emit('messages.delete',random(2)?{keys:[k]}:{jid:chat,all:true}));break;
  case 9:ops.push(emit('messages.reaction',[{key:k,reaction:reactions[random(reactions.length)]}]));break;
  case 10:ops.push(emit('message-receipt.update',[{key:k,receipt:{userJid:'reader'+random(2),readTimestamp:random(100)}}]));break;
  case 11:ops.push(emit('groups.update',[{id:chat,subject:'Subject'+random(3)}]));break;
  case 12:ops.push({op:'flush'},{op:'buffer'});break;
  case 13:ops.push(emit('settings.update',{setting:'locale',value:'en'}));break;
  case 14:ops.push({op:'buffer'});break;
 }}streams.push(ops)}
 const originalSet=globalThis.setTimeout,originalClear=globalThis.clearTimeout,cases=[];
 for(const stream of streams){let now=0,next=1;const timers=new Map();globalThis.setTimeout=(fn,delay)=>{const id=next++;timers.set(id,{fn,at:now+delay});return id};globalThis.clearTimeout=id=>timers.delete(id);const b=makeEventBuffer({debug(){},trace(){},warn(){}}),out=[],functions=new Map();b.process(map=>out.push(JSON.parse(JSON.stringify(map))));const operations=[{op:'buffer'},...stream,{op:'flush'}],steps=[];
  for(const operation of operations){let result,error;try{switch(operation.op){case'buffer':b.buffer();break;case'flush':result=b.flush();break;case'destroy':b.destroy();b.process(map=>out.push(JSON.parse(JSON.stringify(map))));break;case'emit':{const data=structuredClone(operation.data);if(operation.conditions)for(let i=0;i<operation.conditions.length;i++){const c=operation.conditions[i];if(c)data[i].conditional=d=>c.kind==='throw'?(()=>{throw Error('condition failed')})():c.kind==='false'?false:c.kind==='true'?true:c.kind==='missing'?undefined:(()=>{const chat=d.historySets.chats[data[i].id]||d.chatUpserts[data[i].id];return chat?c.last===null?true:c.last>=Number(chat.lastMessageRecvTimestamp||0):undefined})()};b.emit(operation.event,data);break;}case'begin':{let resolve,reject;const promise=b.createBufferedFunction(()=>new Promise((a,z)=>{resolve=a;reject=z}))();promise.catch(()=>{});functions.set(operation.id,{promise,resolve,reject});break;}case'end':{const f=functions.get(operation.id);operation.success?f.resolve():f.reject(Error('work failed'));try{await f.promise}catch{}break;}case'tick':{const end=operation.now;while(true){const first=[...timers].filter(([,t])=>t.at<=end).sort((a,z)=>a[1].at-z[1].at||a[0]-z[0])[0];if(!first)break;timers.delete(first[0]);now=first[1].at;first[1].fn()}now=end;break;}}}catch(e){error=e.message}steps.push({buffering:b.isBuffering(),result,error,emitted:out.splice(0)})}
  b.destroy();cases.push({operations,steps});
 }
 globalThis.setTimeout=originalSet;globalThis.clearTimeout=originalClear;
 const cacheCases=[];for(const size of [9999,10000,10001]){const b=makeEventBuffer({debug(){},trace(){},warn(){}}),out=[];b.process(map=>out.push(map));b.buffer();b.emit('messaging-history.set',hist(Array.from({length:size},(_,i)=>({id:'chat'+i}))));b.flush();b.buffer();b.emit('messaging-history.set',hist([{id:'chat0'}]));b.flush();cacheCases.push({size,counts:out.map(m=>m['messaging-history.set'].chats.length)});b.destroy();}
 await writeFile('tests/fixtures/event_buffer.json',JSON.stringify({cases,cacheCases},null,2)+'\n');console.log(`Generated ${cases.length} complete original event-buffer streams`)
}finally{await rm(temp,{recursive:true,force:true})}
