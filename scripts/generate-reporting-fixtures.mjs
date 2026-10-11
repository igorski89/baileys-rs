// Reporting selector and HMAC fixtures from original upstream code with synthetic secrets.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-reporting-')),hex=b=>b===null?null:Buffer.from(b).toString('hex')
try {
  let proto=await readFile(join(upstream,'WAProto/index.js'),'utf8');await writeFile(join(temp,'proto.mjs'),proto.replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href)))
  let source=stripTypeScriptTypes(await readFile(join(upstream,'src/Utils/reporting-utils.ts'),'utf8')).replaceAll("'../../WAProto/index.js'","'./proto.mjs'").replace("'./crypto'",JSON.stringify(pathToFileURL(resolve('.cache/fixture-tools/node_modules/whatsapp-rust-bridge/dist/index.js')).href))
  source+='\nexport {reportingFields};export const extract=data=>extractReportingTokenContent(data,compiledReportingFields);'
  await writeFile(join(temp,'reporting.mjs'),source)
  const helpers=await import(pathToFileURL(join(temp,'reporting.mjs'))),{proto:models}=await import(pathToFileURL(join(temp,'proto.mjs')))
  await writeFile('proto/reporting-fields.json',JSON.stringify(helpers.reportingFields,null,2)+'\n')
  const encode=m=>models.Message.encode(m).finish(),secret=Buffer.alloc(32,7),base={conversation:'reporting text',messageContextInfo:{messageSecret:secret}},key={id:'ABC',remoteJid:'200@s.whatsapp.net',fromMe:true}
  const messages=[base,{imageMessage:{url:'excluded',caption:'image',mimetype:'image/jpeg',mediaKey:Buffer.alloc(32,9),contextInfo:{stanzaId:'quote',participant:'200@s.whatsapp.net',expiration:86400,ephemeralSettingTimestamp:1700000000}}},{extendedTextMessage:{text:'text',matchedText:'ignored',canonicalUrl:'ignored',contextInfo:{expiration:123}}},{contactMessage:{displayName:'contact',vcard:'card'}},{locationMessage:{degreesLatitude:1.5,degreesLongitude:2.5,name:'place'}},{protocolMessage:{type:0,key:{id:'reference',remoteJid:'100@s.whatsapp.net'},editedMessage:{conversation:'private edited text'}}},{viewOnceMessage:{message:{conversation:'wrapped'}}},{reactionMessage:{}},{encReactionMessage:{}},{encEventResponseMessage:{}},{pollUpdateMessage:{}},{conversation:''},{},...Array.from({length:16},(_,i)=>({...base,padding:i+1}))]
  const cases=[]
  for(const [index,m] of messages.entries()){let message={messageContextInfo:{messageSecret:secret},...m};delete message.padding;let bytes=Buffer.from(encode(message));if(m.padding)bytes=Buffer.concat([bytes,Buffer.alloc(m.padding,m.padding)]);const k=index%2?{...key,fromMe:false,participant:'100:7@s.whatsapp.net'}:key;const node=await helpers.getMessageReportingToken(bytes,message,k);cases.push({wire:hex(bytes),message:hex(encode(message)),key:k,include:helpers.shouldIncludeReportingToken(message),content:hex(helpers.extract(bytes)),token:node?hex(node.content[0].content):null})}
  for(const k of [{...key,id:'💚',remoteJid:'é@lid'},{...key,id:''},{...key,participant:'300@g.us'}]){const node=await helpers.getMessageReportingToken(Buffer.from(encode(base)),base,k);cases.push({wire:hex(encode(base)),message:hex(encode(base)),key:k,include:true,content:hex(helpers.extract(Buffer.from(encode(base)))),token:node?hex(node.content[0].content):null})}
  const raw=['','0a03616263','0a036162','0b00','0801','08020801','0a016212040a01782001','08808080808000','0880808080808000','0580808080','090102030405060708','faffffff0701','0a01780802'].map(wire=>({wire,content:hex(helpers.extract(Buffer.from(wire,'hex')))}))
  await writeFile('tests/fixtures/reporting.json',JSON.stringify({cases,raw},null,2)+'\n')
  console.log(`Generated ${cases.length} reporting tokens and ${raw.length} raw selector cases`)
}finally{await rm(temp,{recursive:true,force:true})}
