// Run the pinned MIT message construction helpers against synthetic protobufs.
import {stripTypeScriptTypes, createRequire} from 'node:module'
import {readFile, writeFile, mkdtemp, rm} from 'node:fs/promises'
import {resolve, join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream = resolve(process.argv[2] ?? '../Baileys')
const require = createRequire(resolve('.cache/fixture-tools/package.json'))
const temp = await mkdtemp(join(tmpdir(), 'baileys-content-'))
try {
  const source = (await readFile(join(upstream, 'WAProto/index.js'), 'utf8')).replace('"protobufjs/minimal.js"', JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href))
  await writeFile(join(temp, 'proto.mjs'), source)
  for (const name of ['constants', 'jid-utils']) {
    let text = await readFile(join(upstream, `src/WABinary/${name}.ts`), 'utf8')
    text = text.replace(/export enum WAJIDDomains \{([^}]+)\}/, (_, body) => 'export const WAJIDDomains = {' + body.replaceAll('=', ':') + '}')
    await writeFile(join(temp, `${name}.mjs`), stripTypeScriptTypes(text).replaceAll("'./constants'", "'./constants.mjs'"))
  }
  const messages = await readFile(join(upstream, 'src/Utils/messages.ts'), 'utf8')
  const reporting=await readFile(join(upstream,'src/Utils/reporting-utils.ts'),'utf8')
  const defaults=await readFile(join(upstream,'src/Defaults/index.ts'),'utf8')
  const helpers=messages.slice(messages.indexOf('export const extractUrlFromText'),messages.indexOf('export const prepareWAMessageMedia'))+messages.slice(messages.indexOf('export const prepareDisappearingMessageSettingContent'),messages.indexOf('export const generateWAMessageFromContent'))+messages.slice(messages.indexOf('export const getContentType'),messages.indexOf('export const extractMessageContent'))
  const sourceReporting=reporting.slice(reporting.indexOf('export const shouldIncludeReportingToken'),reporting.indexOf('const generateMsgSecretKey'))
  const url=defaults.split('\n').find(line=>line.startsWith('export const URL_REGEX'))
  await writeFile(join(temp,'helpers.mjs'),`import {proto} from './proto.mjs';let random=0,clock=0,prepared;export function reset(value){random=0;clock=0;prepared=value};const randomBytes=n=>Buffer.alloc(n,++random);class Date extends globalThis.Date {static now(){return 1700000000123+ ++clock}};const WAProto=proto,WA_DEFAULT_EPHEMERAL=604800,CALL_AUDIO_PREFIX='https://call.whatsapp.com/voice/',CALL_VIDEO_PREFIX='https://call.whatsapp.com/video/';class Boom extends Error {constructor(message,options){super(message);this.status=options?.statusCode}};const prepareWAMessageMedia=async()=>prepared;let http;export function setHttp(fn){http=fn};const fetch=(...args)=>http(...args);`+stripTypeScriptTypes([url,sourceReporting,helpers].join('\n')))
  const {proto}=await import(pathToFileURL(join(temp,'proto.mjs')))
  const h=await import(pathToFileURL(join(temp,'helpers.mjs')))
  const hex=b=>Buffer.from(b).toString('hex'),wire=m=>hex(proto.Message.encode(m).finish()),obj=m=>proto.Message.fromObject(m)
  const key={remoteJid:'200@s.whatsapp.net',id:'ORIGINAL',fromMe:true}
  const requests=[
    {text:''},{text:'plain text'},{text:'visit https://example.com/path?q=1'},{text:'https://user:pass@example.com and https://valid.example/path'},
    {text:'https://example.com',linkPreview:null},{text:'preview',linkPreview:{'matched-text':'https://example.com',title:'title',description:'description',jpegThumbnail:[1,2,3],highQualityThumbnail:{directPath:'/thumb',mediaKey:[4],mediaKeyTimestamp:1700,width:320,height:200,fileSha256:[5],fileEncSha256:[6]}}},
    {contacts:{displayName:'Contacts',contacts:[]}},{contacts:{displayName:'One',contacts:[{displayName:'Alice',vcard:'BEGIN:VCARD\nEND:VCARD'}]}},{contacts:{displayName:'Two',contacts:[{displayName:'Alice',vcard:'A',contextInfo:{mentionedJid:['old@lid']}},{displayName:'Bob',vcard:'B'}]}},
    {location:{degreesLatitude:47.37,degreesLongitude:8.54,name:'Zurich',address:'place',contextInfo:{expiration:77,mentionedJid:['old@lid']}}},
    {react:{key,text:'💚'}},{react:{key,text:'',senderTimestampMs:0}},{react:{key,text:'👍',senderTimestampMs:-5}},
    {delete:key},{disappearingMessagesInChat:true},{disappearingMessagesInChat:false},{disappearingMessagesInChat:60},
    {groupInvite:{inviteCode:'CODE',inviteExpiration:1700,text:'join',jid:'group@g.us',subject:'group'}},
    {pin:key,type:1},{pin:key,type:1,time:604800},{pin:key,type:1,time:0},{pin:key,type:2,time:2592000},
    {buttonReply:{id:'b1',displayText:'yes',index:2},type:'template'},{buttonReply:{id:'b2',displayText:'no',index:3},type:'plain'},
    {listReply:{title:'List',description:'reply',listType:1,singleSelectReply:{selectedRowId:'row'}}},
    {event:{name:'Event',startDate:1700000000999}},{event:{name:'Call',description:'description',startDate:1700000000999,endDate:1700000100555,call:'audio',isCancelled:true,extraGuestsAllowed:false,isScheduleCall:true,messageSecret:[]}},
    {event:{name:'Video',startDate:-1555,call:'video',location:{name:'remote'}}},
    {poll:{name:'Poll',values:['One','Two']}},{poll:{name:'Single',values:['One','Two'],selectableCount:1}},{poll:{name:'Community',values:['One','Two'],selectableCount:2,toAnnouncementGroup:true,messageSecret:[7]}},
    {poll:{name:'Empty',values:[]}},{poll:{name:'Invalid',values:['One'],selectableCount:2}},{poll:{name:'Negative',values:['One'],selectableCount:-1}},
    {album:{expectedImageCount:2,expectedVideoCount:1}},{album:{}},{sharePhoneNumber:false},{requestPhoneNumber:false},{limitSharing:true},{limitSharing:false},
    {forward:{key,message:{ephemeralMessage:{message:{conversation:'forward'}}}}},{forward:{key:{...key,fromMe:false},message:{imageMessage:{caption:'forward',contextInfo:{forwardingScore:4,mentionedJid:['old@lid']}}}},force:true},
    {prepared:{imageMessage:{caption:'prepared',contextInfo:{expiration:77,mentionedJid:['old@lid']}}}},{prepared:{reactionMessage:{text:'prepared'}}},{prepared:{pollUpdateMessage:{}}}
  ]
  const modifiers=[{}, {mentions:['300@lid']}, {mentionAll:true}, {contextInfo:{mentionedJid:[],expiration:0,forwardingScore:4,participant:'400@lid'}}, {viewOnce:true,mentions:['300@lid'],mentionAll:true}, {edit:key,contextInfo:{expiration:5}}, {albumParentKey:key,mentions:['300@lid'],contextInfo:{stanzaId:'context'}}]
  requests.push({poll:{name:'Empty secret',values:['A'],messageSecret:[]}}, {event:{name:'Zero end',startDate:0,endDate:0,messageSecret:[3]}}, {react:{key,text:'x',senderTimestampMs:5}})
  const cases=[]
  async function run(request,modifier={},options={},hooks={}) {
    const input=structuredClone({...request,...modifier})
    let kind=Object.keys(request)[0],payload={},forward=null,keyWire=null
    if(kind==='contacts')payload={contactsArrayMessage:request.contacts}
    if(kind==='location')payload={locationMessage:request.location}
    if(kind==='react')payload={reactionMessage:request.react}
    if(kind==='delete'||kind==='pin')keyWire=hex(proto.MessageKey.encode(proto.MessageKey.fromObject(request[kind])).finish())
    if(kind==='groupInvite')payload={groupInviteMessage:{inviteCode:request.groupInvite.inviteCode,inviteExpiration:request.groupInvite.inviteExpiration,caption:request.groupInvite.text,groupJid:request.groupInvite.jid,groupName:request.groupInvite.subject}}
    if(kind==='listReply')payload={listResponseMessage:request.listReply}
    if(kind==='forward'){const f=proto.WebMessageInfo.fromObject(request.forward);forward=hex(proto.WebMessageInfo.encode(f).finish());input.forward=f}
    if(kind==='prepared')payload=request.prepared
    if(kind==='event'){
      payload={eventMessage:{name:request.event.name,description:request.event.description,startTime:Math.floor(request.event.startDate/1000),endTime:request.event.endDate===undefined?undefined:Math.trunc(request.event.endDate/1000),location:request.event.location}}
      input.event.startDate=new Date(request.event.startDate);if(request.event.endDate!==undefined)input.event.endDate=new Date(request.event.endDate)
    }
    if(kind==='text'&&request.linkPreview?.highQualityThumbnail)payload={imageMessage:request.linkPreview.highQualityThumbnail}
    const calls=[]
    h.reset(obj(payload));h.setHttp(async()=> {if(hooks.thumbnailError)throw Error('thumbnail failed');return {ok:hooks.thumbnailOk!==false,arrayBuffer:async()=>Uint8Array.from([9,8,7]).buffer}})
    const config={...options, getUrlInfo:async url=>{calls.push(['preview',url]);if(hooks.previewError)throw Error('preview failed');return hooks.preview?{'matched-text':url,title:'hook preview'}:undefined},getProfilePicUrl:async jid=>{calls.push(['thumbnail',jid]);return hooks.thumbnail?'https://thumbnail.example/image':undefined},...(!hooks.noCall?{getCallLink:async(kind,opts)=>{calls.push(['call',kind,opts.startTime]);if(hooks.callError)throw Error('call failed');return 'TOKEN'}}:{}),logger:{warn(){}}}
    let output,error
    try {output=wire(await h.generateWAMessageContent(input,config))}catch(e){error={message:e.message,status:e.status??null}}
    cases.push({kind,request,modifier,options,hooks,payload:wire(obj(payload)),forward,keyWire,context:modifier.contextInfo?hex(proto.ContextInfo.encode(proto.ContextInfo.fromObject(modifier.contextInfo)).finish()):null,output:output??null,error:error??null,calls})
  }
  for(const request of requests)for(const modifier of modifiers)await run(request,modifier)
  for(const backgroundColor of ['#abc','123456','AA123456','  #001122  ','red','#','🦊','0x1234','-ffffff','#fffffffff','#123456789abcdef','#123456789abcdefabcdefabcdefabcdefabcdef',1,0,-1])await run({text:'color'},{},{backgroundColor,font:3})
  await run({location:{contextInfo:{participant:'old@lid',mentionedJid:['old@lid']}}},{contextInfo:{participant:null,mentionedJid:[]}})
  await run({text:'https://example.com'},{},{},{preview:true})
  await run({text:'https://example.com'},{},{},{previewError:true})
  for(const thumbnailOk of [true,false])await run(requests.find(r=>r.groupInvite),{},{},{thumbnail:true,thumbnailOk})
  await run(requests.find(r=>r.groupInvite),{},{},{thumbnail:true,thumbnailError:true})
  await run(requests.find(r=>r.event?.call),{},{},{callError:true})
  await run(requests.find(r=>r.event?.call),{},{},{noCall:true})
  const urls=['HTTP://example.com','http://example.com','https://localhost','https://127.0.0.1','https://a.example:8080/path?q=a#b','https://a.example:１２/path','https://user:pass@evil.com https://good.example/a','https://user.name@evil.com','https://example.com\u0085tail','https://example.com\ufefftail','https://example.com/a\u0085tail','https://example.com/a\ufefftail','https://.co','https://a.b','https://a.toolong9/path','https://a.bc:pass@evil.com https://x.yy','https://a.bc/pathhttps://x.yy']
  const extraction=urls.map(text=>({text,url:h.extractUrlFromText(text)??null}))
  await writeFile('tests/fixtures/message_content.json',JSON.stringify({cases,urls:extraction},null,2)+'\n')
  console.log(`Generated ${cases.length} content cases and ${extraction.length} URL cases`)
} finally {await rm(temp,{recursive:true,force:true})}
