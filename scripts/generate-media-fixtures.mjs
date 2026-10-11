// Run the pinned MIT media helpers against synthetic protobufs.
import {stripTypeScriptTypes, createRequire} from 'node:module'
import {readFile, writeFile, mkdtemp, rm} from 'node:fs/promises'
import {resolve, join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream = resolve(process.argv[2] ?? '../Baileys')
const require = createRequire(resolve('.cache/fixture-tools/package.json'))
const temp = await mkdtemp(join(tmpdir(), 'baileys-media-'))
try {
  const source = (await readFile(join(upstream, 'WAProto/index.js'), 'utf8')).replace('"protobufjs/minimal.js"', JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href))
  await writeFile(join(temp, 'proto.mjs'), source)
  for (const name of ['constants', 'jid-utils']) {
    let text = await readFile(join(upstream, `src/WABinary/${name}.ts`), 'utf8')
    text = text.replace(/export enum WAJIDDomains \{([^}]+)\}/, (_, body) => 'export const WAJIDDomains = {' + body.replaceAll('=', ':') + '}')
    await writeFile(join(temp, `${name}.mjs`), stripTypeScriptTypes(text).replaceAll("'./constants'", "'./constants.mjs'"))
  }
  const sendSource=await readFile(join(upstream,'src/Socket/messages-send.ts'),'utf8')
  const refreshSource=sendSource.slice(sendSource.indexOf('const refreshMediaConn ='),sendSource.indexOf('/**',sendSource.indexOf('const refreshMediaConn =')))
  const refreshFactory="export function mediaConnFactory(query){let mediaConn;let mediaHost=DEF_MEDIA_HOST;const S_WHATSAPP_NET='@s.whatsapp.net',logger={debug(){}};"+refreshSource+"return {refresh:refreshMediaConn,host:()=>mediaHost}}"
  const media=await readFile(join(upstream,'src/Utils/messages-media.ts'),'utf8'),defaults=await readFile(join(upstream,'src/Defaults/index.ts'),'utf8')
  const mapping=defaults.slice(defaults.indexOf('export const MEDIA_PATH_MAP'),defaults.indexOf('export type MediaType'))
  const raw=media.slice(media.indexOf('export const getRawMediaUploadData'),media.indexOf('export async function getMediaKeys'))
  const getKeys=media.slice(media.indexOf('export async function getMediaKeys'),media.indexOf('/** Extracts video thumb'))
  const stream=media.slice(media.indexOf('export const encryptedStream'),media.indexOf('export const DEF_MEDIA_HOST'))
  const download=media.slice(media.indexOf('export const DEF_MEDIA_HOST'),media.indexOf('export function extensionForMediaMessage'))
  const uploadFlow=media.slice(media.indexOf('export const getWAUploadToServer'),media.indexOf('const getMediaRetryKey'))
  const retry=media.slice(media.indexOf('const getMediaRetryKey'))
  const upload=media.slice(media.indexOf('export const encodeBase64EncodedStringForUpload'),media.indexOf('export const generateProfilePicture'))
  const info=media.slice(media.indexOf('export const hkdfInfoKey'),media.indexOf('export const getRawMediaUploadData'))
  const binary=await readFile(join(upstream,'src/WABinary/generic-utils.ts'),'utf8')
  const children=binary.slice(binary.indexOf('const indexCache'),binary.indexOf('export const getBinaryNodeChildUInt'))
  const prefix=`import {Boom} from ${JSON.stringify(pathToFileURL(require.resolve('@hapi/boom')).href)};import CryptoOriginal from 'node:crypto';import {Readable,Transform} from 'node:stream';import {once} from 'node:events';import {createWriteStream,promises as fs} from 'node:fs';import {join} from 'node:path';import {proto} from './proto.mjs';import {jidNormalizedUser} from './jid-utils.mjs';const Crypto={...CryptoOriginal,randomBytes:n=>Buffer.from(Array.from({length:n},(_,i)=>i))},DEFAULT_ORIGIN='https://web.whatsapp.com';let sequence=0;const generateMessageIDV2=()=> 'FIXTURE'+sequence++;const tmpdir=()=> ${JSON.stringify(temp)};const getTmpFilesDirectory=()=> ${JSON.stringify(temp)};const hkdf=(key,length,opts)=>Buffer.from(CryptoOriginal.hkdfSync('sha256',key,Buffer.alloc(0),opts.info,length));const aesEncryptGCM=(p,k,iv,aad)=>{const c=CryptoOriginal.createCipheriv('aes-256-gcm',k,iv);c.setAAD(aad);return Buffer.concat([c.update(p),c.final(),c.getAuthTag()])};const aesDecryptGCM=(p,k,iv,aad)=>{const c=CryptoOriginal.createDecipheriv('aes-256-gcm',k,iv);c.setAAD(aad);c.setAuthTag(p.subarray(-16));return Buffer.concat([c.update(p.subarray(0,-16)),c.final()])};const getStream=async bytes=>({stream:Readable.from([bytes.subarray(0,7),bytes.subarray(7)]),type:'buffer'});let body,request;export function setBody(value){body=value;request=undefined};export function getRequest(){return request};const getHttpStream=async(url,opts)=>{request={url,headers:opts.headers};const range=opts.headers.Range?.match(/^bytes=(\d+)-(\d*)$/);const data=range?body.subarray(+range[1],range[2]?+range[2]+1:undefined):body;return Readable.from([data])};`
  // String.raw is needed so the helper's HTTP range parser keeps its backslashes.
  const uploadSeam=`let uploadActions,uploadCalls;export function setUploads(actions){uploadActions=[...actions];uploadCalls=[]};export function uploadCallsSnapshot(){return uploadCalls};const uploadMedia=async request=>{uploadCalls.push(request);const action=uploadActions.shift();if(action?.error)throw new Error(action.error);return action?.result??undefined};`
  const fixedPrefix=(prefix+uploadSeam).replace('/^bytes=(d+)-(d*)$/','/^bytes=(\\d+)-(\\d*)$/')
  await writeFile(join(temp,'media.mjs'),fixedPrefix+stripTypeScriptTypes([mapping,children,info,raw,getKeys,upload,stream,download,retry,uploadFlow,refreshFactory].join('\n')))
  const h=await import(pathToFileURL(join(temp,'media.mjs'))),{proto}=await import(pathToFileURL(join(temp,'proto.mjs')))
  const hex=b=>Buffer.from(b).toString('hex'),key=Buffer.from(Array.from({length:32},(_,i)=>i))
  const types=Object.keys(h.MEDIA_HKDF_KEY_MAPPING),keys=[],encrypted=[]
  for(const type of types){const k=await h.getMediaKeys(key,type);keys.push({type,info:h.hkdfInfoKey(type),path:h.MEDIA_PATH_MAP[type]??null,iv:hex(k.iv),cipher:hex(k.cipherKey),mac:hex(k.macKey)});for(const length of [0,1,15,16,17,32,257]){
    const bytes=Buffer.from(Array.from({length},(_,i)=>i%251)),out=await h.encryptedStream(bytes,type,{saveOriginalFileIfRequired:length===17});const data=await readFile(out.encFilePath);
    encrypted.push({type,plain:hex(bytes),encrypted:hex(data),key:hex(out.mediaKey),mac:hex(out.mac),plainHash:hex(out.fileSha256),encryptedHash:hex(out.fileEncSha256),length:out.fileLength,original:out.originalFilePath?hex(await readFile(out.originalFilePath)):null});await rm(out.encFilePath);if(out.originalFilePath)await rm(out.originalFilePath)
  }}
  const keyStrings=['','data:;base64,',key.toString('base64'),'data:;base64,'+key.toString('base64'),key.toString('base64url'),'a','!!YX !Jj== trailing','not?base64']
  const encodedKeys=[];for(const input of keyStrings){try{const out=await h.getMediaKeys(input,'image');encodedKeys.push({input,iv:hex(out.iv),cipher:hex(out.cipherKey),mac:hex(out.macKey)})}catch(e){encodedKeys.push({input,error:e.message})}}
  const sample=encrypted.find(e=>e.type==='image'&&e.length===257),uploadBody=Buffer.from(sample.encrypted,'hex'),ranges=[]
  const rangeInputs=[{},...[0,1,15,16,17,31,32,33,255].flatMap(startByte=>[{startByte},{startByte,endByte:64}]),...[0,1,15,16,17,64,257,270,400].map(endByte=>({endByte})),{startByte:16,endByte:0},{startByte:64,endByte:32},{startByte:0,endByte:0}]
  for(const range of rangeInputs){h.setBody(uploadBody);try{const out=await h.downloadEncryptedContent('https://example.com/media',await h.getMediaKeys(key,'image'),{...range,options:{headers:{Custom:'value',Origin:'override'}}});const chunks=[];for await(const chunk of out)chunks.push(chunk);ranges.push({range,request:h.getRequest(),output:hex(Buffer.concat(chunks))})}catch(e){ranges.push({range,request:h.getRequest(),error:e.message})}}
  const urls=[];for(const input of [{directPath:'/path'},{directPath:'/path',url:'https://custom.example:8443/file'},{directPath:'/path',url:'https://custom.example/file',host:'explicit.example'},{directPath:'/path',url:'https://[::1]:8443/file'},{directPath:'/path',url:'mailto:a@example.com'},{directPath:'/path',url:'bad'},{directPath:'/path',host:''},{url:'https://custom.example/file'},{directPath:'',url:'https://custom.example/file'},{}]){h.setBody(uploadBody);try{const out=await h.downloadContentFromMessage({...input,mediaKey:key},'image',{host:input.host});for await(const chunk of out){}urls.push({input,url:h.getRequest().url})}catch(e){urls.push({input,error:e.message,status:e.output?.statusCode})}}
  const tokenInputs=['+/==','a b+/_=','💚+','ABC==','a=b=c','~!*()\''];const tokens=tokenInputs.map(input=>({input,output:h.encodeBase64EncodedStringForUpload(input)}))
  const retryRequests=[],retryEvents=[],notifications=[]
  for(const target of [{id:'ID',remoteJid:'200@s.whatsapp.net',fromMe:false},{id:'Unicode-💚',remoteJid:'group@g.us',participant:'300:2@lid',fromMe:true},{id:'',remoteJid:'status@broadcast',participant:'',fromMe:false}]){
    const req=h.encryptMediaRetryRequest(target,key,'100:7@s.whatsapp.net');retryRequests.push({key:target,node:req});
    const bytes=proto.MediaRetryNotification.encode({stanzaId:target.id,directPath:'/new',result:proto.MediaRetryNotification.ResultType.SUCCESS}).finish();const retryKey=Buffer.from((await import('node:crypto')).hkdfSync('sha256',key,Buffer.alloc(0),'WhatsApp Media Retry Notification',32));const iv=Buffer.alloc(12,4);const c=(await import('node:crypto')).createCipheriv('aes-256-gcm',retryKey,iv);c.setAAD(Buffer.from(target.id));const cipher=Buffer.concat([c.update(bytes),c.final(),c.getAuthTag()]);notifications.push({id:target.id,ciphertext:hex(cipher),iv:hex(iv),output:hex(proto.MediaRetryNotification.encode(h.decryptMediaRetryData({ciphertext:cipher,iv},key,target.id)).finish())})
  }
  for(const code of [0,1,2,3,4,99,'bad',undefined]){const node={tag:'notification',attrs:{id:'ID'},content:[{tag:'rmr',attrs:{jid:'group@g.us',from_me:'true',participant:'300@lid'}},{tag:'error',attrs:{...(code===undefined?{}:{code:String(code)}),text:'failed'}}]};try{const e=h.decodeMediaRetryNode(node);retryEvents.push({node,key:e.key,status:e.error.output.statusCode,code:e.error.message})}catch(e){retryEvents.push({node,error:e.message})}}
  for(const content of [[],[{tag:'encrypt',attrs:{},content:[{tag:'enc_p',attrs:{},content:Buffer.alloc(0)},{tag:'enc_iv',attrs:{},content:Buffer.alloc(0)}]}]]){const node={tag:'notification',attrs:{id:'ID'},content:[{tag:'rmr',attrs:{jid:'200@lid',from_me:'false'}},...content]},e=h.decodeMediaRetryNode(node);retryEvents.push({node,key:e.key,status:e.error?.output.statusCode??null,media:e.media?{ciphertext:hex(e.media.ciphertext),iv:hex(e.media.iv)}:null})}
  const messageSource=await readFile(join(upstream,'src/Utils/messages.ts'),'utf8');
  const prepareSource=messageSource.slice(messageSource.indexOf('export const prepareWAMessageMedia'),messageSource.indexOf('export const prepareDisappearingMessageSettingContent'));
  const mediaMaps=messageSource.slice(messageSource.indexOf('const MIMETYPE_MAP'),messageSource.indexOf('/**',messageSource.indexOf('const MessageTypeProto')));
  const color=messageSource.slice(messageSource.indexOf('const assertColor'),messageSource.indexOf('export const prepareWAMessageMedia'));
  const propertyAndContent=messageSource.slice(messageSource.indexOf('export const hasNonNullishProperty'),messageSource.indexOf('export const generateWAMessageFromContent'))
  const reportingSource=await readFile(join(upstream,'src/Utils/reporting-utils.ts'),'utf8')
  const shouldReport=reportingSource.slice(reportingSource.indexOf('export const shouldIncludeReportingToken'),reportingSource.indexOf('const generateMsgSecretKey'))
  const prepPrefix=`import {proto} from './proto.mjs';import {encryptedStream as encryptOriginal,getRawMediaUploadData as rawOriginal,MEDIA_PATH_MAP} from './media.mjs';import {promises as fs} from 'node:fs';import {isJidNewsletter} from './jid-utils.mjs';import {Boom} from ${JSON.stringify(pathToFileURL(require.resolve('@hapi/boom')).href)};const WAProto=proto,MEDIA_KEYS=Object.keys(MEDIA_PATH_MAP),unixTimestampSeconds=()=>1700000000,randomBytes=n=>Buffer.alloc(n,7);class Date extends globalThis.Date{static now(){return 1700000000123}};let mode,calls,uploads;export function setup(value){mode=value;calls=[];uploads=[]};export function trace(){return {calls,uploads}};const bytes=value=>Buffer.isBuffer(value)?value:Buffer.from(Array.from({length:17},(_,i)=>i));const encryptedStream=(media,type,opts)=>encryptOriginal(bytes(media),type,opts);const getRawMediaUploadData=(media,type,logger)=>rawOriginal(bytes(media),type,logger);const generateThumbnail=async(path,type)=>{calls.push({kind:'thumbnail',type,original:!!path});if(mode==='fail')throw new Error('metadata failure');return {thumbnail:Buffer.from([9,8]).toString('base64'),originalImageDimensions:{width:320,height:200}}};const getAudioDuration=async path=>{calls.push({kind:'duration',original:!!path});if(mode==='fail')throw new Error('metadata failure');return 3};const getAudioWaveform=async path=>{calls.push({kind:'waveform',original:!!path});if(mode==='wave-fail')throw new Error('wave failure');return Buffer.from([1,2,3])};export const upload=async(path,opts)=>{uploads.push({...opts,bytes:(await fs.readFile(path)).toString('hex')});if(mode==='upload-fail')throw new Error('upload failure');return {mediaUrl:'https://media.example/file',directPath:'/media/file'}};`;
  await writeFile(join(temp,'prepare.mjs'),prepPrefix+stripTypeScriptTypes(mediaMaps+color+prepareSource+propertyAndContent+shouldReport));
  const prep=await import(pathToFileURL(join(temp,'prepare.mjs'))),preparedCases=[];
  for(const type of ['image','video','audio','document','sticker'])for(const newsletter of [false,true])for(const mode of ['ok','fail'])for(const fields of [{},{mimetype:'',fileName:'',caption:'caption',ptt:true,ptv:type==='video',seconds:0,width:0,height:5,jpegThumbnail:[],waveform:[]},{mimetype:'custom/type',fileName:'named',caption:'caption',ptt:true,width:77,height:88},{url:'override',directPath:'override-path',mediaKey:[99],fileSha256:[98],fileEncSha256:[97],fileLength:99,mediaKeyTimestamp:7,stickerSentTs:42}]) {
    prep.setup(mode);const request={[type]:Buffer.from(Array.from({length:17},(_,i)=>i)),...fields},options={jid:newsletter?'channel@newsletter':'200@s.whatsapp.net',upload:prep.upload,backgroundColor:'#123456',mediaUploadTimeoutMs:500};let output,error;try{output=proto.Message.encode(await prep.prepareWAMessageMedia(request,options)).finish()}catch(e){error=e.message}preparedCases.push({type,newsletter,mode,fields,template:hex(proto.Message.encode(proto.Message.fromObject({[type+'Message']:fields})).finish()),output:output?hex(output):null,error,...prep.trace()});
  }
  for(const backgroundColor of ['',0,1,-1,'#abc','  ']){
    const type='audio',fields={ptt:true,seconds:0,waveform:[],backgroundArgb:42};prep.setup('ok');const request={audio:Buffer.from(Array.from({length:17},(_,i)=>i)),...fields};
    const output=await prep.prepareWAMessageMedia(request,{upload:prep.upload,backgroundColor,mediaUploadTimeoutMs:500});
    preparedCases.push({type,newsletter:false,mode:'ok',fields,backgroundColor,template:hex(proto.Message.encode(proto.Message.fromObject({audioMessage:fields})).finish()),output:hex(proto.Message.encode(output).finish()),...prep.trace()});
  }
  for(const type of ['image','video','audio','document','sticker']) {
    prep.setup('ok');let cached;const requests=[{[type]:{url:'https://input.example/file'}},{[type]:{url:'https://input.example/file'},caption:'second',mimetype:'custom/type',seconds:0,ptv:true,contextInfo:{expiration:7}}];const cache={get:async()=>cached,set:async(key,value)=>{cached=value}};const outputs=[];for(const request of requests)outputs.push(hex(proto.Message.encode(await prep.prepareWAMessageMedia(request,{upload:prep.upload,mediaCache:cache})).finish()));preparedCases.push({type,cache:true,outputs,...prep.trace()})
  }
  await writeFile('tests/fixtures/media_preparation.json',JSON.stringify(preparedCases,null,2)+'\n');
  const mediaContent=[];
  for(const kind of ['ptv','product'])for(const newsletter of [false,true])for(const modifier of [{},{mentions:['300@lid'],mentionAll:true},{viewOnce:true,mentions:['300@lid']},{edit:{id:'EDIT',remoteJid:'200@s.whatsapp.net',fromMe:true}}]){
    prep.setup('ok');const bytes=Buffer.from(Array.from({length:17},(_,i)=>i));const input=kind==='ptv'?{ptv:true,video:bytes,caption:'ignored',seconds:7}:{product:{productId:'P',title:'Title',priceAmount1000:99,productImage:bytes},businessOwnerJid:'200@lid',body:'body',contextInfo:{expiration:7}};
    const options={jid:newsletter?'channel@newsletter':'200@s.whatsapp.net',upload:prep.upload};const output=await prep.generateWAMessageContent({...input,...modifier},options);
    const template=kind==='product'?proto.Message.fromObject({productMessage:{...input,product:{...input.product,productImage:undefined}}}):{};
    mediaContent.push({kind,newsletter,modifier,template:hex(proto.Message.encode(template).finish()),output:hex(proto.Message.encode(output).finish()),...prep.trace()});
  }
  await writeFile('tests/fixtures/media_content.json',JSON.stringify(mediaContent,null,2)+'\n');
  const extraction=messageSource.slice(messageSource.indexOf('export const getContentType'),messageSource.indexOf('export const getDevice'));
  const assertSource=messageSource.slice(messageSource.indexOf('export const assertMediaContent'));
  const updateSource=sendSource.slice(sendSource.indexOf('updateMediaMessage: async'),sendSource.indexOf('sendMessage: async',sendSource.indexOf('updateMediaMessage: async'))).trim().replace('updateMediaMessage: async','export const updateMediaMessage = async').replace(/,$/,'');
  const downloadSource=messageSource.slice(messageSource.indexOf('export const downloadMediaMessage'),messageSource.indexOf('/** Checks whether'));
  const retryPrefix=`import {proto} from './proto.mjs';import {encryptMediaRetryRequest,decryptMediaRetryData,getStatusCodeForMediaRetry,getUrlFromDirectPath} from './media.mjs';import {Boom} from ${JSON.stringify(pathToFileURL(require.resolve('@hapi/boom')).href)};import {Readable} from 'node:stream';const authState={creds:{me:{id:'100:7@s.whatsapp.net'}}},logger={debug(){}};let updates,mediaHost,sent,emitted,downloads;export function retrySetup(value,host){updates=value;mediaHost=host;sent=[];emitted=[];downloads=[]};export function retryTrace(){return {sent,emitted,downloads}};const sendNode=async node=>sent.push(node),waitForMsgMediaUpdate=async check=>{if(!await check(updates))throw new Error('not matched')},ev={emit:(name,value)=>emitted.push({name,value})};const REUPLOAD_REQUIRED_STATUS=[410,404],downloadContentFromMessage=async(download,type,options)=>{downloads.push({download,type,options});return Readable.from([Buffer.from([1,2,3])])};`;
  await writeFile(join(temp,'media-retry-message.mjs'),retryPrefix+stripTypeScriptTypes([extraction,assertSource,updateSource,downloadSource].join("\n")));
  const retryMessage=await import(pathToFileURL(join(temp,'media-retry-message.mjs'))),retryMessageCases=[],downloadMessageCases=[];
  const runtime=obj=>proto.WebMessageInfo.decode(proto.WebMessageInfo.encode(proto.WebMessageInfo.fromObject(obj)).finish());
  const payload={mediaKey:Array.from(key),url:'https://old.example/media',directPath:'/old'};
  const roots=['documentMessage','imageMessage','videoMessage','audioMessage','stickerMessage'].map(type=>({[type]:payload}));
  const nested=[{buttonsMessage:{imageMessage:payload}},{templateMessage:{hydratedFourRowTemplate:{documentMessage:payload}}},{templateMessage:{hydratedTemplate:{videoMessage:payload}}},{templateMessage:{fourRowTemplate:{imageMessage:payload}}}];
  const wrappers=['ephemeralMessage','viewOnceMessage','documentWithCaptionMessage','viewOnceMessageV2','viewOnceMessageV2Extension','editedMessage','associatedChildMessage','groupStatusMessage','groupStatusMessageV2'].map(type=>({[type]:{message:{imageMessage:payload}}}));
  const inputs=[...roots,...nested,...wrappers,{ptvMessage:payload},{extendedTextMessage:{mediaKey:Array.from(key),thumbnailDirectPath:'/thumb'}},{conversation:'text'},{},null,{imageMessage:payload,documentMessage:{...payload,caption:'priority'}}];
  const crypto=await import('node:crypto');
  for(const input of inputs){for(const variant of ['success','missingPath','missingResult','unknownResult','deviceError','notificationError','tampered']){
    const message=runtime({key:{id:'ID',remoteJid:'200@s.whatsapp.net',fromMe:false},message:input}),before=hex(proto.WebMessageInfo.encode(message).finish());
    const event={key:{id:'ID',remoteJid:'different@lid',fromMe:true}};
    if(variant==='notificationError')event.error=new (await import(pathToFileURL(require.resolve('@hapi/boom')).href)).Boom('failed',{statusCode:503});
    else{const plain=proto.MediaRetryNotification.encode({stanzaId:'different-stanza',...(variant==='missingResult'?{}:{result:variant==='unknownResult'?99:variant==='deviceError'?proto.MediaRetryNotification.ResultType.NOT_FOUND:proto.MediaRetryNotification.ResultType.SUCCESS}),...(variant==='missingPath'?{}:{directPath:'/new'})}).finish();const k=Buffer.from(crypto.hkdfSync('sha256',key,Buffer.alloc(0),'WhatsApp Media Retry Notification',32)),iv=Buffer.alloc(12,4),c=crypto.createCipheriv('aes-256-gcm',k,iv);c.setAAD(Buffer.from('ID'));const ciphertext=Buffer.concat([c.update(plain),c.final(),c.getAuthTag()]);if(variant==='tampered')ciphertext[0]^=1;event.media={ciphertext,iv};}
    retryMessage.retrySetup([event],'custom.example:8443');let error,status;try{await retryMessage.updateMediaMessage(message)}catch(e){error=e.message;status=e.output?.statusCode}
    retryMessageCases.push({variant,input:before,event:convert({key:event.key,media:event.media,error:event.error?{status:503}:undefined}),output:hex(proto.WebMessageInfo.encode(message).finish()),error,status,...convert(retryMessage.retryTrace())});
  }
  retryMessage.retrySetup([],undefined);const message=runtime({message:input});let output,error,status;try{output=hex(await retryMessage.downloadMediaMessage(message,'buffer',{}))}catch(e){error=e.message;status=e.output?.statusCode}downloadMessageCases.push({input:hex(proto.WebMessageInfo.encode(message).finish()),output,error,status,...convert(retryMessage.retryTrace())});}
  await writeFile('tests/fixtures/media_message_retry.json',JSON.stringify(retryMessageCases,null,2)+'\n');
  await writeFile('tests/fixtures/media_message_download.json',JSON.stringify(downloadMessageCases,null,2)+'\n');
  const initial={hosts:[{hostname:'first.example',maxContentLengthBytes:100},{hostname:'second.example',maxContentLengthBytes:200}],auth:'old +/',ttl:60};
  const updated={...initial,hosts:[{hostname:'changed.example',maxContentLengthBytes:300}],auth:'new +/'};
  const uploadCases=[];
  const scenarios=[
    {name:'first success',actions:[{result:{url:'https://media/file',direct_path:'/file',ts:123,fbid:456,meta_hmac:'META'}}]},
    {name:'invalid response refreshes auth retains hosts',actions:[{}, {result:{direct_path:'/new'}}]},
    {name:'transport failure advances without refresh',actions:[{error:'offline'}, {result:{url:'https://media/file'}}]},
    {name:'custom hosts precede server hosts',custom:[{hostname:'custom.example',maxContentLengthBytes:5}],actions:[{error:'offline'},{result:{url:'https://media/file'}}]},
    {name:'refresh failure continues',refreshError:true,actions:[{},{}]},
    {name:'empty URLs trigger refresh',actions:[{result:{url:'',direct_path:''}}, {result:{url:'https://media/file'}}]},
    {name:'no hosts',initial:{...initial,hosts:[]},actions:[]},
    {name:'all hosts failed',actions:[{error:'one'},{error:'two'}]},
  ];
  for(const scenario of scenarios){h.setUploads(scenario.actions);const refreshes=[];const refresh=async force=>{refreshes.push(force);if(force&&scenario.refreshError)throw new Error('refresh failed');return force?updated:scenario.initial??initial};const config={customUploadHosts:scenario.custom??[],logger:{debug(){},warn(){}},options:{headers:{Origin:'override','Content-Type':'wrong',Custom:'value'}}};let result,error,status;try{result=await h.getWAUploadToServer(config,refresh)('/synthetic/file',{mediaType:'image',fileEncSha256B64:'+/==',timeoutMs:500})}catch(e){error=e.message;status=e.output?.statusCode}uploadCases.push({...scenario,initial:scenario.initial??initial,updated,result,error,status,refreshes,requests:h.uploadCallsSnapshot()})}
  await writeFile('tests/fixtures/media_upload.json',JSON.stringify(uploadCases,null,2)+'\n');
  const connectionQueries=[];let connectionNumber=0;
  const factory=h.mediaConnFactory(async node=>{connectionQueries.push(node);connectionNumber++;return {tag:'iq',attrs:{},content:[{tag:'media_conn',attrs:{auth:'auth'+connectionNumber,ttl:'60'},content:connectionNumber===2?[]:[{tag:'host',attrs:{hostname:'media'+connectionNumber+'.example',maxContentLengthBytes:'1048576'}}]}]}});
  const connectionResults=[];for(const force of [false,false,true,false]){const out=await factory.refresh(force);connectionResults.push({force,auth:out.auth,ttl:out.ttl,hosts:out.hosts,host:factory.host()})}
  await writeFile('tests/fixtures/media_connection.json',JSON.stringify({queries:connectionQueries,results:connectionResults},null,2)+'\n');
  function convert(value){if(Buffer.isBuffer(value))return {hex:hex(value)};if(Array.isArray(value))return value.map(convert);if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([k,v])=>[k,convert(v)]));return value}
  await writeFile('tests/fixtures/media.json',JSON.stringify(convert({keys,encrypted,encodedKeys,ranges,urls,tokens,retryRequests,retryEvents,notifications}),null,2)+'\n');console.log(`Generated ${keys.length} media types, ${encrypted.length} encryptions and ${ranges.length} downloads`)
}finally{await rm(temp,{recursive:true,force:true})}
