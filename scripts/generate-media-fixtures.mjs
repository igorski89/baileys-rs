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
  const media=await readFile(join(upstream,'src/Utils/messages-media.ts'),'utf8'),defaults=await readFile(join(upstream,'src/Defaults/index.ts'),'utf8')
  const mapping=defaults.slice(defaults.indexOf('export const MEDIA_PATH_MAP'),defaults.indexOf('export type MediaType'))
  const getKeys=media.slice(media.indexOf('export async function getMediaKeys'),media.indexOf('/** Extracts video thumb'))
  const stream=media.slice(media.indexOf('export const encryptedStream'),media.indexOf('export const DEF_MEDIA_HOST'))
  const download=media.slice(media.indexOf('export const DEF_MEDIA_HOST'),media.indexOf('export function extensionForMediaMessage'))
  const retry=media.slice(media.indexOf('const getMediaRetryKey'))
  const upload=media.slice(media.indexOf('export const encodeBase64EncodedStringForUpload'),media.indexOf('export const generateProfilePicture'))
  const info=media.slice(media.indexOf('export const hkdfInfoKey'),media.indexOf('export const getRawMediaUploadData'))
  const binary=await readFile(join(upstream,'src/WABinary/generic-utils.ts'),'utf8')
  const children=binary.slice(binary.indexOf('const indexCache'),binary.indexOf('export const getBinaryNodeChildUInt'))
  const prefix=`import {Boom} from ${JSON.stringify(pathToFileURL(require.resolve('@hapi/boom')).href)};import CryptoOriginal from 'node:crypto';import {Readable,Transform} from 'node:stream';import {once} from 'node:events';import {createWriteStream,promises as fs} from 'node:fs';import {join} from 'node:path';import {proto} from './proto.mjs';import {jidNormalizedUser} from './jid-utils.mjs';const Crypto={...CryptoOriginal,randomBytes:n=>Buffer.from(Array.from({length:n},(_,i)=>i))},DEFAULT_ORIGIN='https://web.whatsapp.com';let sequence=0;const generateMessageIDV2=()=> 'FIXTURE'+sequence++;const getTmpFilesDirectory=()=> ${JSON.stringify(temp)};const hkdf=(key,length,opts)=>Buffer.from(CryptoOriginal.hkdfSync('sha256',key,Buffer.alloc(0),opts.info,length));const aesEncryptGCM=(p,k,iv,aad)=>{const c=CryptoOriginal.createCipheriv('aes-256-gcm',k,iv);c.setAAD(aad);return Buffer.concat([c.update(p),c.final(),c.getAuthTag()])};const aesDecryptGCM=(p,k,iv,aad)=>{const c=CryptoOriginal.createDecipheriv('aes-256-gcm',k,iv);c.setAAD(aad);c.setAuthTag(p.subarray(-16));return Buffer.concat([c.update(p.subarray(0,-16)),c.final()])};const getStream=async bytes=>({stream:Readable.from([bytes.subarray(0,7),bytes.subarray(7)]),type:'buffer'});let body,request;export function setBody(value){body=value;request=undefined};export function getRequest(){return request};const getHttpStream=async(url,opts)=>{request={url,headers:opts.headers};const range=opts.headers.Range?.match(/^bytes=(\d+)-(\d*)$/);const data=range?body.subarray(+range[1],range[2]?+range[2]+1:undefined):body;return Readable.from([data])};`
  // String.raw is needed so the helper's HTTP range parser keeps its backslashes.
  const fixedPrefix=prefix.replace('/^bytes=(d+)-(d*)$/','/^bytes=(\\d+)-(\\d*)$/')
  await writeFile(join(temp,'media.mjs'),fixedPrefix+stripTypeScriptTypes([mapping,children,info,getKeys,upload,stream,download,retry].join('\n')))
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
  function convert(value){if(Buffer.isBuffer(value))return {hex:hex(value)};if(Array.isArray(value))return value.map(convert);if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([k,v])=>[k,convert(v)]));return value}
  await writeFile('tests/fixtures/media.json',JSON.stringify(convert({keys,encrypted,encodedKeys,ranges,urls,tokens,retryRequests,retryEvents,notifications}),null,2)+'\n');console.log(`Generated ${keys.length} media types, ${encrypted.length} encryptions and ${ranges.length} downloads`)
}finally{await rm(temp,{recursive:true,force:true})}
