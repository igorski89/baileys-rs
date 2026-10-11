// Synthetic query/parser fixtures from upstream USync and Signal stanza helpers.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-usync-'))
const hex=b=>Buffer.from(b).toString('hex')
const clean=v=>v instanceof Date?v.getTime():Buffer.isBuffer(v)?{type:'Buffer',data:v.toString('base64')}:Array.isArray(v)?v.map(clean):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).filter(([,v])=>v!==undefined).map(([k,v])=>[k,clean(v)])):v
try {
  let proto=await readFile(join(upstream,'WAProto/index.js'),'utf8');proto=proto.replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href));await writeFile(join(temp,'proto.mjs'),proto)
  for(const name of ['constants','jid-utils','encode','decode','generic-utils']) {
    let source=await readFile(join(upstream,`src/WABinary/${name}.ts`),'utf8')
    source=source.replace(/export enum WAJIDDomains \{([^}]+)\}/,(_,body)=>'export const WAJIDDomains = {'+body.replaceAll('=',':')+'}')
    source=stripTypeScriptTypes(source).replaceAll("'./constants'","'./constants.mjs'").replaceAll("'./jid-utils'","'./jid-utils.mjs'").replaceAll("'../../WAProto/index.js'","'./proto.mjs'").replace(/import \{\s*\} from '\.\/types'\s*/g,'').replace("import { Boom } from '@hapi/boom'","class Boom extends Error {constructor(message,options){super(message);this.output={statusCode:options?.statusCode}}}")
    await writeFile(join(temp,`${name}.mjs`),source)
  }
  await writeFile(join(temp,'binary.mjs'),"export * from './generic-utils.mjs';export * from './jid-utils.mjs';")
  const names=['USyncUser','USyncQuery','Protocols/USyncDeviceProtocol','Protocols/USyncContactProtocol','Protocols/USyncStatusProtocol','Protocols/USyncDisappearingModeProtocol','Protocols/USyncUsernameProtocol','Protocols/UsyncLIDProtocol','Protocols/UsyncBotProfileProtocol','Protocols/index']
  for(const name of names) {
    let source=stripTypeScriptTypes(await readFile(join(upstream,`src/WAUSync/${name}.ts`),'utf8')).replace(/'\.\.\/(?:\.\.\/)?WABinary'/g,"'./binary.mjs'").replace(/from '\.\.?\/([^']+)'/g,(all,path)=>all.includes('binary.mjs')?all:`from './${path.replace(/^Protocols\//,'')}.mjs'`).replaceAll("'./Protocols.mjs'","'./index.mjs'")
    await writeFile(join(temp,`${name.split('/').pop()}.mjs`),source)
  }
  const {USyncQuery}=await import(pathToFileURL(join(temp,'USyncQuery.mjs'))),{USyncUser}=await import(pathToFileURL(join(temp,'USyncUser.mjs'))),{encodeBinaryNode}=await import(pathToFileURL(join(temp,'encode.mjs'))),{decodeBinaryNode}=await import(pathToFileURL(join(temp,'decode.mjs')))
  const socketSource=await readFile(join(upstream,'src/Socket/socket.ts'),'utf8'),body=socketSource.slice(socketSource.indexOf('const executeUSyncQuery ='),socketSource.indexOf('const onWhatsApp ='))
  await writeFile(join(temp,'build.mjs'),stripTypeScriptTypes('export async function build(usyncQuery){const S_WHATSAPP_NET="@s.whatsapp.net",generateMessageTag=()=>"sid";let stanza;const query=async n=>{stanza=n};'+body+'await executeUSyncQuery(usyncQuery);return stanza;}'))
  const {build}=await import(pathToFileURL(join(temp,'build.mjs')))
  const protocolMethods={devices:'withDeviceProtocol',contact:'withContactProtocol',status:'withStatusProtocol',disappearing_mode:'withDisappearingModeProtocol',bot:'withBotProfileProtocol',lid:'withLIDProtocol',username:'withUsernameProtocol'}
  const configurations=[
    {protocols:['devices','lid'],users:[{id:'100@s.whatsapp.net'},{id:'200:7@lid'}]},
    {protocols:['contact'],users:[{id:'100@s.whatsapp.net',phone:'+100'},{username:'sample',usernameKey:'1234',lid:'900@lid'},{type:'out'},{id:'300@s.whatsapp.net'}]},
    {protocols:['status','disappearing_mode','username'],users:[{id:'100@s.whatsapp.net'}],context:'background',mode:'full'},
    {protocols:['bot','lid'],users:[{id:'123@bot',personaId:'synthetic',lid:'900@lid'}]},
    {protocols:['devices','contact','status','disappearing_mode','lid','username'],users:[]}
  ]
  const text=(tag,value)=>({tag,attrs:{},content:Buffer.from(value)})
  const response={tag:'iq',attrs:{type:'result'},content:[{tag:'usync',attrs:{},content:[{tag:'list',attrs:{},content:[{tag:'user',attrs:{jid:'100@s.whatsapp.net'},content:[{tag:'contact',attrs:{type:'in'}},{tag:'lid',attrs:{val:'900@lid'}},{tag:'status',attrs:{t:'1700000000'},content:Buffer.from('available')},{tag:'disappearing_mode',attrs:{duration:'86400',t:'1700000001'}},{tag:'username',attrs:{},content:Buffer.from('sample')},{tag:'devices',attrs:{},content:[{tag:'device-list',attrs:{},content:[{tag:'device',attrs:{id:'0'}},{tag:'device',attrs:{id:'7','key-index':'4'}},{tag:'device',attrs:{id:'99','key-index':'5',is_hosted:'true'}},{tag:'device',attrs:{id:'8','key-index':'6'}},{tag:'ignored',attrs:{id:'100'}}]},{tag:'key-index-list',attrs:{ts:'1700000000',expected_ts:'1700000002'},content:Buffer.from([1,2,3])}]}]},{tag:'user',attrs:{jid:'200@lid'},content:[{tag:'contact',attrs:{type:'out'}},{tag:'status',attrs:{code:'401'}},{tag:'unknown',attrs:{}}]},{tag:'user',attrs:{},content:[]}]}]}]}
  const all=new USyncQuery();for(const name of Object.keys(protocolMethods).filter(n=>n!=='bot'))all[protocolMethods[name]]()
  const decoded=await decodeBinaryNode(encodeBinaryNode(response))
  const queries=[]
  for(const config of configurations){let q=new USyncQuery();for(const p of config.protocols)q[protocolMethods[p]]();q.users=config.users.map(v=>Object.assign(new USyncUser(),v));if(config.context)q.context=config.context;if(config.mode)q.mode=config.mode;queries.push({config:{...config,context:q.context,mode:q.mode},wire:hex(encodeBinaryNode(await build(q))),parsed:clean(q.parseUSyncQueryResult(decoded))})}
  const bot={tag:'bot',attrs:{jid:'123@bot'},content:[{tag:'bot',attrs:{},content:[{tag:'profile',attrs:{persona_id:'synthetic'},content:[text('name','Assistant'),text('attributes','helpful'),text('description','Synthetic bot'),text('category','test'),{tag:'default',attrs:{}},{tag:'commands',attrs:{},content:[text('description','Commands'),{tag:'command',attrs:{},content:[text('name','hello'),text('description','Say hello')]}]},{tag:'prompts',attrs:{},content:[{tag:'prompt',attrs:{},content:[text('emoji','*'),text('text','Try this')]}]}]}]}]}
  const {USyncBotProfileProtocol}=await import(pathToFileURL(join(temp,'UsyncBotProfileProtocol.mjs')))
  const {extractDeviceJids}=await loadSignalHelpers()
  const parsed=all.parseUSyncQueryResult(decoded),devices=[false,true].map(exclude=>({exclude,result:clean(extractDeviceJids(parsed.list,'100:7@s.whatsapp.net','900',exclude))}))
  const helpers=await loadSignalHelpers(),direct=JSON.parse(await readFile('tests/fixtures/direct.json','utf8')),b=direct.bundle
  const buf=hex=>Buffer.from(hex,'hex'),id=(tag,n,width)=>({tag,attrs:{},content:Buffer.from(Array.from({length:width},(_,i)=>(n>>>((width-i-1)*8))&255))})
  const signed={tag:'skey',attrs:{},content:[id('id',5,3),{tag:'value',attrs:{},content:buf(b.signedPreKey.publicKey).subarray(1)},{tag:'signature',attrs:{},content:buf(b.signedPreKey.signature)}]},pre={tag:'key',attrs:{},content:[id('id',7,3),{tag:'value',attrs:{},content:buf(b.preKey.publicKey).subarray(1)}]}
  const user={tag:'user',attrs:{jid:'200:1@s.whatsapp.net'},content:[id('registration',11,4),{tag:'identity',attrs:{},content:buf(b.identityKey).subarray(1)},signed,pre]}
  const bundles={tag:'iq',attrs:{type:'result'},content:[{tag:'list',attrs:{},content:[user]}]},receipt={tag:'receipt',attrs:{},content:[id('registration',11,4),{tag:'keys',attrs:{},content:[{tag:'type',attrs:{},content:Buffer.from([5])},{tag:'identity',attrs:{},content:buf(b.identityKey).subarray(1)},signed,pre]}]}
  const injected=[];await helpers.parseAndInjectE2ESessions(bundles,{async injectE2ESession(session){injected.push(clean(session))}})
  const signal={bundles:hex(encodeBinaryNode(bundles)),receipt:hex(encodeBinaryNode(receipt)),injected,retry:clean(helpers.extractE2ESessionFromRetryReceipt(receipt))}
  await writeFile('tests/fixtures/usync.json',JSON.stringify({queries,response:hex(encodeBinaryNode(response)),bot:hex(encodeBinaryNode(bot)),botParsed:clean(new USyncBotProfileProtocol().parser(bot)),devices,signal},null,2)+'\n')
  console.log(`Generated ${queries.length} USync query fixtures and all protocol parsers`)
  async function loadSignalHelpers(){let source=await readFile(join(upstream,'src/Utils/signal.ts'),'utf8');const start=source.indexOf('export const extractE2ESessionFromRetryReceipt'),end=source.indexOf('/**\n * get the next N keys');source=source.slice(start,end);await writeFile(join(temp,'signal.mjs'),stripTypeScriptTypes('import {getBinaryNodeChild,getBinaryNodeChildBuffer,getBinaryNodeChildUInt,getBinaryNodeChildren,assertNodeErrorFree} from "./generic-utils.mjs";import {jidDecode,getServerFromDomainType,WAJIDDomains} from "./jid-utils.mjs";const KEY_BUNDLE_TYPE=Buffer.from([5]);const generateSignalPubKey=b=>b.length===33?b:Buffer.concat([Buffer.from([5]),b]);const isValidUInt=n=>typeof n==="number"&&Number.isInteger(n);const chunk=(a,n)=>Array.from({length:Math.ceil(a.length/n)},(_,i)=>a.slice(i*n,(i+1)*n));'+source));return import(pathToFileURL(join(temp,'signal.mjs')))}
}finally{await rm(temp,{recursive:true,force:true})}
