// Run the MIT-licensed Baileys WAM encoder over hand-written buffers and over every event and global of its tables.
import {stripTypeScriptTypes} from 'node:module'
import {createHash} from 'node:crypto'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {join,resolve} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),temp=await mkdtemp(join(tmpdir(),'baileys-wam-'))
try {
  for(const name of ['constants','BinaryInfo','encode'])await writeFile(join(temp,name+'.mjs'),stripTypeScriptTypes(await readFile(join(upstream,`src/WAM/${name}.ts`),'utf8')).replace(/from '\.\/(\w+)'/g,"from './$1.mjs'"))
  const {encodeWAM}=await import(pathToFileURL(join(temp,'encode.mjs'))),{BinaryInfo}=await import(pathToFileURL(join(temp,'BinaryInfo.mjs'))),{WEB_EVENTS,WEB_GLOBALS}=await import(pathToFileURL(join(temp,'constants.mjs')))
  const event=(name,props={},globals={})=>({[name]:{props,globals}})
  // Long strings are recorded by length, and long encodings by digest.
  const long=n=>'é'.repeat(n),short=v=>typeof v==='string'&&v.length>200?{repeat:v[0],count:v.length}:Array.isArray(v)?v.map(short):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).map(([k,v])=>[k,short(v)])):v
  const inputs=[
    {events:[]},{sequence:1,events:[]},{sequence:65535,protocolVersion:7,events:[]},
    {events:[event('WamDroppedEvent')]},
    {events:[event('WamDroppedEvent',{droppedEventCode:5,droppedEventCount:300,isFromWamsys:true})]},
    {events:[event('WamDroppedEvent',{isFromWamsys:false,droppedEventCount:0,droppedEventCode:1})]},
    {events:[event('WamDroppedEvent',{droppedEventCode:null})]},{events:[event('WamDroppedEvent',{droppedEventCode:1,droppedEventCount:null})]},
    {events:[event('WamDroppedEvent',{notAField:1})]},{events:[event('NotAnEvent',{})]},{events:[event('WamDroppedEvent',{},{notAGlobal:1})]},
    ...[-1,-128,-129,127,128,-32768,-32769,32767,32768,-2147483648,-2147483649,2147483647,2147483648,9007199254740991,1.5,-0.25,-0,1e300].map(n=>({events:[event('WamDroppedEvent',{droppedEventCode:n})]})),
    ...[NaN,Infinity,-Infinity].map(n=>({special:String(n),events:[event('WamDroppedEvent',{droppedEventCode:n})]})),
    ...['','a','💚 text',long(127),long(128),long(32767),long(32768)].map(s=>({events:[event('MetaVerifiedInteraction',{businessOwnerJid:s})]})),
    {events:[event('MetaVerifiedInteraction',{businessOwnerJid:'100@s.whatsapp.net',businessOwnerPlatform:2,metaVerifiedInteractionAction:19},{appVersion:'2.3000.1',appIsBetaRelease:false,abKey2:null,beaconSessionId:123456789,appBuild:4})]},
    {sequence:3,events:[event('WamDroppedEvent',{droppedEventCode:1},{appIsBetaRelease:true}),event('WamDroppedEvent',{},{browser:'Chrome'}),event('MetaVerifiedInteraction',{businessOwnerJid:'x'})]}
  ]
  const run=input=>{const {special,...options}=input;let wire,error;try{wire=encodeWAM(new BinaryInfo(structuredClone(options))).toString('hex')}catch(e){error=typeof e==='string'?e:e.constructor.name}return {wire:wire??null,error:error??null}}
  const cases=inputs.map(input=>{
    // Non-finite numbers do not survive JSON; they are restored from their names.
    const plain=input.special?{...input,events:[event('WamDroppedEvent',{droppedEventCode:null})]}:input
    const {wire,error}=run(input)
    return {input:short(plain),...(wire&&wire.length>400?{sha256:createHash('sha256').update(Buffer.from(wire,'hex')).digest('hex')}:{wire}),error}})
  // Every event with all of its fields, and one event carrying every global, pin the generated tables.
  const tables=createHash('sha256')
  for(const e of WEB_EVENTS)tables.update(Buffer.from(run({events:[event(e.name,Object.fromEntries(Object.keys(e.props).map((k,i)=>[k,i+2])))]}).wire,'hex'))
  tables.update(Buffer.from(run({events:[event('WamDroppedEvent',{},Object.fromEntries(WEB_GLOBALS.map((g,i)=>[g.name,i+2])))]}).wire,'hex'))
  await writeFile('tests/fixtures/wam.json',JSON.stringify({cases,events:WEB_EVENTS.length,globals:WEB_GLOBALS.length,tables:tables.digest('hex')},null,1)+'\n')
  console.log(`Generated ${cases.length} original WAM encodings and a digest over ${WEB_EVENTS.length} events and ${WEB_GLOBALS.length} globals`)
}finally{await rm(temp,{recursive:true,force:true})}
