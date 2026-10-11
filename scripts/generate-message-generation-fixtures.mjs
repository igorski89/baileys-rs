// Run the pinned MIT message construction helpers against synthetic protobufs.
import {stripTypeScriptTypes, createRequire} from 'node:module'
import {readFile, writeFile, mkdtemp, rm} from 'node:fs/promises'
import {resolve, join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream = resolve(process.argv[2] ?? '../Baileys')
const require = createRequire(resolve('.cache/fixture-tools/package.json'))
const temp = await mkdtemp(join(tmpdir(), 'baileys-generation-'))
try {
  const source = (await readFile(join(upstream, 'WAProto/index.js'), 'utf8')).replace('"protobufjs/minimal.js"', JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href))
  await writeFile(join(temp, 'proto.mjs'), source)
  for (const name of ['constants', 'jid-utils']) {
    let text = await readFile(join(upstream, `src/WABinary/${name}.ts`), 'utf8')
    text = text.replace(/export enum WAJIDDomains \{([^}]+)\}/, (_, body) => 'export const WAJIDDomains = {' + body.replaceAll('=', ':') + '}')
    await writeFile(join(temp, `${name}.mjs`), stripTypeScriptTypes(text).replaceAll("'./constants'", "'./constants.mjs'"))
  }
  const messages = await readFile(join(upstream, 'src/Utils/messages.ts'), 'utf8')
  const helpers = messages.slice(messages.indexOf('export const prepareDisappearingMessageSettingContent'), messages.indexOf('export const hasNonNullishProperty')) + messages.slice(messages.indexOf('export const generateWAMessageFromContent'), messages.indexOf('export const generateWAMessage =')) + messages.slice(messages.indexOf('export const getContentType'), messages.indexOf('export const getDevice'))
  await writeFile(join(temp, 'helpers.mjs'), `import {proto} from './proto.mjs';import {isJidNewsletter,isJidGroup,isJidStatusBroadcast,jidNormalizedUser} from './jid-utils.mjs';const WAProto=proto,WAMessageStatus=proto.WebMessageInfo.Status,WA_DEFAULT_EPHEMERAL=604800;class Boom extends Error{};const unixTimestampSeconds=d=>Math.floor(d.getTime()/1000),generateMessageIDV2=()=> 'FIXED';` + stripTypeScriptTypes(helpers))
  const {proto} = await import(pathToFileURL(join(temp, 'proto.mjs')))
  const h = await import(pathToFileURL(join(temp, 'helpers.mjs')))
  const hex = b => Buffer.from(b).toString('hex')
  const wire = m => hex(proto.Message.encode(m).finish())
  const obj = m => proto.Message.decode(proto.Message.encode(proto.Message.fromObject(m)).finish())
  const clone = m => proto.Message.decode(proto.Message.encode(m).finish())
  const call = fn => {try {return {output: fn()}} catch(e) {return {error: e.message}}}
  const schema = await readFile(join(upstream, 'WAProto/WAProto.proto'), 'utf8')
  const block = schema.split('message Message {')[1].split('    message ')[0]
  const fields = [...block.matchAll(/optional \w+ (\w+) = \d+;/g)].map(m => m[1])
  const classifications = [], forwards = [], generated = [], extracted = []
  const quote = proto.WebMessageInfo.fromObject({key:{remoteJid:'other@g.us',id:'QUOTE',participant:'300:9@c.us',fromMe:false},message:{ephemeralMessage:{message:{extendedTextMessage:{text:'quoted',contextInfo:{mentionedJid:['discard@lid'],forwardingScore:8}},imageMessage:{caption:'discard'}}}}})
  const inputs = [{}, ...fields.map(f => ({[f]: f === 'conversation' ? 'text' : {}})), {senderKeyDistributionMessage:{},extendedTextMessage:{text:'hello',contextInfo:{expiration:70,mentionedJid:['old@lid'],stanzaId:'old'}}}, {imageMessage:{contextInfo:{forwardingScore:4294967295}},extendedTextMessage:{text:'secondary'}}]
  for (const [, type, field] of block.matchAll(/optional (\w+) (\w+) = \d+;/g)) {
    const ctor = proto.Message[type] ?? proto[type]
    if (ctor && Object.hasOwn(ctor.prototype, 'contextInfo')) inputs.push({[field]:{contextInfo:{forwardingScore:3,isForwarded:false,mentionedJid:['old@lid'],expiration:77,remoteJid:'old@g.us'}}})
  }
  for (const input of inputs) {
    const m = obj(input)
    classifications.push({input:wire(m),type:h.getContentType(m) ?? null})
    for (const fromMe of [false,true]) for (const force of [false,true]) {
      const envelope = proto.WebMessageInfo.fromObject({key:{fromMe},message:m})
      forwards.push({input:hex(proto.WebMessageInfo.encode(envelope).finish()),force,...call(()=>wire(h.generateForwardMessageContent(envelope,force)))})
    }
    for (const mode of ['plain','quote','ephemeral','both']) {
      const options = {userJid:'100:7@s.whatsapp.net',messageId:'GENERATED',timestamp:new Date(1700000000123),...(mode === 'quote' || mode === 'both' ? {quoted:proto.WebMessageInfo.decode(proto.WebMessageInfo.encode(quote).finish())} : {}),...(mode === 'ephemeral' || mode === 'both' ? {ephemeralExpiration:3600} : {})}
      generated.push({input:wire(m),jid:'200@s.whatsapp.net',mode,...call(()=>hex(proto.WebMessageInfo.encode(h.generateWAMessageFromContent('200@s.whatsapp.net',clone(m),options)).finish()))})
    }
  }
  forwards.push({input:hex(proto.WebMessageInfo.encode({key:{}}).finish()),force:false,...call(()=>wire(h.generateForwardMessageContent({key:{}},false)))})
  const wrappers=['ephemeralMessage','viewOnceMessage','documentWithCaptionMessage','viewOnceMessageV2','viewOnceMessageV2Extension','editedMessage','associatedChildMessage','groupStatusMessage','groupStatusMessageV2']
  let deep={imageMessage:{caption:'deep'}}
  const special=[...wrappers.map(w=>({[w]:{message:{imageMessage:{caption:w}}}}))]
  for(let i=0;i<6;i++)deep={ephemeralMessage:{message:deep}}
  special.push(deep,{ephemeralMessage:{message:{viewOnceMessage:{message:{extendedTextMessage:{text:'nested'}}}}}})
  for (const input of special) for (const jid of ['group@g.us','status@broadcast','channel@newsletter']) {
    const m=obj(input),options={userJid:'100:7@s.whatsapp.net',messageId:'GENERATED',timestamp:new Date(1700000000123),quoted:quote,ephemeralExpiration:3600}
    generated.push({input:wire(m),jid,mode:'both',...call(()=>hex(proto.WebMessageInfo.encode(h.generateWAMessageFromContent(jid,clone(m),options)).finish()))})
  }
  const quotes=[{key:{remoteJid:'200@s.whatsapp.net',fromMe:true,id:'Q'},message:{conversation:'own'}},{key:{remoteJid:'200@s.whatsapp.net',fromMe:false,id:'Q',participant:'300:1@lid'},participant:'400:2@s.whatsapp.net',message:{imageMessage:{caption:'caption',contextInfo:{expiration:7}}}},{key:{remoteJid:'200@s.whatsapp.net',id:'Q'},message:{conversation:'fallback'}}]
  quotes.push({key:{remoteJid:'200@s.whatsapp.net',id:'EMPTY'},message:{}}, {key:{remoteJid:'200@s.whatsapp.net',id:'MISSING'}}, {key:{remoteJid:'200@s.whatsapp.net',id:'META'},message:{messageContextInfo:{messageSecret:Buffer.alloc(32,1)}}})
  for (const q of quotes) {
    const m=obj({extendedTextMessage:{text:'new',contextInfo:{remoteJid:'retained@g.us',mentionedJid:['500@lid']}}}),quoted=proto.WebMessageInfo.fromObject(q)
    generated.push({input:wire(m),jid:'200@s.whatsapp.net',mode:'both',quote:hex(proto.WebMessageInfo.encode(quoted).finish()),...call(()=>hex(proto.WebMessageInfo.encode(h.generateWAMessageFromContent('200@s.whatsapp.net',m,{userJid:'100:7@s.whatsapp.net',messageId:'GENERATED',timestamp:new Date(1700000000123),quoted,ephemeralExpiration:3600})).finish()))})
  }
  const extractionInputs=[null,{},...special,...['imageMessage','documentMessage','videoMessage','locationMessage'].flatMap(field=>[{buttonsMessage:{[field]:{},contentText:'buttons'}},{templateMessage:{hydratedFourRowTemplate:{[field]:{},hydratedContentText:'hydrated'}}},{templateMessage:{hydratedTemplate:{[field]:{},hydratedContentText:'other'}}},{templateMessage:{fourRowTemplate:{[field]:{}}}}]),{buttonsMessage:{contentText:'buttons'}},{buttonsMessage:{}},{templateMessage:{hydratedTemplate:{hydratedContentText:'hydrated'}}},{templateMessage:{hydratedFourRowTemplate:{hydratedTitleText:'title'}}},{templateMessage:{fourRowTemplate:{}}},{templateMessage:{hydratedTemplate:{hydratedContentText:'preferred'},fourRowTemplate:{imageMessage:{}}}}]
  for (const input of extractionInputs) {const m=input===null?null:obj(input),out=h.extractMessageContent(m);extracted.push({input:m===null?null:wire(m),output:out===undefined?null:wire(out)})}
  const disappearing=[null,0,1,604800,4294967295].map(expiration=>({expiration,output:wire(h.prepareDisappearingMessageSettingContent(expiration??undefined))}))
  await writeFile('tests/fixtures/message_generation.json',JSON.stringify({quote:hex(proto.WebMessageInfo.encode(quote).finish()),classifications,forwards,generated,extracted,disappearing},null,2)+'\n')
  console.log(`Generated ${classifications.length} classifications, ${forwards.length} forwards, ${generated.length} envelopes, ${extracted.length} extractions`)
} finally {await rm(temp,{recursive:true,force:true})}
