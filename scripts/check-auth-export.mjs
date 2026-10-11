// Used by the explicit Rust/Node interoperability test with synthetic snapshots.
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
import assert from 'node:assert/strict'
const folder=resolve(process.argv[2])
const upstream=resolve(process.argv[3]??'../Baileys')
const require=createRequire(resolve('.cache/fixture-tools/package.json'))
const curve=require('libsignal/src/curve.js')
const temp=await mkdtemp(join(tmpdir(),'baileys-auth-interop-'))
try {
  const generics=await readFile(join(upstream,'src/Utils/generics.ts'),'utf8')
  await writeFile(join(temp,'generics.mjs'),stripTypeScriptTypes(generics.slice(generics.indexOf('export const BufferJSON'),generics.indexOf('export const getKeyAuthor'))))
  let source=await readFile(join(upstream,'WAProto/index.js'),'utf8')
  source=source.replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href))
  await writeFile(join(temp,'proto.mjs'),source)
  await writeFile(join(temp,'auth-utils.mjs'),'export function initAuthCreds(){ throw Error("Rust snapshot did not contain credentials") }')
  source=stripTypeScriptTypes(await readFile(join(upstream,'src/Utils/use-multi-file-auth-state.ts'),'utf8'))
    .replace("import { Mutex } from 'async-mutex'",`import mutex from ${JSON.stringify(pathToFileURL(require.resolve('async-mutex')).href)}; const {Mutex}=mutex`)
    .replace("'../../WAProto/index.js'","'./proto.mjs'").replace("'./auth-utils'","'./auth-utils.mjs'").replace("'./generics'","'./generics.mjs'")
  await writeFile(join(temp,'multi-file.mjs'),source)
  const {useMultiFileAuthState}=await import(pathToFileURL(join(temp,'multi-file.mjs')))
  const {state,saveCreds}=await useMultiFileAuthState(folder)
  assert.equal(state.creds.me.id,'test:7@s.whatsapp.net')
  assert.equal(state.creds.customFutureField,'preserved')
  assert.equal(state.creds.me.imgUrl,null)
  assert.equal(curve.verifySignature(Buffer.concat([Buffer.from([5]),state.creds.signedIdentityKey.public]),Buffer.concat([Buffer.from([5]),state.creds.signedPreKey.keyPair.public]),state.creds.signedPreKey.signature),true)
  const appKey=(await state.keys.get('app-state-sync-key',['synthetic'])).synthetic
  assert.equal(appKey.timestamp.toString(),'9223372036854775807')
  assert.deepEqual([...appKey.keyData],Array(32).fill(7))
  for(const type of ['pre-key','session','sender-key','sender-key-memory','app-state-sync-version','lid-mapping','device-list','tctoken','identity-key'])assert.ok((await state.keys.get(type,['synthetic'])).synthetic,type)
  state.creds.registered=false;state.creds.accountSyncCounter++
  await saveCreds()
  await state.keys.set({'identity-key':{synthetic:Buffer.alloc(33,31)},'device-list':{synthetic:null}})
  console.log('Upstream adapter resumed and updated the synthetic Rust export')
}finally{await rm(temp,{recursive:true,force:true})}
