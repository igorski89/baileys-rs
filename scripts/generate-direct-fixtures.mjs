// The libsignal runtime is an interoperability oracle only; no source is copied.
// All identities, sessions and plaintext below are synthetic.
import {createRequire} from 'node:module'
import {createPrivateKey,createPublicKey} from 'node:crypto'
import {writeFile} from 'node:fs/promises'
import {resolve} from 'node:path'
const require=createRequire(resolve('.cache/fixture-tools/package.json'))
const signal=require('libsignal'),curve=require('libsignal/src/curve.js')
const hex=b=>Buffer.from(b).toString('hex')
function pair(byte){const privKey=Buffer.alloc(32,byte);privKey[0]&=248;privKey[31]=(privKey[31]&127)|64;const der=createPrivateKey({key:Buffer.concat([Buffer.from('302e020100300506032b656e04220420','hex'),privKey]),format:'der',type:'pkcs8'});const pubKey=Buffer.concat([Buffer.from([5]),createPublicKey(der).export({format:'der',type:'spki'}).subarray(-32)]);return {privKey,pubKey}}
function storage(byte){const identity=pair(byte),signed=pair(byte+1),pre=pair(byte+2);let session=null;const removed=[];return {identity,signed,pre,removed,async getOurIdentity(){return identity},async getOurRegistrationId(){return byte},async isTrustedIdentity(){return true},async loadSignedPreKey(){return signed},async loadPreKey(id){return id===7?pre:undefined},async removePreKey(id){removed.push(id)},async loadSession(){return session?signal.SessionRecord.deserialize(session):null},async storeSession(id,record){session=record.serialize()},snapshot(){return session}}}
const alice=storage(1),bob=storage(11),aliceAddr=new signal.ProtocolAddress('bob',1),bobAddr=new signal.ProtocolAddress('alice',1)
const bundle={registrationId:11,identityKey:bob.identity.pubKey,signedPreKey:{keyId:5,publicKey:bob.signed.pubKey,signature:curve.calculateSignature(bob.identity.privKey,bob.signed.pubKey)},preKey:{keyId:7,publicKey:bob.pre.pubKey}}
const deterministic=[pair(31),pair(32),pair(33),pair(34),pair(35),pair(36)],original=curve.generateKeyPair;let index=0;const generated=[]
curve.generateKeyPair=()=>{const key=deterministic[index++]??pair(40+index);generated.push(key);return key}
const builder=new signal.SessionBuilder(alice,aliceAddr),ac=new signal.SessionCipher(alice,aliceAddr),bc=new signal.SessionCipher(bob,bobAddr)
const messages=[]
const keyInfo=s=>({identity:hex(s.identity.privKey),signed:hex(s.signed.privKey),pre:hex(s.pre.privKey),registration:s===alice?1:11})
const snapshot=()=>({alice:structuredClone(alice.snapshot()),bob:structuredClone(bob.snapshot())})
try{
  await builder.initOutgoing(bundle);const initial=snapshot()
  async function exchange(sender,data,deliver=true){const cipher=sender==='alice'?ac:bc,receiver=sender==='alice'?bc:ac;const encrypted=await cipher.encrypt(data);const wire=Buffer.from(encrypted.body,'binary');if(deliver){const plain=encrypted.type===3?await receiver.decryptPreKeyWhisperMessage(wire):await receiver.decryptWhisperMessage(wire);if(hex(plain)!==hex(data))throw Error('roundtrip mismatch')};messages.push({sender,data:hex(data),type:encrypted.type===3?'pkmsg':'msg',ciphertext:hex(wire),delivered:deliver,...snapshot()});return wire}
  await exchange('alice',Buffer.from('first prekey message'))
  await exchange('alice',Buffer.from('second prekey message'))
  await exchange('bob',Buffer.from('first reply'))
  for(const length of [0,1,15,16,17,256])await exchange('alice',Buffer.from(Array.from({length},(_,i)=>i%251)))
  await exchange('bob',Buffer.from('ratcheted reply'))
  const delayed=await exchange('alice',Buffer.from('delayed'),false)
  await exchange('alice',Buffer.from('ahead'))
  if(hex(await bc.decryptWhisperMessage(delayed))!==hex(Buffer.from('delayed')))throw Error('delayed mismatch')
  const afterDelayed=snapshot()
  const noPreStart=generated.length,noPreAlice=storage(1),noPreBob=storage(11)
  await new signal.SessionBuilder(noPreAlice,aliceAddr).initOutgoing({...bundle,preKey:undefined})
  const noPreInitial=structuredClone(noPreAlice.snapshot()),noPreData=Buffer.from('without a one-time prekey')
  const noPreEncrypted=await new signal.SessionCipher(noPreAlice,aliceAddr).encrypt(noPreData)
  const noPreWire=Buffer.from(noPreEncrypted.body,'binary')
  const noPrePlain=await new signal.SessionCipher(noPreBob,bobAddr).decryptPreKeyWhisperMessage(noPreWire)
  if(hex(noPrePlain)!==hex(noPreData))throw Error('no-prekey mismatch')
  const noPreKey={ephemeral:generated.slice(noPreStart).map(p=>hex(p.privKey)),initial:noPreInitial,data:hex(noPreData),ciphertext:hex(noPreWire),alice:noPreAlice.snapshot(),bob:noPreBob.snapshot()}
  await writeFile('tests/fixtures/direct.json',JSON.stringify({alice:keyInfo(alice),bob:keyInfo(bob),bundle:{registrationId:11,identityKey:hex(bundle.identityKey),signedPreKey:{keyId:5,publicKey:hex(bundle.signedPreKey.publicKey),signature:hex(bundle.signedPreKey.signature)},preKey:{keyId:7,publicKey:hex(bundle.preKey.publicKey)}},ephemeral:generated.map(p=>hex(p.privKey)),initial,messages,afterDelayed,removed:bob.removed,noPreKey},null,2)+'\n')
  console.log(`Generated ${messages.length} direct-message exchanges and delayed delivery`)
}finally{curve.generateKeyPair=original}
