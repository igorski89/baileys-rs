// Run original processSyncAction, contact helpers and conditional closures.
import {stripTypeScriptTypes,createRequire} from 'node:module'
import {readFile,writeFile,mkdtemp,rm} from 'node:fs/promises'
import {resolve,join} from 'node:path'
import {tmpdir} from 'node:os'
import {pathToFileURL} from 'node:url'
const upstream=resolve(process.argv[2]??'../Baileys'),require=createRequire(resolve('.cache/fixture-tools/package.json')),temp=await mkdtemp(join(tmpdir(),'baileys-actions-'))
try {
 const protoSource=(await readFile(join(upstream,'WAProto/index.js'),'utf8')).replace('"protobufjs/minimal.js"',JSON.stringify(pathToFileURL(require.resolve('protobufjs/minimal.js')).href));await writeFile(join(temp,'proto.mjs'),protoSource)
 const source=await readFile(join(upstream,'src/Utils/chat-utils.ts'),'utf8'),contact=await readFile(join(upstream,'src/Utils/sync-action-utils.ts'),'utf8'),jid=await readFile(join(upstream,'src/WABinary/jid-utils.ts'),'utf8');
 const classifiers=jid.split('\n').filter(line=>/^export const (isPnUser|isLidUser) /.test(line)).join('\n');
 await writeFile(join(temp,'helpers.mjs'),`const LabelAssociationType={Chat:'label_jid',Message:'label_message'},toNumber=t=>typeof t==='object'&&t?('toNumber'in t?t.toNumber():t.low):t||0;`+stripTypeScriptTypes(classifiers+'\n'+contact.slice(contact.indexOf('export const processContactAction'))+'\n'+source.slice(source.indexOf('export const processSyncAction'))));
 const h=await import(pathToFileURL(join(temp,'helpers.mjs'))),{proto}=await import(pathToFileURL(join(temp,'proto.mjs')));
 const actions=[{},...['muteAction','pinAction','archiveChatAction','markChatAsReadAction','deleteMessageForMeAction','contactAction','pushNameSetting','unarchiveChatsSetting','starAction','deleteChatAction','labelEditAction','labelAssociationAction','localeSetting','timeFormatAction','pnForLidChatAction','privacySettingRelayAllCalls','statusPrivacy','lockChatAction','privacySettingDisableLinkPreviewsAction','notificationActivitySettingAction','lidContactAction','privacySettingChannelsPersonalisedRecommendationAction'].map(k=>({[k]:{}})),
  ...[false,true].flatMap(b=>[{muteAction:{muted:b,muteEndTimestamp:123}},{pinAction:{pinned:b},timestamp:123},{archiveChatAction:{archived:b,messageRange:{lastMessageTimestamp:10,lastSystemMessageTimestamp:99}}},{markChatAsReadAction:{read:b,messageRange:{lastSystemMessageTimestamp:25}}},{starAction:{starred:b}},{unarchiveChatsSetting:{unarchiveChats:b}},{labelAssociationAction:{labeled:b}},{lockChatAction:{locked:b}},{timeFormatAction:{isTwentyFourHourFormatEnabled:b}}]),
  {contactAction:{fullName:'Full',firstName:'First',username:'user',lidJid:'201@lid',pnJid:'ignored@s.whatsapp.net'}},{contactAction:{firstName:'First',pnJid:'300@s.whatsapp.net'}},{contactAction:{username:'user',lidJid:'201@hosted.lid'}},{lidContactAction:{username:'liduser',firstName:'First',fullName:'Full'}},
  {pushNameSetting:{name:'New'}},{pushNameSetting:{name:'Old'}},{labelEditAction:{name:'Label',color:4,deleted:false,predefinedId:0}},{labelEditAction:{name:'',color:0,deleted:true,predefinedId:-1}},
  {localeSetting:{locale:'de-CH'}},{pnForLidChatAction:{pnJid:'300@s.whatsapp.net'}},{notificationActivitySettingAction:{notificationActivitySetting:0}},{notificationActivitySettingAction:{notificationActivitySetting:3}},
  {statusPrivacy:{mode:1,userJid:['200@s.whatsapp.net']}},{privacySettingChannelsPersonalisedRecommendationAction:{isUserOptedOut:true}},{privacySettingDisableLinkPreviewsAction:{isPreviewsDisabled:false}},{privacySettingRelayAllCalls:{isEnabled:true}},
  {muteAction:{muted:true},pinAction:{pinned:true},contactAction:{fullName:'precedence'}},{notificationActivitySettingAction:{notificationActivitySetting:0},lidContactAction:{username:'fallback'}},{archiveChatAction:{archived:true,messageRange:{lastMessageTimestamp:0,lastSystemMessageTimestamp:99}}},{pinAction:{pinned:true},timestamp:'9007199254740993'}];
 const cases=[],me={id:'100@s.whatsapp.net',lid:'101@lid',name:'Old'};
 const indices=[['other','200@s.whatsapp.net','msg','1'],['label_jid','201@lid','msg','0'],['archive','200@s.whatsapp.net','msg','1'],['star','200@s.whatsapp.net','msg','1'],['deleteMessageForMe','200@s.whatsapp.net','msg','0'],['deleteChat','200@s.whatsapp.net','msg','0']];
 for(const object of actions)for(const index of indices)for(const mode of ['normal','initial','initialUnarchive']) {
  const action=proto.SyncActionValue.decode(proto.SyncActionValue.encode(proto.SyncActionValue.fromObject(object)).finish()),events=[],settings={unarchiveChats:mode==='initialUnarchive'};
  const ev={emit:(event,data)=>{let conditions;if(event==='chats.update'&&data[0].conditional){const test=(history,upsert)=>{const historySets={chats:{}},chatUpserts={};if(history!==undefined)historySets.chats[index[1]]=history;if(upsert!==undefined)chatUpserts[index[1]]=upsert;return data[0].conditional({historySets,chatUpserts})??null};conditions=[test(),test(undefined,{}),test({lastMessageRecvTimestamp:0}),test({lastMessageRecvTimestamp:10}),test({lastMessageRecvTimestamp:50}),test({lastMessageRecvTimestamp:10},{lastMessageRecvTimestamp:100})]};events.push({event,data:JSON.parse(JSON.stringify(data)),conditions})}};
  h.processSyncAction({syncAction:{value:action},index},ev,me,mode==='normal'?undefined:{accountSettings:settings});
  cases.push({action:Buffer.from(proto.SyncActionValue.encode(action).finish()).toString('hex'),index,mode,me,events,unarchive:settings.unarchiveChats});
 }
 await writeFile('tests/fixtures/app_state_actions.json',JSON.stringify(cases,null,2)+'\n');console.log(`Generated ${cases.length} original action and condition cases`)
}finally{await rm(temp,{recursive:true,force:true})}
