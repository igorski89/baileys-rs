// List every value upstream Baileys exports from src/Utils and src/WABinary and whether the Rust port has a counterpart.
// Usage: node scripts/audit-utils-api.mjs ../Baileys   (exit code 1 when exports are unaccounted for)
// Matching is by identifier only: it shows that a counterpart exists, not that it behaves the same.
import {readFileSync,readdirSync,statSync} from 'node:fs'
import {join,resolve} from 'node:path'
const upstream=resolve(process.argv[2]??'../Baileys')
const walk=dir=>readdirSync(dir).flatMap(name=>{const path=join(dir,name);return statSync(path).isDirectory()?walk(path):path.endsWith('.rs')?[path]:[]})
const rust=walk('src').map(path=>readFileSync(path,'utf8')).join('\n'),idents=new Set([...rust.matchAll(/\b(?:fn|struct|enum|trait|const|static|type|mod)\s+([A-Za-z0-9_]+)/g)].map(m=>m[1]))
const snake=name=>name.replace(/([a-z0-9])([A-Z])/g,'$1_$2').replace(/([A-Z]+)([A-Z][a-z])/g,'$1_$2').toLowerCase()
// Upstream names whose Rust counterpart is named differently.
const renamed={
  // generics
  BufferJSON:'serialization',getKeyAuthor:'key_author',writeRandomPadMax16:'pad_with_byte',unpadRandomMax16:'unpad',generateParticipantHashV2:'participant_hash_v2',encodeWAMessage:'encode',generateMessageIDV2:'generate_id_v2',generateMessageID:'generate_id',
  getCallStatusFromNode:'status',isWABusinessPlatform:'is_business_platform',fetchLatestBaileysVersion:'fetch_latest_baileys_version',fetchLatestWaWebVersion:'fetch_latest_wa_web_version',
  // decode-wa-message
  extractAddressingContext:'addressing_context',decodeMessageNode:'decode_node',decryptMessageNode:'decrypt_node',
  // messages
  extractUrlFromText:'detect_url',generateLinkPreviewIfRequired:'link_preview',prepareWAMessageMedia:'prepare_media_message',prepareDisappearingMessageSettingContent:'disappearing_setting',generateForwardMessageContent:'forward',generateWAMessageContent:'generate',generateWAMessageFromContent:'from_content',generateWAMessage:'generate',
  getContentType:'content_type',normalizeMessageContent:'normalize',extractMessageContent:'extract',getDevice:'device_kind',updateMessageWithReceipt:'update_with_receipt',updateMessageWithReaction:'update_with_reaction',updateMessageWithPollUpdate:'update_with_poll_update',updateMessageWithEventResponse:'update_with_event_response',
  getAggregateVotesInPollMessage:'aggregate_votes',getAggregateResponsesInEventMessage:'aggregate_event_responses',aggregateMessageKeysNotFromMe:'aggregate',downloadMediaMessage:'download_media_message_to',assertMediaContent:'download_content',
  // messages-media
  hkdfInfoKey:'hkdf_info',getRawMediaUploadData:'upload_raw',getMediaKeys:'Keys',encodeBase64EncodedStringForUpload:'upload_token',getAudioDuration:'audio_duration',getAudioWaveform:'audio_waveform',generateThumbnail:'thumbnail',getHttpStream:'get',encryptedStream:'encrypt_stream',
  DEF_MEDIA_HOST:'DEFAULT_MEDIA_HOST',getUrlFromDirectPath:'direct_path_url',downloadContentFromMessage:'download_message_to',downloadEncryptedContent:'download_to',extensionForMediaMessage:'media_extension',uploadWithNodeHttp:'post_file',getWAUploadToServer:'upload_media',
  encryptMediaRetryRequest:'retry_request',decodeMediaRetryNode:'decode_retry',decryptMediaRetryData:'decrypt_retry',getStatusCodeForMediaRetry:'retry_status',
  // validate-connection, crypto, signal, noise
  generateLoginNode:'login_payload',generateRegistrationNode:'registration_payload',generateSignalPubKey:'signal_public_key',Curve:'KeyPair',aesDecryptWithIV:'aes_decrypt_cbc_with_iv',aesEncrypWithIV:'aes_encrypt_cbc_with_iv',hmacSign:'hmac_sha256',
  generateOrGetPreKeys:'generate_or_get_prekeys',getPreKeys:'pre_key',extractE2ESessionFromRetryReceipt:'extract_retry_bundle',parseAndInjectE2ESessions:'inject_session_bundles',getNextPreKeys:'allocate_prekeys',getNextPreKeysNode:'upload_pre_keys',makeNoiseHandler:'NoiseHandler',
  // history, chat-utils
  downloadHistory:'download',processHistoryMessage:'process',downloadAndProcessHistorySyncNotification:'download_and_process',makeLtHashGenerator:'LtHashGenerator',newLTHashState:'new_lthash_state',isMissingKeyError:'is_missing_key',
  encodeSyncdPatch:'encode_patch',decodeSyncdMutations:'decode_mutations',decodeSyncdPatch:'decode_patch',extractSyncdPatches:'extract_collections',downloadExternalBlob:'BlobDownloader',downloadExternalPatch:'download_patch',decodeSyncdSnapshot:'decode_snapshot',chatModificationToAppPatch:'chat_modification_patch',processSyncAction:'process_action',
  // auth, link preview, events, process-message, the rest
  makeCacheableSignalKeyStore:'CachedKeyStore',addTransactionCapability:'transaction',assertMeId:'me_id',initAuthCreds:'Credentials',useMultiFileAuthState:'import_baileys_folder',getUrlInfo:'url_info',makeEventBuffer:'Buffer',
  cleanMessage:'clean',isRealMessage:'is_real',shouldIncrementChatUnread:'increments_unread',getChatId:'chat_id',Browsers:'Browser',getCompanionPlatformId:'companion_platform_id',buildPairingQRData:'pairing_qr',buildAckStanza:'acknowledgement',MessageRetryManager:'Manager',
  // WABinary
  getBinaryNodeChildren:'children',getBinaryNodeChild:'child',getAllBinaryNodeChildren:'all_children',getBinaryNodeChildBuffer:'child_buffer',getBinaryNodeChildString:'child_string',getBinaryNodeChildUInt:'child_uint',assertNodeErrorFree:'assert_error_free',reduceBinaryNodeToDictionary:'dictionary',getBinaryNodeMessages:'messages',binaryNodeToString:'to_tree_string',
  jidEncode:'encode',jidDecode:'parse',areJidsSameUser:'are_same_user',isJidMetaAI:'is_meta_ai',isJidBroadcast:'is_broadcast',isJidGroup:'is_group',isJidStatusBroadcast:'is_status_broadcast',isJidNewsletter:'is_newsletter',isJidBot:'is_bot',jidNormalizedUser:'normalized_user',
  isPnUser:'is_pn_user',isLidUser:'is_lid_user',isHostedPnUser:'is_hosted_pn_user',isHostedLidUser:'is_hosted_lid_user',transferDevice:'transfer_device',encodeBinaryNode:'encode',decodeBinaryNode:'decode',decodeDecompressedBinaryNode:'decode'}
// Exports with no counterpart on purpose, by reason.
const skipped={
  'language or runtime facility':['isStringNullOrEmpty','toNumber','unixTimestampSeconds','debouncedTimeout','delay','delayCancellable','promiseTimeout','trimUndefined','encodeBigEndian','toReadable','toBuffer','getStream','hasNonNullishProperty','makeMutex','makeKeyedMutex'],
  'served by the typed event stream':['bindWaitForEvent','bindWaitForConnectionUpdate'],
  'inlined or folded into its caller':['mediaMessageSHA256B64','decompressingIfRequired','generateRegistrationId','generateMdTagPrefix','getStatusFromReceiptType','getErrorCodeFromStreamError','getCodeFromWSError','getDecryptionJid','NO_MESSAGE_FOUND_ERROR_TEXT','MISSING_KEYS_ERROR_TEXT','DECRYPTION_RETRY_CONFIG','NACK_REASONS','SERVER_ERROR_CODES','createSignalIdentity','xmppSignedPreKey','xmppPreKey','getHistoryMsg','ensureLTHashStateVersion','isAppStateSyncIrrecoverable','LT_HASH_ANTI_TAMPERING','getPlatformId','getCompanionWebClientType','getServerFromDomainType']}
const reasons=Object.fromEntries(Object.entries(skipped).flatMap(([reason,names])=>names.map(name=>[name,reason])))
const files=[...readFileSync(join(upstream,'src/Utils/index.ts'),'utf8').matchAll(/'\.\/([^']+)'/g)].map(m=>'Utils/'+m[1]+'.ts').concat(['WABinary/generic-utils.ts','WABinary/jid-utils.ts','WABinary/encode.ts','WABinary/decode.ts'])
const report=[]
for(const file of files){
  const source=readFileSync(join(upstream,'src',file),'utf8')
  for(const [,name] of source.matchAll(/^export (?:const|function\*?|async function\*?|class) ([A-Za-z0-9_]+)/gm)){
    if(name in reasons){report.push({file,name,status:reasons[name]});continue}
    const target=[renamed[name],snake(name),name,name.toUpperCase()].find(candidate=>candidate&&idents.has(candidate))
    report.push({file,name,target,status:target?'covered':'MISSING'})
  }
}
const missing=report.filter(r=>r.status==='MISSING'),covered=report.filter(r=>r.status==='covered')
for(const r of report)console.log(`${r.status.padEnd(32)} ${r.file.padEnd(40)} ${r.name}${r.target&&r.target!==snake(r.name)?' -> '+r.target:''}`)
console.log(`\n${covered.length} covered, ${missing.length} missing, ${report.length-covered.length-missing.length} without a counterpart on purpose`)
process.exit(missing.length?1:0)
