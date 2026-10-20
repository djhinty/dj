/* DJ Private 1:1: WebRTC calling, text and media over a P2P data channel.
   Cloudflare Durable Object only forwards temporary SDP/ICE signaling;
   no database, Firebase, file storage, or message persistence is used. */
const $ = id => document.getElementById(id);
const nodes = Object.fromEntries([
  'welcomeView','chatView','entryForm','nameInput','roomInput','setupNotice','createBtn','joinBtn',
  'sidebar','sidebarScrim','menuBtn','closeSidebar','memberCount','memberList','sidebarRoomName',
  'roomTitle','headerMembers','meAvatar','meName','roomCodeDisplay','messageList','conversationContent',
  'messageInput','sendBtn','attachBtn','fileInput','uploadStatus','uploadLabel','uploadProgress',
  'inviteSidebarBtn','copyInviteBtn','leaveBtn','voiceBtn','videoBtn','peopleModal','closePeopleModal',
  'callPeopleList','peopleHelp','incomingModal','incomingTitle','incomingSubtitle','incomingAvatar',
  'acceptBtn','declineBtn','callOverlay','callStateText','audioPlaceholder','audioAvatar','audioPeerName',
  'localVideo','remoteVideo','remoteAudio','localPlaceholder','localPreview','micToggle','cameraToggle',
  'hangupBtn','callFooterText','toastArea'
].map(id=>[id,$(id)]));
const config = window.DJ_CONFIG || {};
const configured = /^(https:\/\/|http:\/\/localhost(?::\d+)?\/)/i.test((config.signalingUrl || '') + '/') && !/YOUR_|REPLACE_|example/i.test(config.signalingUrl);
const iceServers = Array.isArray(config.iceServers) && config.iceServers.length
  ? config.iceServers : [{urls:['stun:stun.l.google.com:19302','stun:stun1.l.google.com:19302']}];
const MAX_IMAGE = 15*1024*1024;
const MAX_VIDEO = 50*1024*1024;
const CHUNK_BYTES = 16*1024;
let socket=null, roomId='', myId='', myName='', peer=null;
let rtc=null, channel=null, remoteStream=null, localStream=null, call=null, pendingCall=null;
let busy=false, transferBusy=false, outgoing=false, ringTimer=null, heartbeat=null;
let makingOffer=false, ignoreOffer=false, isSettingRemoteAnswerPending=false, pendingIce=[];
let incomingTransfer=null, messageHistory=[], blobUrls=new Set(), sessionId=0, transferEpoch=0;
const colors=[['#d9d3ff','#564b9f'],['#c9e9d7','#326c57'],['#f8d4c8','#875843'],['#cde4fd','#3f658e'],['#ffe7b9','#956f2f']];
function toast(message,error=false){const el=document.createElement('div');el.className=`toast${error?' error':''}`;el.textContent=message;nodes.toastArea.append(el);setTimeout(()=>el.remove(),5000);}
function letter(s){return (String(s||'?').trim().charAt(0)||'?').toUpperCase();}
function avatar(name,cls='member-avatar'){const el=document.createElement('span');el.className=cls;let h=0;for(const c of String(name))h=(h*31+c.charCodeAt())|0;const [bg,fg]=colors[Math.abs(h)%colors.length];el.style.background=bg;el.style.color=fg;el.textContent=letter(name);return el;}
function codeFrom(value){const s=String(value||'').trim();try{const url=new URL(s);return (url.searchParams.get('room')||'').toUpperCase();}catch{return s.replace(/^#?room=/i,'').replace(/\s/g,'').toUpperCase();}}
function randomCode(){const alphabet='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';const b=crypto.getRandomValues(new Uint8Array(20));return Array.from(b,x=>alphabet[x%alphabet.length]).join('');}
function inviteUrl(){const u=new URL(location.href);u.searchParams.set('room',roomId);u.hash='';return u.toString();}
async function copyInvite(){try{await navigator.clipboard.writeText(inviteUrl());toast('Invite link copied!');}catch{toast('Copy manually from the address bar.',true);}}
function roomUrl(code){const base=config.signalingUrl.replace(/\/+$/,'').replace(/^https:/,'wss:').replace(/^http:/,'ws:');return `${base}/room/${encodeURIComponent(code)}`;}
function setEntryBusy(value){nodes.createBtn.disabled=value;nodes.joinBtn.disabled=value;}
function connected(){return !!(channel&&channel.readyState==='open');}
function setReady(){const ready=connected();nodes.sendBtn.disabled=!ready;nodes.attachBtn.disabled=!ready;nodes.voiceBtn.disabled=!ready;nodes.videoBtn.disabled=!ready;
 nodes.messageInput.disabled=!ready;nodes.messageInput.placeholder=ready?'Write a message...':'Waiting for your friend to connect...';
 nodes.headerMembers.textContent=ready?'2 connected · private':peer?'Connecting to friend…':'Waiting for a friend…';}
function cleanText(s,limit){return String(s||'').replace(/[\u0000-\u001F\u007F]/g,'').trim().slice(0,limit);}
function enterRoom(code,name){if(!configured){nodes.setupNotice.hidden=false;toast('Configure your free Cloudflare signaling Worker in config.js first.',true);return;}
 if(!/^[-A-Z2-9]{20}$/.test(code)||/[IO01]/.test(code)){toast('Enter a valid 20-character DJ room code or invitation link.',true);return;}
 if(!name||name.length>40){toast('Enter a display name (up to 40 characters).',true);return;}
 const thisSession=++sessionId;setEntryBusy(true);myName=name;roomId=code;
 nodes.roomTitle.textContent='Private room';nodes.sidebarRoomName.textContent='Private room';nodes.roomCodeDisplay.textContent=`ROOM · ${code.slice(0,6)}…`;
 nodes.meName.textContent=name;nodes.meAvatar.textContent=letter(name);
 nodes.welcomeView.classList.add('hidden');nodes.chatView.classList.remove('hidden');history.replaceState({},'',`${location.pathname}?room=${encodeURIComponent(code)}`);
 setReady();renderMembers();
 try{socket=new WebSocket(roomUrl(code));}
 catch(e){toast('Invalid signaling server address.',true);leaveRoom();return;}
 const ws=socket;
 ws.onopen=()=>{if(thisSession!==sessionId)return;ws.send(JSON.stringify({type:'hello',name}));heartbeat=setInterval(()=>{if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify({type:'ping'}));},25000);};
 ws.onmessage=evt=>{if(thisSession!==sessionId)return;try{const msg=JSON.parse(evt.data);handleSignal(msg);}catch(e){console.warn('Signal handling:',e);}};
 ws.onerror=()=>{if(thisSession===sessionId)toast('Unable to reach DJ signaling server. Check its URL and deployment.',true);};
 ws.onclose=evt=>{if(thisSession!==sessionId)return;clearInterval(heartbeat);heartbeat=null;
  if(evt.code===1008)toast('This room is full or access was denied. DJ supports two people per room.',true);
  else toast('Signaling connection closed. Rejoin the room to reconnect.',true);
  leaveRoom(false);};
 setEntryBusy(false);
}
function createRoom(){const name=nodes.nameInput.value.trim().replace(/\s+/g,' ');enterRoom(randomCode(),name);}
function joinRoom(){const name=nodes.nameInput.value.trim().replace(/\s+/g,' ');enterRoom(codeFrom(nodes.roomInput.value),name);}
function sendSignal(data){if(socket?.readyState!==WebSocket.OPEN)return;socket.send(JSON.stringify({type:'signal',...data}));}
async function handleSignal(msg){
 if(msg.type==='welcome'){
  myId=msg.id;const found=(msg.peers||[])[0];if(found){peer=found;renderMembers();ensurePeer(false);}
  else {toast('Room ready. Share the invite link with one friend.');renderMembers();}
 }else if(msg.type==='peer-joined'){
  if(msg.peer?.id===myId)return;peer=msg.peer;renderMembers();ensurePeer(true);toast(`${peer.name} joined the room.`);
 }else if(msg.type==='peer-left'){
  toast('Your friend left. Messages and shared media were cleared.');disconnectPeer();
 }else if(msg.type==='signal'){
  if(msg.from!==peer?.id)return;
  try{await applySignal(msg);}catch(e){console.warn(e);toast('Connection setup failed; try rejoining the room.',true);}
 }else if(msg.type==='error'){toast(String(msg.message||'Connection rejected.'),true);}
}
function ensurePeer(initiate){if(rtc||!peer)return;
 rtc=new RTCPeerConnection({iceServers});const pc=rtc;
 remoteStream=new MediaStream();makingOffer=false;ignoreOffer=false;isSettingRemoteAnswerPending=false;pendingIce=[];
 pc.onicecandidate=e=>{if(e.candidate)sendSignal({candidate:e.candidate.toJSON()});};
 pc.ontrack=e=>{
  const tracks=e.streams[0]?.getTracks()||[e.track];for(const tr of tracks){if(!remoteStream.getTrackById(tr.id))remoteStream.addTrack(tr);}
  nodes.remoteVideo.srcObject=remoteStream;nodes.remoteAudio.srcObject=remoteStream;
  if(call){if(call.type==='video'){nodes.remoteVideo.classList.add('video-active');nodes.audioPlaceholder.classList.add('hidden');nodes.remoteVideo.play().catch(()=>{});}else nodes.remoteAudio.play().catch(()=>{});}
 };
 pc.ondatachannel=e=>attachChannel(e.channel);
 pc.onconnectionstatechange=()=>{if(pc!==rtc)return;
  if(pc.connectionState==='failed'){toast('Peer connection failed. A TURN server may be needed on your network.',true);disconnectPeer();}
  if(pc.connectionState==='disconnected'){nodes.headerMembers.textContent='Connection interrupted…';}
 };
 pc.onnegotiationneeded=async()=>{try{makingOffer=true;await pc.setLocalDescription();sendSignal({description:{type:pc.localDescription.type,sdp:pc.localDescription.sdp}});}catch(e){console.warn('Negotiation:',e);}finally{makingOffer=false;}};
 if(initiate)attachChannel(pc.createDataChannel('dj-private',{ordered:true}));
 setReady();
}
async function applySignal(msg){const pc=rtc;if(!pc)return;
 if(msg.description){const description=msg.description;
  const readyForOffer=!makingOffer&&(pc.signalingState==='stable'||isSettingRemoteAnswerPending);
  const offerCollision=description.type==='offer'&&!readyForOffer;
  // Existing occupant is impolite, new joiner is polite (WebRTC perfect negotiation).
  const polite=(myId>peer.id);
  ignoreOffer=!polite&&offerCollision;if(ignoreOffer)return;
  isSettingRemoteAnswerPending=description.type==='answer';
  try{await pc.setRemoteDescription(description);}finally{isSettingRemoteAnswerPending=false;}
  for(const candidate of pendingIce.splice(0)){try{await pc.addIceCandidate(candidate);}catch(e){console.warn('Queued candidate:',e);}}
  if(description.type==='offer'){await pc.setLocalDescription();sendSignal({description:{type:pc.localDescription.type,sdp:pc.localDescription.sdp}});}
 }else if(msg.candidate){if(!pc.remoteDescription){pendingIce.push(msg.candidate);return;}try{await pc.addIceCandidate(msg.candidate);}catch(e){if(!ignoreOffer)throw e;}}
}
function attachChannel(dc){channel=dc;dc.binaryType='arraybuffer';dc.bufferedAmountLowThreshold=64*1024;
 dc.onopen=()=>{setReady();toast('Private connection ready. You can chat, share files or make a call.');};
 dc.onclose=()=>{if(channel!==dc)return;setReady();};
 dc.onmessage=e=>{receiveData(e.data).catch(err=>{console.warn(err);toast('Unable to receive shared media.',true);incomingTransfer=null;});};
}
function sendData(message){if(!connected())throw Error('Your friend is not connected yet.');channel.send(JSON.stringify(message));}
function clearHistory(){transferEpoch++;messageHistory=[];nodes.messageList.replaceChildren();for(const url of blobUrls)URL.revokeObjectURL(url);blobUrls.clear();renderMessages();incomingTransfer=null;}
function disconnectPeer(){finishCall(false);if(rtc){rtc.ontrack=null;rtc.ondatachannel=null;rtc.onnegotiationneeded=null;rtc.onicecandidate=null;rtc.close();rtc=null;}
 channel=null;remoteStream=null;peer=null;pendingIce=[];incomingTransfer=null;clearHistory();renderMembers();setReady();}
function leaveRoom(closeSocket=true){sessionId++;clearInterval(heartbeat);heartbeat=null;closePeopleModal();nodes.incomingModal.classList.add('hidden');pendingCall=null;disconnectPeer();
 if(closeSocket&&socket&&socket.readyState<=WebSocket.OPEN){try{socket.close(1000,'leaving');}catch{}}
 socket=null;roomId='';myId='';myName='';history.replaceState({},'',location.pathname);nodes.chatView.classList.add('hidden');nodes.welcomeView.classList.remove('hidden');nodes.roomInput.value='';nodes.messageInput.value='';hideSidebar();setEntryBusy(false);}
function renderMembers(){nodes.memberList.replaceChildren();const list=[...(myName?[{name:myName,me:true}]:[]),...(peer?[{name:peer.name,me:false}]:[])];
 nodes.memberCount.textContent=String(list.length);for(const member of list){const row=document.createElement('div');row.className='member-row';row.append(avatar(member.name));const info=document.createElement('div');info.className='member-info';const title=document.createElement('strong');title.textContent=member.name;const sub=document.createElement('span');sub.textContent=member.me?'You':'Live peer';info.append(title,sub);row.append(info);nodes.memberList.append(row);}}
function addMessage(item){messageHistory.push({...item,when:new Date()});renderMessages();}
function renderMessages(){const wrap=nodes.messageList,scroll=nodes.conversationContent;const near=scroll.scrollHeight-scroll.scrollTop-scroll.clientHeight<170;wrap.replaceChildren();
 if(!messageHistory.length){const empty=document.createElement('div');empty.className='empty-state';const art=document.createElement('div');art.className='empty-illustration';art.textContent='✦';const title=document.createElement('h2');title.textContent='Private by design.';const desc=document.createElement('p');desc.textContent='Messages and media stay on your devices, not in a cloud database.';empty.append(art,title,desc);wrap.append(empty);return;}
 for(const item of messageHistory){const mine=item.mine;const row=document.createElement('div');row.className=`message-row ${mine?'mine':'theirs'}`;row.append(avatar(mine?myName:peer?.name||'Friend','message-avatar'));
 const main=document.createElement('div');main.className='message-main';const meta=document.createElement('div');meta.className='message-meta';const author=document.createElement('strong');author.textContent=mine?'You':item.sender||peer?.name||'Friend';const when=document.createElement('span');when.textContent=item.when.toLocaleTimeString([],{hour:'numeric',minute:'2-digit'});meta.append(author,when);main.append(meta);
 const bubble=document.createElement('div');bubble.className='message-bubble';if(item.kind==='image'&&item.url){bubble.classList.add('media-bubble');const pic=document.createElement('img');pic.src=item.url;pic.alt=item.name||'Shared image';pic.loading='lazy';bubble.append(pic);}
 else if(item.kind==='video'&&item.url){bubble.classList.add('media-bubble');const vid=document.createElement('video');vid.src=item.url;vid.controls=true;vid.playsInline=true;vid.preload='metadata';bubble.append(vid);}else bubble.textContent=item.text||'[Media unavailable]';
 if(item.url&&item.name){const caption=document.createElement('div');caption.className='media-caption';caption.textContent=item.name;bubble.append(caption);}
 main.append(bubble);row.append(main);wrap.append(row);}
 if(near)requestAnimationFrame(()=>scroll.scrollTop=scroll.scrollHeight);
}
function sendMessage(){const text=nodes.messageInput.value.trim();if(!text||!connected())return;
 try{sendData({type:'text',text:text.slice(0,4000)});addMessage({mine:true,kind:'text',text});nodes.messageInput.value='';autoResize();}catch(e){toast(e.message,true);}}
function autoResize(){nodes.messageInput.style.height='auto';nodes.messageInput.style.height=`${Math.min(nodes.messageInput.scrollHeight,150)}px`;}
async function sendFiles(files){if(transferBusy){toast('Finish the current transfer before sending another file.');return;}
 for(const file of files){if(!connected())break;const isImage=file.type.startsWith('image/'),isVideo=file.type.startsWith('video/');if(!isImage&&!isVideo){toast('Only images and videos are supported.',true);continue;}
 const max=isImage?MAX_IMAGE:MAX_VIDEO;if(!file.size||file.size>max){toast(`${file.name}: maximum ${isImage?'15':'50'} MB.`,true);continue;}
 const name=cleanText(file.name,140)||'Shared media';const id=crypto.randomUUID();const epoch=transferEpoch;transferBusy=true;nodes.uploadStatus.classList.remove('hidden');nodes.uploadProgress.style.width='0%';nodes.uploadLabel.textContent=`Sending ${name} directly…`;
 try{sendData({type:'file-start',id,name,mime:file.type,kind:isImage?'image':'video',size:file.size});
  // Chunks are deliberately small; backpressure prevents excessive buffering.
  for(let off=0;off<file.size;off+=CHUNK_BYTES){if(epoch!==transferEpoch)throw Error('Session ended.');if(!connected())throw Error('The peer disconnected.');
   await awaitBufferSpace(channel);const data=await file.slice(off,off+CHUNK_BYTES).arrayBuffer();channel.send(data);
   const pct=Math.floor(100*Math.min(1,(off+data.byteLength)/file.size));nodes.uploadProgress.style.width=pct+'%';nodes.uploadLabel.textContent=`Sending ${name} · ${pct}%`;
  }
  if(epoch!==transferEpoch)throw Error('Session ended.');sendData({type:'file-end',id});const url=URL.createObjectURL(file);blobUrls.add(url);addMessage({mine:true,kind:isImage?'image':'video',url,name});toast('Sent directly to your friend — not uploaded.');
 }catch(e){toast('Transfer stopped: '+e.message,true);if(connected())sendData({type:'file-cancel',id});}
 finally{transferBusy=false;nodes.uploadStatus.classList.add('hidden');nodes.uploadProgress.style.width='0%';}
 }
}
function awaitBufferSpace(dc){if(dc.bufferedAmount<512*1024)return Promise.resolve();
 return new Promise((resolve,reject)=>{let timer;const cleanup=()=>{dc.removeEventListener('bufferedamountlow',resume);dc.removeEventListener('close',fail);clearTimeout(timer);};
 const resume=()=>{cleanup();resolve();},fail=()=>{cleanup();reject(Error('Connection closed'));};dc.addEventListener('bufferedamountlow',resume,{once:true});dc.addEventListener('close',fail,{once:true});timer=setTimeout(()=>{cleanup();reject(Error('Transfer timed out'));},30000);});}
async function receiveData(value){if(typeof value!=='string'){
  if(!incomingTransfer)return;const part=value instanceof ArrayBuffer?value:await value.arrayBuffer();incomingTransfer.chunks.push(part);incomingTransfer.received+=part.byteLength;
  if(incomingTransfer.received>incomingTransfer.size)throw Error('Received more data than expected.');return;
 }
 let msg;try{msg=JSON.parse(value);}catch{return;}
 if(msg.type==='text'){if(typeof msg.text!=='string')return;addMessage({mine:false,kind:'text',text:msg.text.slice(0,4000)});}
 else if(msg.type==='file-start'){
  if(incomingTransfer)return;const valid=msg.kind==='image'?msg.mime?.startsWith('image/'):msg.kind==='video'&&msg.mime?.startsWith('video/');
  const max=msg.kind==='image'?MAX_IMAGE:MAX_VIDEO;
  if(!valid||!Number.isSafeInteger(msg.size)||msg.size<=0||msg.size>max)return;
  incomingTransfer={id:msg.id,name:cleanText(msg.name,140)||'Media',mime:msg.mime,kind:msg.kind,size:msg.size,chunks:[],received:0};
 }
 else if(msg.type==='file-end'){
  const tr=incomingTransfer;incomingTransfer=null;if(!tr||tr.id!==msg.id||tr.received!==tr.size){toast('Incomplete media received; discarded.',true);return;}
  const blob=new Blob(tr.chunks,{type:tr.mime});const url=URL.createObjectURL(blob);blobUrls.add(url);addMessage({mine:false,kind:tr.kind,url,name:tr.name});toast(`${tr.kind==='video'?'Video':'Image'} received privately.`);
 }
 else if(msg.type==='file-cancel'){incomingTransfer=null;}
 else if(msg.type==='call-request'){onCallRequest(msg);}
 else if(msg.type==='call-accept'){await onCallAccepted(msg);}
 else if(msg.type==='call-decline'){toast('Call declined.');finishCall(false);}
 else if(msg.type==='call-end'){toast('Call ended. Session messages and media cleared.');finishCall(true);}
}
function onCallRequest(msg){if(!connected()||call||pendingCall||!['audio','video'].includes(msg.callType))return;
 pendingCall={type:msg.callType};nodes.incomingTitle.textContent=`${peer?.name||'A friend'} is calling`;nodes.incomingSubtitle.textContent=msg.callType==='video'?'Incoming video call':'Incoming voice call';nodes.incomingAvatar.textContent=letter(peer?.name);nodes.incomingModal.classList.remove('hidden');}
function showCallUI(type){nodes.callOverlay.classList.remove('hidden');nodes.audioPlaceholder.classList.remove('hidden');nodes.remoteVideo.classList.remove('video-active');nodes.cameraToggle.classList.toggle('hidden',type!=='video');nodes.localPreview.classList.toggle('hidden',type!=='video');nodes.localPlaceholder.classList.add('hidden');nodes.micToggle.classList.remove('off');nodes.cameraToggle.classList.remove('off');nodes.audioAvatar.textContent=letter(peer?.name);nodes.audioPeerName.textContent=peer?.name||'Friend';nodes.callStateText.textContent=`Connecting to ${peer?.name||'friend'}…`;}
async function getMedia(type){if(!navigator.mediaDevices?.getUserMedia)throw Error('Camera/microphone requires HTTPS or localhost.');return navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true},video:type==='video'?{facingMode:'user',width:{ideal:960},height:{ideal:720}}:false});}
function attachLocalTracks(){if(!rtc||!localStream)return;for(const track of localStream.getTracks())rtc.addTrack(track,localStream);
 nodes.localVideo.srcObject=localStream;if(call?.type==='video')nodes.localVideo.play().catch(()=>{});}
async function startCall(type){if(!connected()||call||pendingCall||busy)return;busy=true;
 try{const stream=await getMedia(type);localStream=stream;call={type,initiated:true};showCallUI(type);nodes.callStateText.textContent=`Calling ${peer.name}…`;sendData({type:'call-request',callType:type});
 ringTimer=setTimeout(()=>{if(call?.initiated){toast('No answer.');try{sendData({type:'call-decline'});}catch{}finishCall(false);}},60000);
 }catch(e){toast(`Cannot start call: ${e.message}`,true);finishCall(false);}finally{busy=false;}}
async function acceptCall(){if(!pendingCall||busy||call)return;busy=true;const type=pendingCall.type;pendingCall=null;nodes.incomingModal.classList.add('hidden');
 try{localStream=await getMedia(type);call={type,initiated:false};showCallUI(type);attachLocalTracks();sendData({type:'call-accept'});nodes.callStateText.textContent='Connecting…';
 }catch(e){toast(`Could not answer: ${e.message}`,true);try{sendData({type:'call-decline'});}catch{}finishCall(false);}finally{busy=false;}}
async function onCallAccepted(){if(!call?.initiated)return;clearTimeout(ringTimer);ringTimer=null;attachLocalTracks();nodes.callStateText.textContent='Connecting…';}
function declineCall(){pendingCall=null;nodes.incomingModal.classList.add('hidden');try{sendData({type:'call-decline'});}catch{}}
function finishCall(clearData){clearTimeout(ringTimer);ringTimer=null;pendingCall=null;call=null;nodes.incomingModal.classList.add('hidden');nodes.callOverlay.classList.add('hidden');
 if(localStream){localStream.getTracks().forEach(t=>t.stop());localStream=null;}
 if(rtc){for(const sender of rtc.getSenders()){if(sender.track){try{rtc.removeTrack(sender);}catch{}}}}
 nodes.localVideo.srcObject=null;nodes.remoteVideo.srcObject=null;nodes.remoteAudio.srcObject=null;
 if(remoteStream){remoteStream.getTracks().forEach(t=>t.stop());remoteStream=new MediaStream();}
 if(clearData)clearHistory();}
function endCall(){if(!call&&!pendingCall)return;try{sendData({type:'call-end'});}catch{}finishCall(true);toast('Call ended. Temporary conversation cleared.');}
function toggleMic(){const tracks=localStream?.getAudioTracks()||[];if(!tracks.length)return;const on=!tracks[0].enabled;tracks.forEach(t=>t.enabled=on);nodes.micToggle.classList.toggle('off',!on);nodes.micToggle.title=on?'Mute microphone':'Unmute microphone';}
function toggleCamera(){const tracks=localStream?.getVideoTracks()||[];if(!tracks.length)return;const on=!tracks[0].enabled;tracks.forEach(t=>t.enabled=on);nodes.cameraToggle.classList.toggle('off',!on);nodes.localPlaceholder.classList.toggle('hidden',on);nodes.cameraToggle.title=on?'Turn camera off':'Turn camera on';}
function showPeopleModal(type){if(!connected()){toast('Invite a friend and connect before calling.',true);return;}
 if(call){toast('End your current call first.',true);return;}nodes.peopleHelp.textContent=`Start a ${type==='video'?'video':'voice'} call with your connected friend.`;nodes.callPeopleList.replaceChildren();
 const person=document.createElement('div');person.className='call-person';person.append(avatar(peer.name));const name=document.createElement('strong');name.textContent=peer.name;person.append(name);const b=document.createElement('button');b.type='button';b.textContent=type==='video'?'Start video ↗':'Start voice ↗';b.addEventListener('click',()=>{closePeopleModal();startCall(type);});person.append(b);nodes.callPeopleList.append(person);nodes.peopleModal.classList.remove('hidden');}
function closePeopleModal(){nodes.peopleModal.classList.add('hidden');}
function showSidebar(){nodes.sidebar.classList.add('open');nodes.sidebarScrim.classList.remove('hidden');}
function hideSidebar(){nodes.sidebar.classList.remove('open');nodes.sidebarScrim.classList.add('hidden');}
function registerEvents(){nodes.createBtn.addEventListener('click',createRoom);nodes.joinBtn.addEventListener('click',joinRoom);
 nodes.entryForm.addEventListener('submit',e=>{e.preventDefault();nodes.roomInput.value.trim()?joinRoom():createRoom();});
 nodes.sendBtn.addEventListener('click',sendMessage);nodes.messageInput.addEventListener('keydown',e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendMessage();}});nodes.messageInput.addEventListener('input',autoResize);
 nodes.attachBtn.addEventListener('click',()=>nodes.fileInput.click());nodes.fileInput.addEventListener('change',e=>{const files=[...e.target.files];e.target.value='';sendFiles(files);});
 nodes.inviteSidebarBtn.addEventListener('click',copyInvite);nodes.copyInviteBtn.addEventListener('click',copyInvite);nodes.leaveBtn.addEventListener('click',()=>leaveRoom());
 nodes.menuBtn.addEventListener('click',showSidebar);nodes.closeSidebar.addEventListener('click',hideSidebar);nodes.sidebarScrim.addEventListener('click',hideSidebar);
 nodes.voiceBtn.addEventListener('click',()=>showPeopleModal('audio'));nodes.videoBtn.addEventListener('click',()=>showPeopleModal('video'));
 nodes.closePeopleModal.addEventListener('click',closePeopleModal);nodes.peopleModal.addEventListener('click',e=>{if(e.target===nodes.peopleModal)closePeopleModal();});
 nodes.acceptBtn.addEventListener('click',acceptCall);nodes.declineBtn.addEventListener('click',declineCall);nodes.hangupBtn.addEventListener('click',endCall);nodes.micToggle.addEventListener('click',toggleMic);nodes.cameraToggle.addEventListener('click',toggleCamera);
 nodes.callOverlay.addEventListener('click',()=>{if(call?.type==='video')nodes.remoteVideo.play().catch(()=>{});else nodes.remoteAudio.play().catch(()=>{});});
 document.addEventListener('keydown',e=>{if(e.key==='Escape'){closePeopleModal();hideSidebar();}});
 window.addEventListener('pagehide',()=>{localStream?.getTracks().forEach(t=>t.stop());try{socket?.close();}catch{}clearHistory();});
}
registerEvents();setReady();const urlRoom=new URLSearchParams(location.search).get('room');if(urlRoom)nodes.roomInput.value=urlRoom;if(!configured)nodes.setupNotice.hidden=false;
