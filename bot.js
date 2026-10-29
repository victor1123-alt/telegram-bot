// channel-watch.js
// Logs activity across every channel, supergroup and group your account is in,
// and optionally DMs everyone who joins.
// ESM ("type": "module"), Node 18+

import { TelegramClient, Api } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { Raw, NewMessage } from "telegram/events/index.js";
import input from "input";
import fs from "node:fs";

// ---------- CONFIG ----------
const apiId = Number(process.env.TG_API_ID ?? 37239194);
const apiHash = process.env.TG_API_HASH ?? "af9b785e54836ba46c6fb68ae4f41d13";
const SESSION_FILE = "./session.txt";

const LOG_TO = "me";
const LOG_MESSAGES = true;
const LOG_JOINS = true;
const LOG_LEAVES = true;
const LOG_OWN_MESSAGES = true;
const SKIP_BOTS = true;

const MAX_TEXT = 500;
const QUEUE_DELAY_MS = 1000;
const REFRESH_MS = 5 * 60_000;
const DEDUPE_TTL_MS = 60_000;
const MSG_DEDUPE_TTL_MS = 10 * 60_000;
const PUSH_TTL_MS = 30 * 60_000;

const DEBUG_PARTICIPANTS = true;

const ENABLE_POLLING = true;
const POLL_MS = 90_000;
const POLL_CHAT_DELAY_MS = 1500;
const POLL_PAGE_DELAY_MS = 1200;
const POLL_MAX_PAGES = 25;

const AUTO_CONVERT_BROADCAST = false;

// ---------- JOIN DM CONFIG ----------
const DM_ON_JOIN = true;
const DM_TEMPLATE =
  "Hey {name} — saw you just joined {chat}. Welcome.\n\n" +
  "I watch that channel closely, so if you want to talk about anything posted there, just reply here.";
const DM_DELAY_MS = 4000;          // gap between DMs; do not drop below ~3s
const DM_DAILY_CAP = 40;           // raise slowly on an aged account
const DM_MAX_QUEUE = 300;          // drop the rest if backfill explodes
const DM_ONCE_PER_USER = true;     // never DM the same account twice, ever
const DM_SKIP_BOTS = true;
const DM_ONLY_IN = [];             // cids or "@username"; empty = every chat
const DM_EXCLUDE = [];             // cids or "@username"
const DM_LOG_FAILURES = true;
const DM_DRY_RUN = false;          // true = resolve + render, never send

// ---------- STATE ----------
let selfId = null;
let logPeer = null;
let logPeerId = null;
const chats = new Map();
const recent = new Map();
const snapshots = new Map();
const pollState = new Map();
let refreshPromise = null;
let shuttingDown = false;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const sessionString = fs.existsSync(SESSION_FILE)
  ? fs.readFileSync(SESSION_FILE, "utf8").trim()
  : "";

const client = new TelegramClient(new StringSession(sessionString), apiId, apiHash, {
  connectionRetries: 5,
  autoReconnect: true,
  useWSS: true,
});

// ---------- SEND QUEUE ----------
const queue = [];
let draining = false;

function floodSeconds(e) {
  const m = /FLOOD_WAIT_(\d+)/.exec(e?.errorMessage ?? e?.message ?? "");
  if (m) return Number(m[1]);
  if (typeof e?.seconds === "number") return e.seconds;
  return 0;
}

function enqueue(fn) {
  queue.push({ fn, tries: 0 });
  if (draining) return;
  draining = true;
  void (async () => {
    while (queue.length) {
      const job = queue.shift();
      try {
        await job.fn();
      } catch (e) {
        const wait = floodSeconds(e);
        if (wait && job.tries < 5) {
          job.tries++;
          queue.unshift(job);
          console.error(`[queue] flood ${wait}s, requeueing`);
          await sleep(wait * 1000 + 750);
          continue;
        }
        console.error("queue error:", e.message);
      }
      await sleep(QUEUE_DELAY_MS);
    }
    draining = false;
  })();
}

// ---------- DM STATE ----------
const DM_STATE_FILE = "./dm-state.json";
const dmSent = new Map();          // uid -> first-DM timestamp
let dmDate = "";
let dmCount = 0;

function loadDmState() {
  try {
    const raw = JSON.parse(fs.readFileSync(DM_STATE_FILE, "utf8"));
    dmDate = raw.date ?? "";
    dmCount = Number(raw.count ?? 0);
    const cutoff = Date.now() - 90 * 24 * 3600_000;
    for (const [uid, ts] of Object.entries(raw.sent ?? {})) {
      if (Number(ts) > cutoff) dmSent.set(Number(uid), Number(ts));
    }
    console.log(`[dm] restored ${dmSent.size} contacts, ${dmCount} sent today`);
  } catch {}
}

function saveDmState() {
  try {
    const sent = {};
    for (const [uid, ts] of dmSent) sent[uid] = ts;
    fs.writeFileSync(DM_STATE_FILE, JSON.stringify({ date: dmDate, count: dmCount, sent }));
  } catch (e) {
    console.error("[dm] state save failed:", e.message);
  }
}

function dmCapLeft() {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== dmDate) { dmDate = today; dmCount = 0; }
  return DM_DAILY_CAP - dmCount;
}

// ---------- SNAPSHOT PERSISTENCE ----------
const SNAP_FILE = "./snapshots.json";

function loadSnapshots() {
  try {
    const raw = JSON.parse(fs.readFileSync(SNAP_FILE, "utf8"));
    for (const [cid, arr] of Object.entries(raw)) snapshots.set(Number(cid), new Set(arr));
    console.log(`[poll] restored ${snapshots.size} snapshots`);
  } catch {}
}

function saveSnapshots() {
  try {
    const obj = {};
    for (const cid of chats.keys()) {
      const set = snapshots.get(cid);
      if (set) obj[cid] = [...set];
    }
    fs.writeFileSync(SNAP_FILE, JSON.stringify(obj));
  } catch (e) {
    console.error("[poll] snapshot save failed:", e.message);
  }
}

// ---------- HELPERS ----------
function trim(s, n = MAX_TEXT) {
  s = (s ?? "").toString().replace(/\s+/g, " ").trim();
  return s.length > n ? s.slice(0, n) + "…" : s;
}

function kindOf(peer) {
  const c = peer?.className;
  if (c === "PeerChannel") return "channel";
  if (c === "PeerChat") return "group";
  return null;
}

function idOf(peer) {
  const c = peer?.className;
  if (c === "PeerChannel") return -1000000000000 - Number(peer.channelId);
  if (c === "PeerChat") return -Number(peer.chatId);
  return null;
}

function metaOf(cid, fallbackKind = "channel") {
  return chats.get(cid) ?? { title: `chat ${cid}`, username: null, kind: fallbackKind };
}

function linkFor(peer, msgId) {
  const meta = chats.get(idOf(peer));
  if (meta?.username) return `https://t.me/${meta.username}/${msgId}`;
  if (peer?.className === "PeerChannel") return `https://t.me/c/${peer.channelId}/${msgId}`;
  return null;
}

function once(key, ttl = DEDUPE_TTL_MS) {
  const now = Date.now();
  for (const [k, exp] of recent) if (exp < now) recent.delete(k);
  const exp = recent.get(key);
  if (exp && exp > now) return false;
  recent.set(key, now + ttl);
  return true;
}

async function nameOf(uid) {
  if (uid == null) return "unknown";
  try {
    const u = await client.getEntity(uid);
    return u.firstName || u.title || u.username || `id ${uid}`;
  } catch {
    return `id ${uid}`;
  }
}

function describe(msg) {
  if (msg.text) return trim(msg.text);
  if (msg.photo) return "[photo]";
  if (msg.video) return "[video]";
  if (msg.voice) return "[voice note]";
  if (msg.audio) return "[audio]";
  if (msg.sticker) return "[sticker]";
  if (msg.geo) return "[location]";
  if (msg.document) {
    const name = msg.document.attributes?.find((a) => a.fileName)?.fileName;
    return `[file${name ? ` ${name}` : ""}]`;
  }
  return `[${msg.className}]`;
}

// ---------- DM ENGINE ----------
const dmQueue = [];
let dmDraining = false;

function dmAllowedChat(cid, meta) {
  const hay = [String(cid), meta.username ? `@${meta.username.toLowerCase()}` : null].filter(Boolean);
  const match = (list) => list.some((x) => hay.includes(String(x).toLowerCase()));
  if (DM_ONLY_IN.length && !match(DM_ONLY_IN)) return false;
  if (DM_EXCLUDE.length && match(DM_EXCLUDE)) return false;
  return true;
}

function renderDm(name, meta) {
  const link = meta.username ? `https://t.me/${meta.username}` : "";
  return DM_TEMPLATE
    .replaceAll("{name}", name || "there")
    .replaceAll("{chat}", meta.title)
    .replaceAll("{link}", link)
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function dmJoin(uid, meta, cid, origin = "push") {
  if (!DM_ON_JOIN) return;
  uid = Number(uid);
  if (!uid || uid === selfId) return;
  if (DM_ONCE_PER_USER && dmSent.has(uid)) return;
  if (!dmAllowedChat(cid, meta)) return;
  if (dmQueue.length >= DM_MAX_QUEUE) {
    console.log(`[dm] queue full, dropping ${uid}`);
    return;
  }
  dmQueue.push({ uid, meta, cid, origin, tries: 0 });
  void drainDms();
}

async function drainDms() {
  if (dmDraining) return;
  dmDraining = true;
  try {
    while (dmQueue.length) {
      if (dmCapLeft() <= 0) {
        console.log(`[dm] daily cap ${DM_DAILY_CAP} reached, ${dmQueue.length} queued`);
        break;
      }
      const job = dmQueue.shift();

      let user = null;
      try {
        user = await client.getEntity(job.uid);
      } catch {
        continue;                                   // unresolvable, nothing to do
      }
      if (!user || user.deleted) continue;
      if (DM_SKIP_BOTS && user.bot) continue;

      const display = user.firstName || user.username || `id ${job.uid}`;
      const body = renderDm(user.firstName, job.meta);

      if (DM_DRY_RUN) {
        console.log(`[dm dry] ${display} (${job.uid}) in ${job.meta.title}\n${body}\n`);
        dmSent.set(job.uid, Date.now());
        continue;
      }

      try {
        await client.sendMessage(user, { message: body, linkPreview: false });
        dmSent.set(job.uid, Date.now());
        dmCount++;
        saveDmState();
        await log(`✉️ DM → ${display} (${job.uid})\n   from ${job.meta.title} [${job.origin}]`);
      } catch (e) {
        const msg = e?.errorMessage ?? e?.message ?? "";
        const wait = floodSeconds(e);

        if (wait && job.tries < 3) {
          job.tries++;
          dmQueue.unshift(job);
          console.log(`[dm] flood ${wait}s, requeueing ${job.uid}`);
          await sleep(wait * 1000 + 1000);
          continue;
        }

        const permanent = /USER_PRIVACY_RESTRICTED|PEER_ID_INVALID|USER_IS_BLOCKED|USER_DEACTIVATED|CHANNEL_PRIVATE|USER_NOT_MUTUAL_CONTACT/.test(msg);
        if (!permanent && job.tries < 2) {
          job.tries++;
          dmQueue.push(job);
          await sleep(8000);
          continue;
        }

        if (DM_LOG_FAILURES) {
          await log(`⚠️ DM failed — ${display} (${job.uid})\n   ${msg || "unknown"} [${job.origin}]`);
        }
        if (permanent) dmSent.set(job.uid, Date.now());   // don't retry next join
      }

      await sleep(DM_DELAY_MS);
    }
  } finally {
    dmDraining = false;
  }
}

// ---------- PROBE ----------
async function probe() {
  console.log(`[probe] ${chats.size} chats indexed`);
  const me = await client.getInputEntity(selfId);
  for (const [cid, meta] of chats) {
    if (meta.kind !== "channel") continue;
    let out = `${cid} "${meta.title}"`;
    try {
      const ent = meta.entity ?? (await client.getEntity(cid));
      const res = await client.invoke(new Api.channels.GetParticipants({
        channel: ent, filter: new Api.ChannelParticipantsSearch({ q: "" }),
        offset: 0, limit: 200, hash: BigInt(0),
      }));
      const full = await client.invoke(new Api.channels.GetFullChannel({ channel: ent }));
      const mine = await client.invoke(new Api.channels.GetParticipant({ channel: ent, participant: me }));
      const admin = ["ChannelParticipantCreator", "ChannelParticipantAdmin"]
        .includes(mine.participant?.className);
      out += ` | OK forbidden=${res.className === "ChatParticipantsForbidden"}`
        + ` members=${full.fullChat.participantsCount} hidden=${!!full.fullChat.participantsHidden} admin=${admin}`;
    } catch (e) {
      out += ` | FAIL ${e.message}`;
    }
    console.log("[probe]", out);
    await sleep(1500);
  }
}

// ---------- CHAT INDEX ----------
async function refreshChats() {
  const next = new Map();
  let count = 0;

  for (const archived of [false, true]) {
    let dialogs;
    try {
      dialogs = await client.getDialogs({ limit: 5000, archived });
    } catch (e) {
      if (!archived) {
        console.error("[index] refresh failed:", e.message);
        if (chats.size) return;
        return;
      }
      continue;
    }

    for (const d of dialogs) {
      if (!(d.isChannel || d.isGroup)) continue;
      const ent = d.entity;
      if (!ent) continue;

      const cid = ent.className === "Channel"
        ? -1000000000000 - Number(ent.id)
        : -Number(ent.id);
      if (cid === logPeerId) continue;

      const kind = ent.className === "Channel" ? (ent.megagroup ? "group" : "channel") : "group";

      next.set(cid, {
        title: d.title ?? ent.title ?? `chat ${cid}`,
        username: ent.username ?? null,
        kind,
        entity: ent,
      });
      count++;
    }
  }

  chats.clear();
  for (const [k, v] of next) chats.set(k, v);
  for (const cid of [...snapshots.keys()]) if (!chats.has(cid)) { snapshots.delete(cid); pollState.delete(cid); }
  for (const cid of [...pollState.keys()]) if (!chats.has(cid)) pollState.delete(cid);

  console.log(`[index] ${count} chats tracked`);
}

function requestRefresh() {
  if (refreshPromise) return refreshPromise;
  refreshPromise = refreshChats().finally(() => { refreshPromise = null; });
  return refreshPromise;
}

// ---------- LOG SINK ----------
async function log(line) {
  console.log(line);
  enqueue(async () => {
    try {
      await client.sendMessage(logPeer, { message: line, linkPreview: false });
    } catch (e) {
      console.error("[log] send failed:", e.message);
    }
  });
}

// ---------- MESSAGE HANDLER ----------
async function onMessage(event) {
  const msg = event.message;
  if (!msg) return;

  const kind = kindOf(msg.peerId);
  if (!kind) return;
  const cid = idOf(msg.peerId);
  if (cid == null || cid === logPeerId) return;
  if (!once(`msg:${cid}:${msg.id}`, MSG_DEDUPE_TTL_MS)) return;
  if (msg.out && !LOG_OWN_MESSAGES) return;
  if (!chats.has(cid)) void requestRefresh();

  if (msg.action) return onAction(msg, cid, kind);
  if (!LOG_MESSAGES) return;

  if (SKIP_BOTS && !msg.out && msg.senderId != null) {
    const sender = await client.getEntity(msg.senderId).catch(() => null);
    if (sender?.bot) return;
  }

  const meta = metaOf(cid, kind);
  const who = msg.out
    ? "you"
    : msg.senderId != null
      ? `${await nameOf(msg.senderId)} (${msg.senderId})`
      : "channel";
  const link = linkFor(msg.peerId, msg.id);

  await log(
    `💬 ${kind === "channel" ? "post" : "msg"} — ${who}\n` +
      `   in ${meta.title}\n` +
      `   ${describe(msg)}` +
      (link ? `\n   ${link}` : "")
  );
}

// ---------- SERVICE MESSAGES ----------
async function onAction(msg, cid, kind) {
  const meta = metaOf(cid, kind);
  const cls = msg.action?.className ?? "";

  if (cls === "MessageActionChannelMigrateFrom") {
    const oldCid = -Number(msg.action.chatId);
    const prev = snapshots.get(oldCid);
    if (prev) {
      snapshots.set(cid, prev);
      snapshots.delete(oldCid);
      pollState.delete(oldCid);
      saveSnapshots();
    }
    await log(`🔀 MIGRATED — ${meta.title}\n   ${oldCid} -> ${cid}`);
    void requestRefresh();
    return;
  }

  if (cls === "MessageActionChatAddUser") {
    for (const uid of msg.action.users ?? []) {
      if (Number(uid) === selfId) continue;
      if (LOG_JOINS && once(`join:${cid}:${uid}`, PUSH_TTL_MS)) {
        await log(`➕ JOIN — ${await nameOf(uid)} (${uid})\n   in ${meta.title}`);
      }
      dmJoin(uid, meta, cid, "added");
    }
    return;
  }

  if (cls === "MessageActionChatJoinedByLink") {
    if (Number(msg.senderId) === selfId) return;
    if (LOG_JOINS && once(`join:${cid}:${msg.senderId}`, PUSH_TTL_MS)) {
      await log(`➕ JOIN — ${await nameOf(msg.senderId)} (${msg.senderId}) via invite link\n   in ${meta.title}`);
    }
    dmJoin(msg.senderId, meta, cid, "invite");
    return;
  }

  if (cls === "MessageActionChatDeleteUser" && LOG_LEAVES) {
    if (!once(`leave:${cid}:${msg.action.userId}`, PUSH_TTL_MS)) return;
    await log(`➖ LEAVE — ${await nameOf(msg.action.userId)} (${msg.action.userId})\n   in ${meta.title}`);
  }
}

// ---------- PARTICIPANT UPDATES ----------
async function onParticipant(update) {
  const cls = update?.className ?? "";
  const isChannel = cls === "UpdateChannelParticipant";
  const isChat = cls === "UpdateChatParticipant";
  if (!isChannel && !isChat) return;

  if (DEBUG_PARTICIPANTS) {
    console.log("[raw]", cls, update.channelId ?? update.chatId, update.userId,
      "prev:", update.prevParticipant?.className ?? "undefined",
      "new:", update.newParticipant?.className ?? "undefined");
  }

  const rawId = isChannel ? update.channelId : update.chatId;
  if (rawId == null) return;
  const cid = isChannel ? -1000000000000 - Number(rawId) : -Number(rawId);
  if (Number.isNaN(cid) || cid === logPeerId) return;

  const join = update.prevParticipant == null;
  const leave = update.newParticipant == null;
  if (!join && !leave) return;

  const uid = update.userId;
  if (uid == null || Number(uid) === selfId) return;
  if (!chats.has(cid)) void requestRefresh();
  const meta = metaOf(cid, isChannel ? "channel" : "group");

  if (join) {
    if (LOG_JOINS && once(`join:${cid}:${uid}`, PUSH_TTL_MS)) {
      await log(`➕ JOIN — ${await nameOf(uid)} (${uid})\n   in ${meta.title}`);
    }
    dmJoin(uid, meta, cid, "update");
    return;
  }

  if (LOG_LEAVES && once(`leave:${cid}:${uid}`, PUSH_TTL_MS)) {
    await log(`➖ LEAVE — ${await nameOf(uid)} (${uid})\n   in ${meta.title}`);
  }
}

// ---------- POLL FALLBACK ----------
function pollReady(cid) {
  const s = pollState.get(cid);
  if (!s) return true;
  if (s.skip) return false;
  return Date.now() >= s.nextAt;
}

function pollFail(cid, err) {
  const s = pollState.get(cid) ?? { fails: 0, nextAt: 0, skip: false };
  s.fails++;
  if (/CHAT_ADMIN_REQUIRED|CHANNEL_PRIVATE|CHANNEL_INVALID|PARTICIPANT_ID_INVALID/.test(err.message)) {
    s.skip = true;
  } else {
    s.nextAt = Date.now() + Math.min(30 * 60_000, 2 ** s.fails * 15_000);
  }
  pollState.set(cid, s);
}

async function fetchMembers(entity) {
  const out = new Set();
  let offset = 0;
  for (let page = 0; page < POLL_MAX_PAGES; page++) {
    const res = await client.invoke(new Api.channels.GetParticipants({
      channel: entity,
      filter: new Api.ChannelParticipantsSearch({ q: "" }),
      offset,
      limit: 200,
      hash: BigInt(0),
    }));

    if (res.className === "ChatParticipantsForbidden") return { ids: out, forbidden: true };

    const parts = res.participants ?? [];
    for (const p of parts) out.add(Number(p.userId));
    if (parts.length < 200) break;
    offset += parts.length;
    await sleep(POLL_PAGE_DELAY_MS);
  }
  return { ids: out, forbidden: false };
}

async function pollChat(cid, meta) {
  if (cid === logPeerId || !pollReady(cid)) return;
  if (!chats.has(cid)) return;
  if (meta.entity?.className === "Chat") {
    pollState.set(cid, { fails: 0, nextAt: 0, skip: true });
    return;
  }

  try {
    const entity = meta.entity ?? (await client.getEntity(cid));
    const { ids: now, forbidden } = await fetchMembers(entity);

    if (forbidden) {
      pollState.set(cid, { fails: 0, nextAt: 0, skip: true });
      return;
    }

    const prev = snapshots.get(cid);
    if (prev && prev.size && now.size === 0) {
      console.log(`[poll] ${meta.title}: empty result, keeping snapshot`);
      pollState.set(cid, { fails: 0, nextAt: 0, skip: false });
      return;
    }

    if (prev) {
      for (const uid of now) {
        if (prev.has(uid) || uid === selfId) continue;
        if (LOG_JOINS && once(`join:${cid}:${uid}`, PUSH_TTL_MS)) {
          await log(`➕ JOIN — ${await nameOf(uid)} (${uid}) [polled]\n   in ${meta.title}`);
        }
        dmJoin(uid, meta, cid, "polled");
      }
      for (const uid of prev) {
        if (now.has(uid) || uid === selfId) continue;
        if (!once(`leave:${cid}:${uid}`, PUSH_TTL_MS)) continue;
        await log(`➖ LEAVE — ${await nameOf(uid)} (${uid}) [polled]\n   in ${meta.title}`);
      }
      if (prev.size !== now.size) console.log(`[poll] ${meta.title}: ${prev.size} -> ${now.size}`);
    }

    snapshots.set(cid, now);
    pollState.set(cid, { fails: 0, nextAt: 0, skip: false });
  } catch (e) {
    pollFail(cid, e);
  }
}

let pollBusy = false;

async function pollAll() {
  if (!ENABLE_POLLING || pollBusy || shuttingDown) return;
  pollBusy = true;
  let ok = 0, skip = 0, fail = 0;
  try {
    for (const [cid, meta] of chats) {
      if (cid === logPeerId) continue;
      if (!pollReady(cid)) { skip++; continue; }
      const before = pollState.get(cid)?.fails ?? 0;
      await pollChat(cid, meta);
      if ((pollState.get(cid)?.fails ?? 0) > before) fail++; else ok++;
      await sleep(POLL_CHAT_DELAY_MS);
    }
  } finally {
    saveSnapshots();
    pollBusy = false;
    console.log(`[poll] cycle done — ok=${ok} skip=${skip} fail=${fail}`);
  }
}

// ---------- MAIN ----------
await client.start({
  phoneNumber: async () => await input.text("phone: "),
  password: async () => await input.text("2FA password: "),
  phoneCode: async () => await input.text("code: "),
  onError: (e) => console.error("auth error:", e),
});

selfId = Number((await client.getMe()).id);
if (!fs.existsSync(SESSION_FILE)) {
  fs.writeFileSync(SESSION_FILE, client.session.save(), { mode: 0o600 });
}

logPeer = await client.getEntity(LOG_TO);
if (LOG_TO === "me") {
  logPeerId = selfId;
} else if (logPeer.className === "Channel") {
  logPeerId = -1000000000000 - Number(logPeer.id);
} else if (logPeer.className === "Chat") {
  logPeerId = -Number(logPeer.id);
} else {
  logPeerId = null;
}

await refreshChats();

if (process.argv.includes("probe")) {
  await probe();
  await client.disconnect();
  process.exit(0);
}

// preview the DM against a real user without sending
const testIdx = process.argv.indexOf("dmtest");
if (testIdx !== -1) {
  const target = process.argv[testIdx + 1];
  if (!target) { console.log("usage: node channel-watch.js dmtest <user id | @username>"); process.exit(1); }
  const user = await client.getEntity(target.startsWith("@") ? target : Number(target));
  const meta = chats.values().next().value ?? { title: "Some Channel", username: null };
  console.log(`--- to ${user.firstName ?? user.username} (${user.id}) ---`);
  console.log(renderDm(user.firstName, meta));
  console.log("---");
  await client.disconnect();
  process.exit(0);
}

const guard = (name, fn) => (e) => void fn(e).catch((err) => console.error(`${name} handler:`, err.message));

client.addEventHandler(guard("message", onMessage), new NewMessage({}));
client.addEventHandler(
  guard("participant", onParticipant),
  new Raw({ types: [Api.UpdateChannelParticipant, Api.UpdateChatParticipant] })
);

loadSnapshots();
loadDmState();
void pollAll();
setInterval(() => void pollAll(), POLL_MS).unref();
setInterval(() => void requestRefresh(), REFRESH_MS).unref();

console.log(`logging to ${LOG_TO === "me" ? "Saved Messages" : logPeer.title ?? LOG_TO}`);
console.log(DM_ON_JOIN
  ? `DMing joiners — cap ${DM_DAILY_CAP}/day, ${DM_DELAY_MS}ms gap${DM_DRY_RUN ? ", DRY RUN" : ""}`
  : "join DMs disabled");
console.log("watching all channels and groups");

process.on("SIGINT", async () => {
  shuttingDown = true;
  console.log("\nshutting down...");
  saveSnapshots();
  saveDmState();
  const deadline = Date.now() + 4000;
  while ((queue.length || draining) && Date.now() < deadline) await sleep(100);
  try { await client.disconnect(); } catch {}
  process.exit(0);
});

await new Promise(() => {});