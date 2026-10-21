// TV Telegram Bot - a TizenBrew service module that runs ON the Samsung TV.
// Polls Telegram (long polling, no open port needed) and sends remote keys to the TV itself
// (wss://127.0.0.1:8002 samsung.remote.control). The code is public (served by jsDelivr), so it holds
// NO secrets: the bot token / chat id live in a config file on the TV, written once over the LAN
// (POST http://<tv-ip>:8095/config). Written in ES5: TizenBrew services may run on an old Node (v4).
"use strict";

var fs = require("fs");
var http = require("http");
var https = require("https");
var tls = require("tls");
var crypto = require("crypto");

var ENV = (typeof process !== "undefined" && process.env) || {};
var CONFIG_FILE = ENV.TVBOT_CONFIG || "/home/owner/share/tvbot-config.json";
var TV_HOST = ENV.TVBOT_TV_HOST || "127.0.0.1";
var HTTP_PORT = Number(ENV.TVBOT_PORT || 8095);
var VERSION = "0.1.0";

var state = { started: new Date().toISOString(), lastPoll: null, lastError: null, botName: null, logs: [], timer: null };

function log(msg) {
  var line = new Date().toISOString().replace("T", " ").slice(0, 19) + " " + msg;
  state.logs.push(line);
  if (state.logs.length > 30) state.logs.shift();
  try { console.log("[tvbot] " + line); } catch (e) { /* no console */ }
}

function toBuf(x, enc) {
  return Buffer.from && Buffer.from !== Uint8Array.from ? Buffer.from(x, enc) : new Buffer(x, enc); // eslint-disable-line
}

function readConfig() {
  try { return JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8")); } catch (e) { return {}; }
}
function writeConfig(c) {
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(c, null, 2));
}

// ── Samsung remote control (minimal WebSocket client over TLS) ─────────────────────────────
function wsFrame(text) {
  var payload = toBuf(text, "utf8");
  var len = payload.length;
  var head;
  if (len < 126) { head = toBuf([0x81, 0x80 | len]); }
  else if (len < 65536) { head = toBuf([0x81, 0x80 | 126, len >> 8, len & 255]); }
  else { throw new Error("frame too large"); }
  var mask = crypto.randomBytes(4);
  var masked = toBuf(payload); // copy
  for (var i = 0; i < len; i++) masked[i] = payload[i] ^ mask[i % 4];
  return Buffer.concat([head, mask, masked]);
}

// Parses complete server frames from buf; returns { messages: [text...], rest: Buffer }.
function wsParse(buf) {
  var out = [];
  while (buf.length >= 2) {
    var b1 = buf[1];
    var len = b1 & 127;
    var off = 2;
    if (len === 126) { if (buf.length < 4) break; len = buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (buf.length < 10) break; len = buf.readUInt32BE(6); off = 10; }
    var masked = (b1 & 128) !== 0;
    var mask = null;
    if (masked) { if (buf.length < off + 4) break; mask = buf.slice(off, off + 4); off += 4; }
    if (buf.length < off + len) break;
    var data = buf.slice(off, off + len);
    if (mask) for (var i = 0; i < data.length; i++) data[i] ^= mask[i % 4];
    var opcode = buf[0] & 15;
    if (opcode === 1) out.push(data.toString("utf8"));
    buf = buf.slice(off + len);
  }
  return { messages: out, rest: buf };
}

/** Sends one remote key (KEY_POWER, KEY_MUTE...) to the TV. cb(err). The first time the TV shows an
 *  "Allow" prompt; the token it returns is saved so later connections need no prompt. */
function sendKey(key, cb) {
  var cfg = readConfig();
  var name = toBuf("TV Telegram Bot", "utf8").toString("base64");
  var path = "/api/v2/channels/samsung.remote.control?name=" + encodeURIComponent(name) + (cfg.tvToken ? "&token=" + encodeURIComponent(cfg.tvToken) : "");
  var done = false;
  function finish(err) { if (done) return; done = true; clearTimeout(t); try { sock.end(); } catch (e) { /* closed */ } cb(err || null); }
  var t = setTimeout(function () { finish(new Error("TV did not answer (no Allow on the TV screen?)")); }, 30000);
  var sock = tls.connect({ host: TV_HOST, port: 8002, rejectUnauthorized: false }, function () {
    var key64 = crypto.randomBytes(16).toString("base64");
    sock.write("GET " + path + " HTTP/1.1\r\nHost: " + TV_HOST + ":8002\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
      "Sec-WebSocket-Key: " + key64 + "\r\nSec-WebSocket-Version: 13\r\n\r\n");
  });
  var buf = toBuf([]);
  var upgraded = false;
  sock.on("data", function (chunk) {
    buf = Buffer.concat([buf, chunk]);
    if (!upgraded) {
      var end = buf.toString("binary").indexOf("\r\n\r\n");
      if (end < 0) return;
      var head = buf.slice(0, end).toString("utf8");
      if (head.indexOf(" 101 ") < 0) return finish(new Error("handshake refused: " + head.split("\r\n")[0]));
      upgraded = true;
      buf = buf.slice(end + 4);
    }
    var r = wsParse(buf);
    buf = r.rest;
    r.messages.forEach(function (m) {
      var msg;
      try { msg = JSON.parse(m); } catch (e) { return; }
      if (msg.event === "ms.channel.connect") {
        var tok = msg.data && msg.data.token;
        if (tok && tok !== cfg.tvToken) { var c = readConfig(); c.tvToken = tok; writeConfig(c); log("TV token saved"); }
        if (key === null) return finish(null); // connect test only
        sock.write(wsFrame(JSON.stringify({ method: "ms.remote.control", params: { Cmd: "Click", DataOfCmd: key, Option: "false", TypeOfRemote: "SendRemoteKey" } })));
        setTimeout(function () { finish(null); }, 400);
      } else if (msg.event === "ms.channel.unauthorized") {
        finish(new Error("TV refused: press Allow on the TV when it asks"));
      }
    });
  });
  sock.on("error", function (e) { finish(e); });
  sock.on("close", function () { finish(done ? null : new Error("TV closed the connection")); });
}

// ── Telegram ───────────────────────────────────────────────────────────────────────────────
function tg(method, params, timeoutMs, cb) {
  var cfg = readConfig();
  if (!cfg.botToken) return cb(new Error("not configured"));
  var body = JSON.stringify(params || {});
  var req = https.request({
    host: "api.telegram.org", path: "/bot" + cfg.botToken + "/" + method, method: "POST",
    headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) },
  }, function (res) {
    var chunks = [];
    res.on("data", function (c) { chunks.push(c); });
    res.on("end", function () {
      var j;
      try { j = JSON.parse(Buffer.concat(chunks).toString("utf8")); } catch (e) { return cb(new Error("bad Telegram reply (" + res.statusCode + ")")); }
      if (!j.ok) return cb(new Error("Telegram: " + (j.description || res.statusCode)));
      cb(null, j.result);
    });
  });
  req.setTimeout(timeoutMs, function () { req.abort(); });
  req.on("error", function (e) { cb(e); });
  req.end(body);
}

function reply(chatId, text) {
  tg("sendMessage", { chat_id: chatId, text: text }, 15000, function (err) { if (err) log("send failed: " + err.message); });
}

var HELP = "Lệnh:\n/tat - tắt tivi ngay\n/tatsau 30 - tắt sau 30 phút\n/huy - huỷ hẹn giờ\n/trangthai - tivi có đang bật không\n/tieng - bật/tắt tiếng\n/to /nho - tăng/giảm âm lượng";

function powerOff(chatId, why) {
  sendKey("KEY_POWER", function (err) {
    if (err) { log("power off failed: " + err.message); if (chatId) reply(chatId, "Không tắt được: " + err.message); return; }
    log("power off (" + why + ")");
    if (chatId) reply(chatId, "Đã tắt tivi.");
  });
}

function handle(msg) {
  var cfg = readConfig();
  var chatId = msg.chat && msg.chat.id;
  if (!cfg.chatId || String(chatId) !== String(cfg.chatId)) { log("ignored message from chat " + chatId); return; }
  var text = String(msg.text || "").trim();
  var cmd = text.split(/\s+/)[0].toLowerCase().replace(/@.*$/, "");
  var arg = text.split(/\s+/)[1];
  if (cmd === "/tat") return powerOff(chatId, "telegram");
  if (cmd === "/tatsau") {
    var min = Number(arg);
    if (!(min > 0 && min <= 600)) return reply(chatId, "Ví dụ: /tatsau 30 (số phút, tối đa 600)");
    if (state.timer) clearTimeout(state.timer.t);
    var at = Date.now() + min * 60000;
    state.timer = { at: at, t: setTimeout(function () { state.timer = null; powerOff(chatId, "timer"); }, min * 60000) };
    var d = new Date(at);
    return reply(chatId, "Sẽ tắt tivi sau " + min + " phút (lúc " + ("0" + d.getHours()).slice(-2) + ":" + ("0" + d.getMinutes()).slice(-2) + ").");
  }
  if (cmd === "/huy") {
    if (!state.timer) return reply(chatId, "Không có hẹn giờ nào.");
    clearTimeout(state.timer.t); state.timer = null;
    return reply(chatId, "Đã huỷ hẹn giờ tắt.");
  }
  if (cmd === "/trangthai") {
    var left = state.timer ? " Hẹn tắt sau " + Math.ceil((state.timer.at - Date.now()) / 60000) + " phút." : "";
    return reply(chatId, "Tivi đang bật (bot chạy trên tivi từ " + state.started.slice(11, 16) + " UTC)." + left);
  }
  var keys = { "/tieng": "KEY_MUTE", "/to": "KEY_VOLUP", "/nho": "KEY_VOLDOWN" };
  if (keys[cmd]) return sendKey(keys[cmd], function (err) { reply(chatId, err ? "Lỗi: " + err.message : "OK"); });
  reply(chatId, HELP);
}

var offset = 0;
var polling = false;
function poll() {
  if (polling) return;
  if (!readConfig().botToken) { setTimeout(poll, 5000); return; }
  polling = true;
  tg("getUpdates", { offset: offset, timeout: 50, allowed_updates: ["message"] }, 65000, function (err, updates) {
    polling = false;
    state.lastPoll = new Date().toISOString();
    if (err) { state.lastError = err.message; log("poll: " + err.message); setTimeout(poll, 5000); return; }
    state.lastError = null;
    (updates || []).forEach(function (u) {
      offset = u.update_id + 1;
      if (u.message) { try { handle(u.message); } catch (e) { log("handler: " + e.message); } }
    });
    setTimeout(poll, 200);
  });
}

function hello() {
  tg("getMe", {}, 15000, function (err, me) {
    if (err) { state.lastError = err.message; log("getMe: " + err.message); return; }
    state.botName = me.username;
    log("bot @" + me.username + " ready");
    var cfg = readConfig();
    if (cfg.chatId && cfg.announce !== false) reply(cfg.chatId, "Tivi vừa bật, bot sẵn sàng. Gõ /tat để tắt.");
  });
}

// ── LAN config / status endpoint ───────────────────────────────────────────────────────────
// GET /status -> JSON. POST /config {botToken, chatId, adminKey}: the first write sets adminKey;
// later writes must send the same adminKey. GET /testkey?key=KEY_MUTE needs ?adminKey= too.
http.createServer(function (req, res) {
  function send(code, obj) { res.writeHead(code, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }); res.end(JSON.stringify(obj)); }
  var url = req.url || "/";
  var cfg = readConfig();
  if (req.method === "GET" && url.indexOf("/status") === 0) {
    return send(200, { version: VERSION, configured: !!(cfg.botToken && cfg.chatId), bot: state.botName, started: state.started, lastPoll: state.lastPoll, lastError: state.lastError, tvToken: !!cfg.tvToken, timerAt: state.timer ? new Date(state.timer.at).toISOString() : null, logs: state.logs });
  }
  var q = {};
  (url.split("?")[1] || "").split("&").forEach(function (p) { var kv = p.split("="); if (kv[0]) q[decodeURIComponent(kv[0])] = decodeURIComponent(kv[1] || ""); });
  if (req.method === "GET" && url.indexOf("/testkey") === 0) {
    if (!cfg.adminKey || q.adminKey !== cfg.adminKey) return send(403, { error: "adminKey" });
    return sendKey(q.key === "connect" ? null : q.key || null, function (err) { send(err ? 500 : 200, err ? { error: err.message } : { ok: true }); });
  }
  if (req.method === "POST" && url.indexOf("/config") === 0) {
    var body = "";
    req.on("data", function (c) { body += c; if (body.length > 10000) req.destroy(); });
    req.on("end", function () {
      var j;
      try { j = JSON.parse(body); } catch (e) { return send(400, { error: "json" }); }
      if (cfg.adminKey && j.adminKey !== cfg.adminKey) return send(403, { error: "adminKey" });
      if (!j.adminKey || String(j.adminKey).length < 16) return send(400, { error: "adminKey (16+ chars) required" });
      var c = readConfig();
      ["botToken", "chatId", "announce"].forEach(function (k) { if (j[k] !== undefined) c[k] = j[k]; });
      c.adminKey = j.adminKey;
      writeConfig(c);
      log("config updated");
      send(200, { ok: true });
      hello();
    });
    return;
  }
  send(404, { error: "not found" });
}).on("error", function (e) { log("http: " + e.message); }).listen(HTTP_PORT);

log("service " + VERSION + " started (tv " + TV_HOST + ", config " + CONFIG_FILE + ")");
hello();
poll();
