'use strict';
/**
 * xray-voroa-xhttp — Node.js runtime wrapper for running xray
 * (VLESS + XHTTP, with VLESS + WS kept as a second transport) on a
 * free PaaS platform. Variant of ~/workspace/deploy-proxy/voroa/server.js.
 *
 * Why XHTTP: on lossy China international links the WS nodes are slow.
 * XHTTP (xray 26.x's recommended replacement for the deprecated WS
 * transport) splits the connection into a long-lived download stream
 * plus small upload POSTs, which survives packet loss far better than
 * one long-lived WebSocket. It works over plain HTTP/1.1 — no HTTP/2
 * required — so a TLS-terminating PaaS gateway that forwards plain
 * HTTP to the app is enough.
 *
 * What it does
 *  1. Finds the xray binary: bundled binary first (several candidate
 *     paths); if it is missing, downloads the pinned release from
 *     GitHub at runtime and extracts it with pure JS (same code as
 *     the WS wrapper — see below).
 *  2. Writes an xray config from env vars and starts xray with TWO
 *     local inbounds on 127.0.0.1: VLESS+WS and VLESS+XHTTP, both
 *     with the same UUID.
 *  3. Listens on $PORT (the port the platform assigns) with a real
 *     HTTP server:
 *       - a WebSocket upgrade  -> rebuilt and forwarded byte-for-byte
 *         to the local WS inbound (xray validates the path);
 *       - any other request whose path is the XHTTP path (or under
 *         it) -> reverse-proxied, headers + streaming body both
 *         ways, to the local XHTTP inbound;
 *       - anything else (platform health checks, stray probes)
 *         -> 200 "ok".
 *
 * Why the XHTTP match is a PREFIX, not an exact path: xray's XHTTP
 * client appends a per-connection session id (and a sequence number)
 * to the URL path by default (sessionIDPlacement defaults to
 * "path"), so real requests arrive as <XHTTP_PATH>/<sessionId>[/<seq>].
 * The local xray server itself validates the prefix, so the wrapper
 * only needs the same prefix check. Exact-path matching would break
 * every XHTTP request.
 *
 * Long-lived streams: XHTTP's download stream holds the response
 * open for the life of the connection, so server.requestTimeout is
 * disabled (Node's default 300 s would kill the stream mid-flight).
 * Bodies are never buffered — req.pipe(proxyReq) and
 * proxyRes.pipe(res) stream both directions.
 *
 * Upstream Connection header (important, found by A/B test 2026-11-19):
 * the xray client itself sends `Connection: close` on every XHTTP
 * request (each session is a dedicated connection). The proxy must
 * preserve that: Go's server does NOT answer a stream-one XHTTP
 * request that arrives with `Connection: keep-alive` — it holds the
 * response until the connection closes (identical bytes: `close` ->
 * 200 in 72 ms, `keep-alive` -> no response in 6 s+). Node's
 * keep-alive agent would otherwise substitute `keep-alive`
 * (filterHeaders strips the client's `close`), hanging every
 * stream-one session. So proxyXhttp forces `Connection: close`
 * upstream. The socket is per-session and is not reused anyway.
 *
 * Listen-first startup: the front binds $PORT before the xray binary
 * is ensured (the buildpack-path runtime download can be slow). The
 * platform health check hits $PORT during startup; starting the front
 * only after xray is up times the check out and fails the deploy
 * (Eurobase first round). Early XHTTP/WS requests get a 502 until
 * xray is listening, but plain HTTP already answers "ok".
 *
 * Modes: the server xhttpSettings sets only the path (no mode), so it
 * accepts every client mode (auto/packet-up, stream-up, stream-one).
 * Measured through this wrapper (2026-11-19, local, via the VM egress
 * proxy): stream-one 12/12, packet-up (auto) 21/25 then 13/25 — the
 * packet-up failures are silent client-side stalls (download GET
 * arrives, upload POST seq 0 never sent; the client never logs an
 * error). Prefer client mode "stream-one" for XHTTP deployments; the
 * default auto/packet-up still works most of the time but is not
 * fully reliable through an HTTP/1.1 proxy.
 *
 * Env vars
 *  PORT             port assigned by the platform (default 8080)
 *  UUID             VLESS user id (required)
 *  WS_PATH          WebSocket path, e.g. /ws-xxxx (default /ws)
 *  XHTTP_PATH       XHTTP path; defaults to $WS_PATH so existing
 *                   deployment params keep working unchanged. Set it
 *                   only if the XHTTP path should differ from WS.
 *  XRAY_PORT        local xray WS port (default: pick a free one)
 *  XRAY_XHTTP_PORT  local xray XHTTP port (default: pick a free one)
 *  XRAY_VERSION     release to download if no bundled binary
 *                   (default v26.10.31 — the version verified locally)
 *  XRAY_HTTP_PROXY  optional http://user:pass@host:port — if set, the
 *                   xray outbound goes through that HTTP proxy.
 *                   Local-test-only knob; production uses freedom.
 *
 * No third-party dependencies: Node built-ins only.
 */

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const http = require('node:http');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { Readable } = require('node:stream');
const zlib = require('node:zlib');

const log = (...a) => console.log(new Date().toISOString(), '[wrapper]', ...a);
const logErr = (...a) => console.error(new Date().toISOString(), '[wrapper]', ...a);

const PORT = parseInt(process.env.PORT || '8080', 10);
const UUID = process.env.UUID || '';
const WS_PATH = process.env.WS_PATH || process.env.APP_ROUTE || '/ws';
const XHTTP_PATH = process.env.XHTTP_PATH || WS_PATH;
const XRAY_VERSION = process.env.XRAY_VERSION || 'v26.10.31';
const XRAY_HTTP_PROXY = process.env.XRAY_HTTP_PROXY || '';
const CONFIG_PATH = path.join(__dirname, 'xray-config.json');

// Set WRAPPER_DEBUG=1 for one log line per proxied XHTTP request and
// WS upgrade (start / response status / end / error). Off by default:
// the deployed wrapper stays quiet.
const DEBUG = process.env.WRAPPER_DEBUG === '1';
const debug = (...a) => {
  if (DEBUG) log('[debug]', ...a);
};
let xhttpSeq = 0;

if (!UUID) {
  logErr('FATAL: UUID env var is required');
  process.exit(1);
}
for (const [name, value] of [
  ['WS_PATH', WS_PATH],
  ['XHTTP_PATH', XHTTP_PATH],
]) {
  if (!value.startsWith('/')) {
    logErr(`FATAL: ${name} must start with /`);
    process.exit(1);
  }
}
if (!Number.isInteger(PORT) || PORT <= 0 || PORT > 65535) {
  logErr('FATAL: invalid PORT:', process.env.PORT);
  process.exit(1);
}

function releaseUrl(version) {
  return `https://github.com/XTLS/Xray-core/releases/download/${version}/Xray-linux-64.zip`;
}

async function isExecutable(p) {
  try {
    await fs.promises.access(p, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

// --- pure-JS ZIP extraction (no external archive tool) -----------------
// Identical to ~/workspace/deploy-proxy/voroa/server.js: parses the End
// of Central Directory + central directory, then inflates the wanted
// entry with zlib. Handles stored (method 0) and deflate (method 8)
// entries, data descriptors (sizes are taken from the central
// directory, so they do not matter), and ZIP64 size fields.

const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c >>> 0;
  }
  return t;
})();

function crc32(buf) {
  let c = 0xffffffff;
  for (let i = 0; i < buf.length; i++) c = CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

async function extractZipEntry(zipPath, wantedName, destPath) {
  const zip = await fs.promises.readFile(zipPath);
  // Find the End of Central Directory record (last 64 KiB + 22 bytes).
  const tailLen = Math.min(zip.length, 65557);
  const tailStart = zip.length - tailLen;
  let eocd = -1;
  for (let i = zip.length - 22; i >= tailStart; i--) {
    if (zip.readUInt32LE(i) === 0x06054b50) {
      eocd = i;
      break;
    }
  }
  if (eocd === -1) throw new Error('not a ZIP file (no end-of-central-directory record)');
  const totalEntries = zip.readUInt16LE(eocd + 10);
  const cdOffset = zip.readUInt32LE(eocd + 16);
  if (totalEntries === 0xffff || cdOffset === 0xffffffff) {
    throw new Error('ZIP64 archives are not supported by this extractor');
  }
  let p = cdOffset;
  let found = null;
  for (let i = 0; i < totalEntries; i++) {
    if (p + 46 > zip.length || zip.readUInt32LE(p) !== 0x02014b50) {
      throw new Error('corrupt central directory at entry ' + i);
    }
    const method = zip.readUInt16LE(p + 10);
    const crc = zip.readUInt32LE(p + 16);
    let compSize = zip.readUInt32LE(p + 20);
    let uncompSize = zip.readUInt32LE(p + 24);
    const nameLen = zip.readUInt16LE(p + 28);
    const extraLen = zip.readUInt16LE(p + 30);
    const commentLen = zip.readUInt16LE(p + 32);
    const localOffset = zip.readUInt32LE(p + 42);
    const name = zip.subarray(p + 46, p + 46 + nameLen).toString('utf8');
    if (compSize === 0xffffffff || uncompSize === 0xffffffff) {
      // ZIP64 extra field (0x0001): uncompressed size, then compressed size.
      let ep = p + 46 + nameLen;
      const extraEnd = ep + extraLen;
      while (ep + 4 <= extraEnd) {
        const id = zip.readUInt16LE(ep);
        const sz = zip.readUInt16LE(ep + 2);
        if (id === 0x0001) {
          let q = ep + 4;
          if (uncompSize === 0xffffffff) {
            uncompSize = Number(zip.readBigUInt64LE(q));
            q += 8;
          }
          if (compSize === 0xffffffff) {
            compSize = Number(zip.readBigUInt64LE(q));
            q += 8;
          }
          break;
        }
        ep += 4 + sz;
      }
    }
    if (name === wantedName || name.endsWith('/' + wantedName)) {
      found = { method, crc, compSize, uncompSize, localOffset };
      break;
    }
    p += 46 + nameLen + extraLen + commentLen;
  }
  if (!found) throw new Error(`entry '${wantedName}' not found in ${zipPath}`);
  if (found.method !== 0 && found.method !== 8) {
    throw new Error(`unsupported compression method ${found.method} for '${wantedName}'`);
  }
  const lh = found.localOffset;
  if (zip.readUInt32LE(lh) !== 0x04034b50) throw new Error('corrupt local file header');
  const lhNameLen = zip.readUInt16LE(lh + 26);
  const lhExtraLen = zip.readUInt16LE(lh + 28);
  const dataStart = lh + 30 + lhNameLen + lhExtraLen;
  const comp = zip.subarray(dataStart, dataStart + found.compSize);
  if (comp.length !== found.compSize) throw new Error('truncated ZIP entry data');
  const data = found.method === 8 ? zlib.inflateRawSync(comp) : Buffer.from(comp);
  if (data.length !== found.uncompSize) {
    throw new Error(`size mismatch after extraction: got ${data.length}, expected ${found.uncompSize}`);
  }
  if (crc32(data) !== found.crc) throw new Error('CRC32 mismatch after extraction');
  await fs.promises.writeFile(destPath, data);
  await fs.promises.chmod(destPath, 0o755);
  return data.length;
}

// --- xray binary -------------------------------------------------------

async function downloadRelease(url, dest) {
  const res = await fetch(url, { redirect: 'follow', signal: AbortSignal.timeout(300000) });
  if (!res.ok) throw new Error(`HTTP ${res.status} downloading ${url}`);
  const ws = fs.createWriteStream(dest);
  await new Promise((resolve, reject) => {
    Readable.fromWeb(res.body).pipe(ws).on('finish', resolve).on('error', reject);
  });
}

// Check the bundled binary in several places: the platform may run from
// a different working directory than the script's directory, or may
// strip the executable bit when it unpacks the upload.
async function findBundledBinary() {
  const candidates = [path.join(__dirname, 'xray'), path.resolve('xray'), path.join(process.cwd(), 'xray')];
  const seen = new Set();
  for (const p of candidates) {
    if (seen.has(p)) continue;
    seen.add(p);
    let st;
    try {
      st = await fs.promises.stat(p);
    } catch (e) {
      if (e.code !== 'ENOENT') log(`bundled-binary candidate ${p}: ${e.message}`);
      continue;
    }
    if (!st.isFile()) {
      log(`bundled-binary candidate ${p}: not a regular file`);
      continue;
    }
    // The platform may unpack the upload without the exec bit; fix it.
    try {
      await fs.promises.chmod(p, 0o755);
    } catch {
      /* ignore */
    }
    if (await isExecutable(p)) {
      log(`using bundled xray binary: ${p} (${st.size} bytes)`);
      return p;
    }
    log(`bundled-binary candidate ${p}: exists (${st.size} bytes) but not executable even after chmod`);
  }
  return null;
}

async function logDirectoryContents() {
  for (const dir of [process.cwd(), __dirname]) {
    try {
      const entries = await fs.promises.readdir(dir, { withFileTypes: true });
      log(
        `contents of ${dir}:`,
        entries.map((e) => (e.isDirectory() ? e.name + '/' : e.name)).join(', ') || '(empty)',
      );
    } catch (e) {
      log(`cannot list ${dir}: ${e.message}`);
    }
  }
}

// Prefer the bundled binary; fall back to a runtime download + pure-JS
// extraction. Never shells out to an external archive tool.
async function ensureXray() {
  const bundled = await findBundledBinary();
  if (bundled) return bundled;
  const url = releaseUrl(XRAY_VERSION);
  log('bundled xray binary not found, downloading', url);
  const zipPath = path.join(os.tmpdir(), 'xray-linux-64.zip');
  try {
    await downloadRelease(url, zipPath);
  } catch (e) {
    logErr('FATAL: failed to download xray release:', e.message);
    process.exit(1);
  }
  const dests = [path.join(__dirname, 'xray'), path.join(os.tmpdir(), 'xray')];
  let dest = null;
  for (const d of dests) {
    try {
      const n = await extractZipEntry(zipPath, 'xray', d);
      dest = d;
      log(`extracted xray (${n} bytes) to ${d} using built-in zlib`);
      break;
    } catch (e) {
      log(`extraction to ${d} failed: ${e.message}`);
    }
  }
  await fs.promises.unlink(zipPath).catch(() => {});
  if (!dest) {
    logErr('FATAL: could not extract xray from the downloaded release');
    process.exit(1);
  }
  if (!(await isExecutable(dest))) {
    logErr('FATAL: xray binary still not executable after download + extraction');
    process.exit(1);
  }
  log('downloaded and extracted xray', XRAY_VERSION);
  return dest;
}

function httpProxyOutbound(proxyUrl) {
  let u;
  try {
    u = new URL(proxyUrl);
  } catch {
    throw new Error('XRAY_HTTP_PROXY is not a valid URL: ' + proxyUrl);
  }
  const server = { address: u.hostname, port: parseInt(u.port || '8080', 10) };
  if (u.username) {
    server.users = [
      { user: decodeURIComponent(u.username), pass: decodeURIComponent(u.password || '') },
    ];
  }
  return { protocol: 'http', tag: 'proxy-out', settings: { servers: [server] } };
}

// Two inbounds, same UUID: WS (kept from the original wrapper) and
// XHTTP. The XHTTP inbound sets only the path — no "mode": the server
// accepts every client mode (auto / packet-up / stream-up / stream-one)
// when its own mode is unset. Outbound is freedom unless the test-only
// XRAY_HTTP_PROXY knob is set.
function buildConfig(wsPort, xhttpPort) {
  return {
    log: { loglevel: 'warning' },
    inbounds: [
      {
        port: wsPort,
        listen: '127.0.0.1',
        protocol: 'vless',
        settings: { clients: [{ id: UUID }], decryption: 'none' },
        streamSettings: { network: 'ws', wsSettings: { path: WS_PATH } },
      },
      {
        port: xhttpPort,
        listen: '127.0.0.1',
        protocol: 'vless',
        settings: { clients: [{ id: UUID }], decryption: 'none' },
        streamSettings: { network: 'xhttp', xhttpSettings: { path: XHTTP_PATH } },
      },
    ],
    outbounds: [XRAY_HTTP_PROXY ? httpProxyOutbound(XRAY_HTTP_PROXY) : { protocol: 'freedom', tag: 'direct' }],
  };
}

function freePort() {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.once('error', reject);
    s.listen(0, '127.0.0.1', () => {
      const p = s.address().port;
      s.close(() => resolve(p));
    });
  });
}

function startXray(bin, configPath) {
  const child = spawn(bin, ['run', '-config', configPath], { stdio: ['ignore', 'pipe', 'pipe'] });
  child.stdout.on('data', (d) => process.stdout.write('[xray] ' + d));
  child.stderr.on('data', (d) => process.stderr.write('[xray] ' + d));
  child.on('error', (e) => {
    logErr('FATAL: failed to spawn xray:', e.message);
    process.exit(1);
  });
  child.on('exit', (code, signal) => {
    logErr(`xray exited (code=${code} signal=${signal}); wrapper exiting`);
    process.exit(code ?? 1);
  });
  return child;
}

async function waitForPort(port, timeoutMs = 15000) {
  const start = Date.now();
  for (;;) {
    const ok = await new Promise((resolve) => {
      const s = net.connect(port, '127.0.0.1');
      s.once('connect', () => {
        s.end();
        resolve(true);
      });
      s.once('error', () => resolve(false));
    });
    if (ok) return;
    if (Date.now() - start > timeoutMs) {
      throw new Error(`xray did not start listening on 127.0.0.1:${port} within ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, 200));
  }
}

// --- HTTP front door on $PORT ------------------------------------------
// A real HTTP server (not the raw-TCP sniffer of the WS wrapper): XHTTP
// needs proper request parsing (method, path, headers, chunked and
// streaming bodies), which Node's parser does correctly. Bodies are
// never buffered — everything streams.

const HOP_BY_HOP = new Set([
  'connection',
  'keep-alive',
  'proxy-connection',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
]);

function filterHeaders(headers) {
  const out = {};
  for (const [k, v] of Object.entries(headers)) {
    if (!HOP_BY_HOP.has(k.toLowerCase())) out[k] = v;
  }
  return out;
}

function pathnameOf(req) {
  try {
    return new URL(req.url, 'http://127.0.0.1').pathname;
  } catch {
    return null;
  }
}

function isXhttpPath(pathname) {
  if (pathname === null) return false;
  return pathname === XHTTP_PATH || pathname.startsWith(XHTTP_PATH + '/');
}

// Reuse one keep-alive agent for upstream requests so the many small
// XHTTP upload POSTs do not each pay a new TCP handshake.
const xhttpAgent = new http.Agent({ keepAlive: true, maxSockets: 200 });

function proxyXhttp(req, res, xhttpPort) {
  const id = ++xhttpSeq;
  const headers = filterHeaders(req.headers);
  // The xray client itself sends `Connection: close` on every XHTTP
  // request (each session is a dedicated connection). The proxy must
  // preserve that: Go's server does NOT answer a stream-one XHTTP
  // request that arrives with `Connection: keep-alive` — it holds the
  // response until the connection closes (verified by A/B test: identical
  // bytes, `close` -> 200 in 72ms, `keep-alive` -> no response in 6s+).
  // Node's keep-alive agent would otherwise substitute `keep-alive`
  // (filterHeaders strips the client's `close`), hanging every
  // stream-one session and stalling packet-up uploads. So force `close`
  // upstream; the socket is per-session and is not reused anyway.
  headers['Connection'] = 'close';
  debug(`xhttp #${id} ${req.method} ${req.url} -> 127.0.0.1:${xhttpPort}`);
  const proxyReq = http.request({
    host: '127.0.0.1',
    port: xhttpPort,
    method: req.method,
    path: req.url, // origin-form path + query, forwarded unchanged
    headers,
    agent: xhttpAgent,
  });
  proxyReq.setTimeout(0);
  req.socket.setTimeout(0);
  let responded = false;
  proxyReq.on('response', (proxyRes) => {
    responded = true;
    debug(`xhttp #${id} response status=${proxyRes.statusCode}`);
    // Forward status + headers (minus hop-by-hop). Keep a content-length
    // when upstream sent one; when upstream streams chunked, Node
    // re-chunks the piped body automatically.
    res.writeHead(proxyRes.statusCode, filterHeaders(proxyRes.headers));
    proxyRes.pipe(res);
    proxyRes.on('end', () => debug(`xhttp #${id} response end`));
    proxyRes.on('error', (e) => debug(`xhttp #${id} response error: ${e.message}`));
  });
  proxyReq.on('error', (e) => {
    log(`xhttp #${id} proxy error:`, e.message);
    if (!responded && !res.headersSent) {
      res.writeHead(502, { 'Content-Type': 'text/plain', 'Content-Length': '11', Connection: 'close' });
      res.end('Bad Gateway');
    } else {
      res.destroy();
    }
  });
  // If the client aborts mid-stream, abort the upstream request too.
  // (res 'close' also fires on normal completion, when proxyReq is
  // already finished and destroy() is a no-op.)
  res.on('close', () => proxyReq.destroy());
  req.pipe(proxyReq);
}

function handleUpgrade(req, socket, head, wsPort) {
  const upgrade = (req.headers.upgrade || '').toLowerCase();
  const connection = (req.headers.connection || '').toLowerCase();
  if (upgrade !== 'websocket' || !connection.includes('upgrade')) {
    socket.destroy();
    return;
  }
  socket.setTimeout(0);
  const upstream = net.connect(wsPort, '127.0.0.1');
  upstream.once('connect', () => {
    // Node consumed the original handshake bytes, so rebuild the
    // upgrade request from the parsed headers (rawHeaders preserves
    // original order and casing) and forward it, then pipe raw.
    const lines = [`${req.method} ${req.url} HTTP/${req.httpVersion}`];
    for (let i = 0; i < req.rawHeaders.length; i += 2) {
      lines.push(`${req.rawHeaders[i]}: ${req.rawHeaders[i + 1]}`);
    }
    upstream.write(lines.join('\r\n') + '\r\n\r\n');
    if (head && head.length) upstream.write(head);
    socket.pipe(upstream);
    upstream.pipe(socket);
  });
  upstream.on('error', (e) => {
    log('ws upstream error:', e.message);
    socket.destroy();
  });
  socket.on('error', () => upstream.destroy());
  upstream.on('close', () => socket.destroy());
  socket.on('close', () => upstream.destroy());
}

function okResponse(res) {
  res.writeHead(200, { 'Content-Type': 'text/plain', 'Content-Length': '2', Connection: 'close' });
  res.end('ok');
}

function startFront(wsPort, xhttpPort) {
  const server = http.createServer((req, res) => {
    debug(`http ${req.method} ${req.url}`);
    if (isXhttpPath(pathnameOf(req))) {
      proxyXhttp(req, res, xhttpPort);
      return;
    }
    okResponse(res);
  });
  // XHTTP download streams hold the response open for the life of the
  // connection; Node's default 300 s request timeout would kill them
  // mid-stream, so it is disabled. headersTimeout stays at its default
  // (headers always arrive promptly).
  server.requestTimeout = 0;
  server.timeout = 0;
  server.on('upgrade', (req, socket, head) => handleUpgrade(req, socket, head, wsPort));
  server.on('connection', (socket) => {
    debug(`new tcp connection from ${socket.remoteAddress}:${socket.remotePort}`);
    socket.on('error', (e) => debug(`tcp socket error: ${e.message}`));
  });
  server.on('checkContinue', (req, res) => {
    res.writeContinue();
    if (isXhttpPath(pathnameOf(req))) proxyXhttp(req, res, xhttpPort);
    else okResponse(res);
  });
  server.on('clientError', (err, socket) => {
    log('clientError:', err.message);
    socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
  });
  server.on('error', (e) => {
    logErr('FATAL: front server error:', e.message);
    process.exit(1);
  });
  server.listen(PORT, '::', () => {
    log(
      `listening on [::]:${PORT} (WS ${WS_PATH} -> 127.0.0.1:${wsPort}, ` +
        `XHTTP ${XHTTP_PATH} -> 127.0.0.1:${xhttpPort}, other HTTP -> 200)`,
    );
  });
  return server;
}

async function main() {
  log(
    `starting (node=${process.version} PORT=${PORT} WS_PATH=${WS_PATH} ` +
      `XHTTP_PATH=${XHTTP_PATH} xray=${XRAY_VERSION})`,
  );
  log('cwd:', process.cwd());
  log('__dirname:', __dirname);
  await logDirectoryContents();
  // Listen-first: pick the loopback ports and bind $PORT *before* the
  // xray binary is ensured (the buildpack-path runtime download can take
  // a while). The platform health check hits $PORT during startup; if
  // the front only starts after xray is up, the check times out and the
  // deploy fails (Eurobase first round). Early XHTTP/WS requests get a
  // 502 until xray is listening, but plain HTTP already answers "ok".
  const wsPort = process.env.XRAY_PORT ? parseInt(process.env.XRAY_PORT, 10) : await freePort();
  const xhttpPort = process.env.XRAY_XHTTP_PORT
    ? parseInt(process.env.XRAY_XHTTP_PORT, 10)
    : await freePort();
  for (const [name, value] of [
    ['XRAY_PORT', wsPort],
    ['XRAY_XHTTP_PORT', xhttpPort],
  ]) {
    if (!Number.isInteger(value) || value <= 0 || value > 65535) {
      logErr(`FATAL: invalid ${name}:`, value);
      process.exit(1);
    }
  }
  startFront(wsPort, xhttpPort);
  const bin = await ensureXray();
  const config = buildConfig(wsPort, xhttpPort);
  let configPath = CONFIG_PATH;
  try {
    await fs.promises.writeFile(configPath, JSON.stringify(config, null, 2));
  } catch {
    configPath = path.join(os.tmpdir(), 'xray-config.json');
    await fs.promises.writeFile(configPath, JSON.stringify(config, null, 2));
  }
  log('xray config written to', configPath);
  const child = startXray(bin, configPath);
  const shutdown = () => {
    try {
      child.kill('SIGTERM');
    } catch {
      /* ignore */
    }
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
  await waitForPort(wsPort);
  await waitForPort(xhttpPort);
  log(`xray is listening on 127.0.0.1:${wsPort} (WS) and 127.0.0.1:${xhttpPort} (XHTTP)`);
}

main().catch((e) => {
  logErr('FATAL:', e.message);
  process.exit(1);
});
