/* Pocket Mind — website platform.
   Gives the app the same window.claude.use() capabilities it has inside Claude:
   db (local-first IndexedDB + sync to a private GitHub repo), user, assets (photos/files), sample (Claude via your API key).
   Keys live only in this browser (localStorage). Nothing personal is ever written to the public site repo. */
(function () {
  "use strict";
  const LS = {
    get(k, d = "") { try { return localStorage.getItem("pm." + k) ?? d; } catch (e) { return d; } },
    set(k, v) { try { v == null || v === "" ? localStorage.removeItem("pm." + k) : localStorage.setItem("pm." + k, v); } catch (e) {} }
  };
  const cfg = () => ({ ghToken: LS.get("ghToken"), repo: LS.get("repo"), apiKey: LS.get("apiKey"), model: LS.get("model", "claude-sonnet-5-5") });
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const hex = n => Array.from(crypto.getRandomValues(new Uint8Array(n)), b => b.toString(16).padStart(2, "0")).join("");
  const syncState = { status: "local only", last: 0, pending: 0, error: "" };
  const listeners = new Set();
  const onSync = () => listeners.forEach(f => { try { f(syncState); } catch (e) {} });

  /* ---------------- IndexedDB ---------------- */
  let dbp = null;
  function idb() {
    if (!dbp) dbp = new Promise((res, rej) => {
      const r = indexedDB.open("pocket-mind-web", 1);
      r.onupgradeneeded = () => { const d = r.result; d.createObjectStore("docs"); d.createObjectStore("blobs"); d.createObjectStore("meta"); };
      r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error);
    });
    return dbp;
  }
  async function tx(store, mode, fn) {
    const d = await idb();
    return new Promise((res, rej) => { const t = d.transaction(store, mode); const r = fn(t.objectStore(store)); t.oncomplete = () => res(r && "result" in r ? r.result : undefined); t.onerror = t.onabort = () => rej(t.error); });
  }
  const idbGet = (s, k) => tx(s, "readonly", st => st.get(k));
  const idbPut = (s, k, v) => tx(s, "readwrite", st => st.put(v, k));
  const idbAll = async s => { const d = await idb(); return new Promise((res, rej) => { const out = []; const t = d.transaction(s); const c = t.objectStore(s).openCursor(); c.onsuccess = () => { const x = c.result; if (x) { out.push([x.key, x.value]); x.continue(); } }; t.oncomplete = () => res(out); t.onerror = () => rej(t.error); }); };

  /* ---------------- document store (in-memory cache + IndexedDB) ----------------
     Each record: { data, updatedAt, deleted } keyed by full path. Paths under data/users/<uid>/ use uid "me". */
  const docs = new Map();
  let loaded = null;
  const load = () => loaded || (loaded = idbAll("docs").then(all => { all.forEach(([k, v]) => docs.set(k, v)); }));
  const subs = new Set();
  let notifyQ = 0;
  const notify = () => { if (notifyQ) return; notifyQ = setTimeout(() => { notifyQ = 0; subs.forEach(s => s()); }, 15); };
  const clone = v => (v == null ? v : JSON.parse(JSON.stringify(v)));
  const snapDoc = p => { const r = docs.get(p); const live = r && !r.deleted; return { id: p.split("/").pop(), exists: !!live, data: () => (live ? clone(r.data) : undefined), metadata: { fromCache: false, hasPendingWrites: false } }; };
  const isKid = (col, p) => p.startsWith(col + "/") && !p.slice(col.length + 1).includes("/");
  async function write(p, data, del) {
    await load();
    const rec = { data: del ? null : clone(data), updatedAt: Date.now() + Math.random() / 1000, deleted: !!del };
    docs.set(p, rec); await idbPut("docs", p, rec); markDirty(p); notify();
  }
  function docRef(p) {
    return {
      id: p.split("/").pop(), path: p,
      async get() { await load(); return snapDoc(p); },
      async set(d) { if (!d || typeof d !== "object") throw { code: "invalid_argument" }; await write(p, d); },
      async update(d) { await load(); const r = docs.get(p); if (!r || r.deleted) throw { code: "invalid_argument", message: "missing document" }; await write(p, Object.assign(clone(r.data), clone(d))); },
      async delete() { await write(p, null, true); },
      async acquire() { return { acquired: true }; },
      onSnapshot(next, err) { let last; const fn = () => { const s = snapDoc(p); const sig = JSON.stringify(s.exists ? s.data() : null); if (sig !== last) { last = sig; next(s); } }; subs.add(fn); load().then(fn, e => err && err({ code: "unavailable" })); return () => subs.delete(fn); },
      collection: c => colRef(p + "/" + c)
    };
  }
  function runQ(q) {
    let rows = [...docs.entries()].filter(([k, r]) => isKid(q.col, k) && !r.deleted).map(([k]) => snapDoc(k));
    for (const [f, op, v] of q.where || []) rows = rows.filter(s => { const x = s.data()[f]; return op === "==" ? x === v : op === "!=" ? x !== v : op === "<" ? x < v : op === "<=" ? x <= v : op === ">" ? x > v : op === ">=" ? x >= v : op === "in" ? v.includes(x) : op === "array-contains" ? (x || []).includes(v) : true; });
    if (q.order) { const { f, d } = q.order; rows.sort((a, b) => { const x = a.data()[f], y = b.data()[f]; return (d === "desc" ? -1 : 1) * ((x > y) - (x < y)); }); }
    if (q.lim) rows = rows.slice(0, q.lim);
    return { docs: rows, size: rows.length, empty: !rows.length, docChanges: () => [], metadata: { fromCache: false, hasPendingWrites: false } };
  }
  function query(q) {
    return {
      where: (f, op, v) => query({ ...q, where: [...(q.where || []), [f, op, v]] }),
      orderBy: (f, d = "asc") => query({ ...q, order: { f, d } }),
      limit: n => query({ ...q, lim: n }),
      async get() { await load(); return runQ(q); },
      onSnapshot(next, err) { let last; const fn = () => { const s = runQ(q); const sig = s.docs.map(d => d.id + ":" + (docs.get(q.col + "/" + d.id) || {}).updatedAt).join(","); if (sig !== last) { last = sig; next(s); } }; subs.add(fn); load().then(fn, () => err && err({ code: "unavailable" })); return () => subs.delete(fn); }
    };
  }
  function colRef(c) { return Object.assign(query({ col: c }), { path: c, doc: id => docRef(c + "/" + (id || hex(10))), add: async d => { const r = docRef(c + "/" + hex(10)); await r.set(d); return r; } }); }
  const dbApi = { doc: docRef, collection: colRef };

  /* ---------------- assets (photos and files) ---------------- */
  const urlCache = new Map();
  function sniff(bytes) {
    const b = new Uint8Array(bytes.slice(0, 8));
    if (b[0] === 0xff && b[1] === 0xd8) return "image/jpeg";
    if (b[0] === 0x89 && b[1] === 0x50) return "image/png";
    if (b[0] === 0x47 && b[1] === 0x49) return "image/gif";
    if (b[0] === 0x52 && b[1] === 0x49 && b[8 - 4] === 0x57) return "image/webp";
    if (b[0] === 0x25 && b[1] === 0x50) return "application/pdf";
    return "text/plain";
  }
  async function assetBlob(id) {
    const rec = await idbGet("blobs", id);
    if (rec && rec.blob) return rec.blob;
    const { ghToken, repo } = cfg();
    if (!ghToken || !repo) throw new Error("This photo is on another device and sync is not set up here.");
    const r = await gh(`/repos/${repo}/contents/media/${id}`, { headers: { Accept: "application/vnd.github.raw" } }, true);
    const buf = await r.arrayBuffer();
    const blob = new Blob([buf], { type: sniff(buf) });
    await idbPut("blobs", id, { blob, synced: true, type: blob.type });
    return blob;
  }
  async function assetURL(id) {
    if (urlCache.has(id)) return urlCache.get(id);
    const p = assetBlob(id).then(b => URL.createObjectURL(b)).catch(() => { urlCache.delete(id); return ""; });
    urlCache.set(id, p); return p;
  }
  const assetsApi = {
    async upload(blob, opts = {}) {
      if (!blob || !blob.size) throw { code: "invalid_request" };
      if (blob.size > 20 * 1024 * 1024) throw { code: "too_large" };
      const id = hex(16);
      await idbPut("blobs", id, { blob, synced: false, type: opts.type || blob.type });
      mediaQueue.add(id); kickSync();
      return { id, url: "", sizeBytes: blob.size, contentType: opts.type || blob.type };
    },
    async list() { const all = await idbAll("blobs"); return { assets: all.map(([id, v]) => ({ id, url: "", contentType: v.type, sizeBytes: v.blob ? v.blob.size : 0, createdAt: "" })), usage: { files: all.length, bytes: 0, maxFiles: 1e9, maxBytes: 1e12 } }; },
    async delete(id) { await tx("blobs", "readwrite", s => s.delete(id)); return { deleted: true }; }
  };

  /* ---------------- Claude (your Anthropic API key) ---------------- */
  const toB64 = blob => new Promise((res, rej) => { const r = new FileReader(); r.onload = () => res(String(r.result).split(",")[1]); r.onerror = rej; r.readAsDataURL(blob); });
  function parseJSON(text) {
    try { return JSON.parse(text); } catch (e) {}
    const f = text.match(/```(?:json)?\s*([\s\S]*?)```/); if (f) { try { return JSON.parse(f[1]); } catch (e) {} }
    const a = Math.min(...["{", "["].map(c => text.indexOf(c)).filter(i => i >= 0)), b = Math.max(text.lastIndexOf("}"), text.lastIndexOf("]"));
    if (a >= 0 && b > a) { try { return JSON.parse(text.slice(a, b + 1)); } catch (e) {} }
    throw { code: "invalid_json", message: "Reply was not JSON", text };
  }
  async function callClaude(input, opts = {}, wantJson) {
    const { apiKey, model } = cfg();
    if (!apiKey) throw { code: "not_granted", message: "Add your Anthropic API key in Settings." };
    const msgs = typeof input === "string" ? [{ role: "user", content: input }] : input.map(m => ({ role: m.role, content: m.content }));
    const last = msgs[msgs.length - 1];
    const imgs = opts.images ? (opts.images instanceof Blob ? [opts.images] : [...opts.images]) : [];
    const parts = [];
    for (const b of imgs) { let mt = b.type; if (!/^image\/(jpeg|png|gif|webp)$/.test(mt)) mt = sniff(await b.slice(0, 8).arrayBuffer()); if (!/^image\//.test(mt)) continue; parts.push({ type: "image", source: { type: "base64", media_type: mt, data: await toB64(b) } }); }
    last.content = [...parts, { type: "text", text: (wantJson ? "Your reply will be machine-parsed. " : "") + last.content }];
    let res;
    try {
      res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST", signal: opts.signal,
        headers: { "content-type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01", "anthropic-dangerous-direct-browser-access": "true" },
        body: JSON.stringify({ model: opts.model || model, max_tokens: 4096, messages: msgs })
      });
    } catch (e) { if (e && e.name === "AbortError") throw { code: "cancelled", message: "cancelled" }; throw { code: "upstream_error", message: String(e) }; }
    if (!res.ok) {
      let msg = ""; try { msg = (await res.json()).error.message; } catch (e) {}
      const code = res.status === 401 || res.status === 403 ? "not_granted" : res.status === 429 ? "rate_limited" : res.status === 400 && /image/i.test(msg) ? "image_rejected" : res.status === 400 ? "invalid_request" : "upstream_error";
      throw { code, message: msg || "HTTP " + res.status };
    }
    const j = await res.json();
    const text = (j.content || []).filter(c => c.type === "text").map(c => c.text).join("");
    if (!text.trim()) throw { code: "empty_completion", message: "empty" };
    if (opts.onText) try { opts.onText({ text, delta: text }); } catch (e) {}
    return { text, truncated: j.stop_reason === "max_tokens", modelTierApplied: "default" };
  }
  const sampleApi = (input, opts) => callClaude(input, opts || {}, false);
  sampleApi.json = async (input, opts) => { const r = await callClaude(input, opts || {}, true); return parseJSON(r.text); };
  sampleApi.limits = async () => ({ maxPromptBytes: 262144, images: { maxCount: 5, maxInputBytes: 20e6, mediaTypes: ["image/jpeg", "image/png", "image/gif", "image/webp"] } });

  /* ---------------- GitHub sync (private data repo) ----------------
     Layout: data/<collection>/<YYYY-MM>.json  = { "<docPath>": {data, updatedAt, deleted} }
             media/<assetId>                   = the photo or file bytes                        */
  async function gh(path, init = {}, raw) {
    const { ghToken } = cfg();
    const r = await fetch("https://api.github.com" + path, { ...init, headers: { Authorization: "Bearer " + ghToken, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", ...(init.headers || {}) } });
    if (!r.ok && !(raw && r.ok)) { let m = ""; try { m = (await r.json()).message; } catch (e) {} const err = new Error(m || "GitHub " + r.status); err.status = r.status; throw err; }
    return raw ? r : r.status === 204 ? null : r.json();
  }
  const b64utf8 = s => btoa(unescape(encodeURIComponent(s)));
  const utf8b64 = s => decodeURIComponent(escape(atob(s.replace(/\n/g, ""))));
  const bucketOf = p => {
    if (!p.includes("/mind/")) return "misc";
    const rest = p.split("/mind/")[1], col = rest.includes("/") ? rest.split("/")[0] : "meta";
    if (col === "system") return null; // local-only diagnostics
    const r = docs.get(p), ts = r && r.data && r.data.ts;
    const m = ts ? new Date(ts).toISOString().slice(0, 7) : "all";
    return col === "ideas" || col === "dumps" ? `${col}/${m}` : col;
  };
  const dirty = new Set(), mediaQueue = new Set();
  function markDirty(p) { const b = bucketOf(p); if (b) { dirty.add(b); (meta.dirtyBuckets = meta.dirtyBuckets || {})[b] = 1; saveMeta(); kickSync(); } }
  let meta = { shas: {}, dirtyBuckets: {} };
  const metaReady = idbGet("meta", "sync").then(m => { if (m) meta = m; Object.keys(meta.dirtyBuckets || {}).forEach(b => dirty.add(b)); }).catch(() => {});
  const saveMeta = () => idbPut("meta", "sync", meta).catch(() => {});
  let syncTimer = 0, syncing = false;
  let firstKick = 0;
  function kickSync(ms = 2500) { const now = Date.now(); if (!firstKick) firstKick = now; clearTimeout(syncTimer); const wait = Math.max(0, Math.min(ms, firstKick + 8000 - now)); syncTimer = setTimeout(() => { firstKick = 0; syncNow(); }, wait); }
  function mergeInto(remote) {
    let changed = false;
    for (const [p, r] of Object.entries(remote || {})) { const l = docs.get(p); if (!l || (r.updatedAt || 0) > (l.updatedAt || 0)) { docs.set(p, r); idbPut("docs", p, r).catch(() => {}); changed = true; } }
    if (changed) notify();
    return changed;
  }
  function bucketContent(b) { const out = {}; for (const [p, r] of docs) if (bucketOf(p) === b) out[p] = r; return out; }
  async function pushBucket(b) {
    const { repo } = cfg(), path = `data/${b}.json`;
    for (let attempt = 0; attempt < 3; attempt++) {
      let sha = null, remote = {};
      try {
        const f = await gh(`/repos/${repo}/contents/${path}`); sha = f.sha;
        const text = f.content ? utf8b64(f.content) : await (await gh(`/repos/${repo}/contents/${path}`, { headers: { Accept: "application/vnd.github.raw" } }, true)).text();
        remote = JSON.parse(text || "{}");
      } catch (e) { if (e.status !== 404) throw e; }
      mergeInto(remote);
      const body = JSON.stringify(bucketContent(b));
      try { const put = await gh(`/repos/${repo}/contents/${path}`, { method: "PUT", body: JSON.stringify({ message: "pocket mind: " + b, content: b64utf8(body), ...(sha ? { sha } : {}) }) }); meta.shas[path] = put.content.sha; return; }
      catch (e) { if (e.status === 409 || e.status === 422) { await sleep(600 * (attempt + 1)); continue; } throw e; }
    }
    throw new Error("Could not save " + b + " after retries");
  }
  async function pushMedia(id) {
    const { repo } = cfg(); const rec = await idbGet("blobs", id); if (!rec || rec.synced) return;
    try { await gh(`/repos/${repo}/contents/media/${id}`, { method: "PUT", body: JSON.stringify({ message: "pocket mind: file", content: await toB64(rec.blob) }) }); }
    catch (e) { if (e.status !== 422) throw e; } // 422 = already there
    rec.synced = true; await idbPut("blobs", id, rec);
  }
  async function pull() {
    const { repo } = cfg();
    const tree = await gh(`/repos/${repo}/git/trees/HEAD?recursive=1`);
    for (const t of tree.tree || []) {
      if (t.type !== "blob" || !/^data\/.+\.json$/.test(t.path) || meta.shas[t.path] === t.sha) continue;
      const r = await gh(`/repos/${repo}/contents/${t.path}`, { headers: { Accept: "application/vnd.github.raw" } }, true);
      try { mergeInto(JSON.parse(await r.text())); meta.shas[t.path] = t.sha; } catch (e) {}
    }
    await saveMeta();
  }
  async function syncNow() {
    const { ghToken, repo } = cfg();
    if (!ghToken || !repo) { syncState.status = "local only"; onSync(); return; }
    if (syncing) { kickSync(3000); return; }
    syncing = true; syncState.status = "syncing"; onSync();
    try {
      await metaReady; await load();
      // media first so other devices never see an idea before its photo
      if (!mediaQueue.size) { (await idbAll("blobs")).forEach(([id, v]) => { if (!v.synced) mediaQueue.add(id); }); }
      for (const id of [...mediaQueue]) { syncState.pending = mediaQueue.size + dirty.size; onSync(); await pushMedia(id); mediaQueue.delete(id); }
      for (const b of [...dirty]) { await pushBucket(b); dirty.delete(b); delete meta.dirtyBuckets[b]; await saveMeta(); }
      await pull();
      syncState.status = "synced"; syncState.last = Date.now(); syncState.error = ""; syncState.pending = mediaQueue.size + dirty.size;
    } catch (e) {
      syncState.status = "sync paused"; syncState.error = e.status === 401 ? "GitHub token was refused. Check Settings." : e.status === 404 ? "Data repo not found. Check the name and token access in Settings." : String(e.message || e);
      kickSync(30000);
    } finally { syncing = false; onSync(); }
  }
  document.addEventListener("visibilitychange", () => { if (document.visibilityState === "visible") kickSync(500); });
  setInterval(() => { if (document.visibilityState === "visible") kickSync(10); }, 90000);

  /* ---------------- settings screen ---------------- */
  function openSettings(first) {
    const c = cfg();
    const wrap = document.createElement("div");
    wrap.id = "pmSettings";
    wrap.setAttribute("role", "dialog"); wrap.setAttribute("aria-modal", "true");
    wrap.style.cssText = "position:fixed;inset:0;z-index:50;background:rgba(10,10,10,.5);display:flex;align-items:flex-end;justify-content:center";
    const field = (id, label, value, type, help) => `<label for="${id}" style="display:flex;flex-direction:column;gap:6px;font-size:14px">${label}<input id="${id}" type="${type}" autocomplete="off" spellcheck="false" value="${(value || "").replace(/"/g, "&quot;")}" style="font:inherit;padding:12px 14px;border-radius:12px;border:1px solid var(--rule);background:var(--surface);color:var(--ink)"><span style="font-size:12.5px;color:var(--muted)">${help}</span></label>`;
    wrap.innerHTML = `<div style="background:var(--ground);color:var(--ink);width:100%;max-width:520px;max-height:92%;overflow:auto;border-radius:20px 20px 0 0;padding:22px 18px calc(24px + env(safe-area-inset-bottom,0px));display:flex;flex-direction:column;gap:16px;font-family:var(--font)">
      <div style="display:flex;justify-content:space-between;align-items:baseline"><h2 style="margin:0;font-weight:500;font-size:20px">${first ? "Set up Pocket Mind" : "Settings and sync"}</h2><button type="button" id="pmClose" style="border:0;background:transparent;color:var(--muted);font:inherit;min-height:44px">Close</button></div>
      <p style="margin:0;font-size:14px;color:var(--muted)">Keys stay only in this browser. Your ideas and photos sync to your private repo so your phone and computer see the same mind.</p>
      ${field("pmKey", "Anthropic API key", c.apiKey, "password", 'For reading dumps with Claude. Create one at console.anthropic.com → API keys. Pay per use.')}
      ${field("pmRepo", "Private data repo", c.repo || "", "text", 'Like <b>your-name/pocket-mind-data</b>. Keep it private.')}
      ${field("pmTok", "GitHub token", c.ghToken, "password", 'Fine-grained token with access to only that repo, permission "Contents: Read and write". GitHub → Settings → Developer settings → Fine-grained tokens.')}
      ${field("pmModel", "Claude model", c.model, "text", "Default claude-sonnet-5-5. Use claude-haiku-5-5 to spend less.")}
      <p id="pmMsg" role="status" aria-live="polite" style="margin:0;font-size:14px;color:var(--muted)"></p>
      <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end">
        ${first ? '<button type="button" id="pmSkip" style="font:inherit;min-height:44px;padding:10px 14px;border-radius:12px;border:1px solid var(--rule);background:var(--surface);color:var(--ink)">Try without sync</button>' : ""}
        <button type="button" id="pmSave" style="font:inherit;font-weight:500;min-height:44px;padding:10px 18px;border-radius:12px;border:0;background:var(--warm);color:var(--warm-ink)">Save and check</button></div>
      <p style="margin:0;font-size:13px;color:var(--muted)">Sync: <span id="pmSync">${syncState.status}</span></p></div>`;
    document.body.appendChild(wrap);
    const $ = s => wrap.querySelector(s), msg = t => ($("#pmMsg").textContent = t);
    const close = () => wrap.remove();
    $("#pmClose").onclick = () => { LS.set("setupSeen", "1"); close(); };
    if ($("#pmSkip")) $("#pmSkip").onclick = () => { LS.set("setupSeen", "1"); close(); };
    $("#pmSave").onclick = async () => {
      const key = $("#pmKey").value.trim(), repo = $("#pmRepo").value.trim().replace(/^https?:\/\/github\.com\//, "").replace(/\.git$/, "").replace(/\/$/, ""), tok = $("#pmTok").value.trim(), model = $("#pmModel").value.trim() || "claude-sonnet-5-5";
      if (repo && !/^[\w.-]+\/[\w.-]+$/.test(repo)) { msg("The repo should look like your-name/pocket-mind-data."); return; }
      LS.set("apiKey", key); LS.set("repo", repo); LS.set("ghToken", tok); LS.set("model", model); LS.set("setupSeen", "1");
      msg("Checking…");
      const notes = [];
      if (repo && tok) {
        try { const r = await gh(`/repos/${repo}`); notes.push(r.private ? "GitHub: connected to your private repo." : "GitHub: connected, but this repo is PUBLIC. Make it private to keep your mind private."); kickSync(10); }
        catch (e) { notes.push(e.status === 401 ? "GitHub: token refused." : e.status === 404 ? "GitHub: repo not found, or the token can't see it." : "GitHub: " + e.message); }
      } else notes.push("GitHub: not set, so everything stays on this device only.");
      if (key) {
        try { await callClaude("Reply with the single word: ok", { model }); notes.push("Claude: working."); }
        catch (e) { notes.push("Claude: " + (e.code === "not_granted" ? "key refused." : e.message || e.code)); }
      } else notes.push("Claude: no key, so dumps are saved but not read.");
      msg(notes.join(" "));
      if ($("#pmSkip")) { $("#pmSkip").textContent = "Start"; $("#pmSkip").style.fontWeight = "500"; }
    };
    listeners.add(s => { const el = wrap.isConnected && $("#pmSync"); if (el) el.textContent = s.status + (s.error ? " · " + s.error : "") + (s.pending ? " · " + s.pending + " waiting" : ""); });
  }

  /* ---------------- expose ---------------- */
  const caps = {
    db: dbApi,
    user: { id: async () => "me", me: async () => ({ id: "me", name: "" }), isOwner: () => true, canEdit: () => true, can: () => true },
    sample: sampleApi,
    assets: assetsApi,
    permissions: { state: async n => (n === "sample" ? (cfg().apiKey ? "granted" : "prompt") : "granted"), request: async () => { openSettings(); return { sample: cfg().apiKey ? "granted" : "prompt" }; }, manage: async () => openSettings() }
  };
  window.claude = { use: async name => (name in caps ? caps[name] : null) };
  window.PM_PLATFORM = { name: "web", assetURL, assetBlob, openSettings, syncNow, onSync: f => { listeners.add(f); f(syncState); return () => listeners.delete(f); }, sync: syncState };
  window.addEventListener("DOMContentLoaded", () => {
    if (!LS.get("setupSeen")) setTimeout(() => openSettings(true), 300);
    kickSync(1500);
  });
})();
