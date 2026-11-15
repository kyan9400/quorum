"use strict";

const $ = (s, el = document) => el.querySelector(s);
const NS = "http://www.w3.org/2000/svg";
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
const fmt = (n) => (n == null ? "–" : Number(n).toLocaleString("en-US"));
const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();

function svg(tag, attrs = {}, parent) {
  const el = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  if (parent) parent.appendChild(el);
  return el;
}

function roleOf(n) {
  if (!n.up) return "down";
  if (!n.answered) return "quiet";
  return n.state; // leader | follower | candidate
}
const ROLE_LABEL = { leader: "leader", follower: "follower", candidate: "candidate", quiet: "starting", down: "killed" };

let toastTimer;
function toast(msg, err = false) {
  const t = $("#toast");
  t.textContent = msg;
  t.className = "toast show" + (err ? " err" : "");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (t.className = "toast"), 2600);
}

const tip = $("#tip");
function showTip(html, x, y) {
  tip.innerHTML = html;
  tip.style.display = "block";
  const w = tip.offsetWidth, h = tip.offsetHeight;
  let left = x + 14, top = y + 14;
  if (left + w > innerWidth - 8) left = x - w - 14;
  if (top + h > innerHeight - 8) top = y - h - 14;
  tip.style.left = Math.max(8, left) + "px";
  tip.style.top = Math.max(8, top) + "px";
}
const hideTip = () => (tip.style.display = "none");

// ===================================================================== tabs
let liveAvailable = true;
function route() {
  const [tab, arg] = (location.hash.slice(1) || (liveAvailable ? "live" : "runs")).split("/");
  const t = tab === "runs" || !liveAvailable ? "runs" : "live";
  for (const a of document.querySelectorAll(".tabs a")) a.setAttribute("aria-selected", a.dataset.tab === t);
  $("#view-live").hidden = t !== "live";
  $("#view-runs").hidden = t !== "runs";
  if (t === "live") startLive(); else stopLive();
  if (t === "runs") openRuns(arg);
}
addEventListener("hashchange", route);

// ===================================================================== live
let es = null, last = null;

function startLive() {
  if (es) return;
  es = new EventSource("api/live/stream");
  es.onmessage = (m) => { last = JSON.parse(m.data); renderLive(last); };
  es.onerror = () => { $("#net-line").innerHTML = '<span class="bad">reconnecting…</span>'; };
}
function stopLive() { if (es) { es.close(); es = null; } }

async function act(action, node = 0) {
  const buttons = document.querySelectorAll("[data-act], .mini");
  buttons.forEach((b) => (b.disabled = true));
  try {
    const r = await fetch("api/live/action", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action, node }) });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) toast(body.error || "that did not work", true);
  } catch { toast("the dashboard is not answering", true); }
  setTimeout(() => buttons.forEach((b) => (b.disabled = false)), 600);
}

document.addEventListener("click", (e) => {
  const b = e.target.closest("[data-act]");
  if (b) act(b.dataset.act, Number(b.dataset.node || 0));
});

function sideOf(groups, id) {
  if (!groups) return -1;
  for (let g = 0; g < groups.length; g++) if (groups[g].includes(id)) return g;
  return -1;
}

function renderLive(s) {
  for (const k of ["cut", "events", "windows", "pending", "recent", "rate"]) s[k] = s[k] || []; // Go sends null for empty
  $('[data-bind="autoheal"]').textContent = Math.round(s.auto_heal_s);
  const words = ["No", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight"];
  $('[data-bind="clients"]').textContent = words[s.clients] || s.clients;
  renderRing(s);
  renderNodes(s);
  renderVerdict(s);
  renderWindows(s);
  renderRate(s);
  renderEvents($("#events"), s.events.slice().reverse(), true);
  renderOps(s);
  const hi = $("#heal-in");
  hi.textContent = s.heal_in_s ? `auto-heal in ${Math.ceil(s.heal_in_s)} s` : "";
}

function ringPos(i, n) {
  const cx = 230, cy = 195, r = 128;
  const a = -Math.PI / 2 + (2 * Math.PI * i) / n;
  return [cx + r * Math.cos(a), cy + r * Math.sin(a)];
}

function renderRing(s) {
  const el = $("#ring");
  el.replaceChildren();
  const n = s.nodes.length;
  const cut = new Set(s.cut.map(([a, b]) => a + "-" + b));
  const leaders = s.nodes.filter((x) => x.answered && x.state === "leader");
  const top = leaders.reduce((m, x) => (!m || x.term > m.term ? x : m), null);
  const links = svg("g", {}, el);
  for (let a = 1; a <= n; a++) for (let b = a + 1; b <= n; b++) {
    let [x1, y1] = ringPos(a - 1, n), [x2, y2] = ringPos(b - 1, n);
    const ab = cut.has(a + "-" + b), ba = cut.has(b + "-" + a);
    const na = s.nodes[a - 1], nb = s.nodes[b - 1];
    const leads = (nd) => nd.answered && nd.state === "leader";
    let cls = "link";
    if (ab && ba) cls += " cut";
    else if (ab || ba) cls += " half";
    else if (na.up && nb.up && (leads(na) || leads(nb))) {
      cls += " flow";
      // the dashes run away from the leader, as its heartbeats do
      if (leads(nb) && !leads(na)) [x1, y1, x2, y2] = [x2, y2, x1, y1];
    }
    svg("line", { x1, y1, x2, y2, class: cls }, links);
  }
  const fill = { leader: css("--leader"), follower: "#1b2840", candidate: "#3a2c10", quiet: "#20262f", down: "#0f131a" };
  const stroke = { leader: css("--leader"), follower: css("--follower"), candidate: css("--candidate"), quiet: css("--quiet"), down: css("--faint") };
  for (const node of s.nodes) {
    const [x, y] = ringPos(node.id - 1, n);
    const role = roleOf(node);
    const g = svg("g", { class: "node " + role, transform: `translate(${x},${y})` }, el);
    if (top && node.id === top.id) svg("circle", { r: 34, class: "halo" }, g);
    svg("circle", { r: 34, class: "body", fill: fill[role], stroke: stroke[role] }, g);
    const ink = role === "leader" ? css("--leader-ink") : role === "down" ? css("--faint") : css("--text");
    svg("text", { y: -2, "text-anchor": "middle", class: "id", fill: ink }, g).textContent = "n" + node.id;
    svg("text", { y: 13, "text-anchor": "middle", class: "role", fill: role === "leader" ? ink : stroke[role] }, g).textContent = ROLE_LABEL[role];
    const below = y > 195 ? 52 : -44;
    const sub = svg("text", { y: below, "text-anchor": "middle", class: "sub" }, g);
    sub.textContent = node.answered ? `term ${node.term} · commit ${fmt(node.commit)}` : node.up ? "process up, not answering yet" : "process killed";
    if (s.groups && s.groups.length) {
      const side = sideOf(s.groups, node.id);
      const minority = side >= 0 && s.groups[side].length * 2 <= n;
      if (minority) svg("text", { y: below + (y > 195 ? 14 : -14), "text-anchor": "middle", class: "side" }, g).textContent = "cut off: no majority";
    }
  }
  const up = s.nodes.filter((x) => x.up).length;
  let line = top ? `<span class="good">leader n${top.id}</span>, term ${top.term}` : '<span class="bad">no leader</span>';
  line += ` · ${up}/${n} running`;
  if (s.groups && s.groups.length) line += ` · <span class="bad">partition ${s.groups.map((g) => "[" + g.join(" ") + "]").join(" | ")}</span>`;
  $("#net-line").innerHTML = line;
}

function renderNodes(s) {
  const tbody = $("#nodes tbody");
  // The bars show the recent end of the logs, where nodes differ: from a
  // little before the furthest-behind node's commit to the longest log.
  const answered = s.nodes.filter((x) => x.answered);
  const maxLast = Math.max(1, ...answered.map((x) => x.last_index || 0));
  const minCommit = answered.length ? Math.min(...answered.map((x) => x.commit || 0)) : 0;
  const from = Math.max(0, Math.min(minCommit, maxLast - 60) - 20);
  const leader = answered.filter((x) => x.state === "leader").sort((a, b) => b.term - a.term)[0];
  const pos = (v) => Math.max(0, Math.min(1, ((v || 0) - from) / (maxLast - from || 1)));
  const pct = (v) => (100 * pos(v)).toFixed(2) + "%";
  const span = (a, b) => (100 * Math.max(0, pos(b) - pos(a))).toFixed(2) + "%";
  $("#log-range").textContent = from > 0 ? `showing entries ${fmt(from)}–${fmt(maxLast)}` : "";
  tbody.innerHTML = s.nodes.map((node) => {
    const role = roleOf(node);
    const side = sideOf(s.groups, node.id);
    const minority = side >= 0 && s.groups[side].length * 2 <= s.nodes.length;
    let bar = '<div class="logbar">';
    if (node.answered) {
      const snap = node.snapshot_index || 0;
      bar += `<div class="snap" style="left:0;width:${pct(snap)}"></div>`;
      bar += `<div class="commit" style="left:${pct(Math.max(snap, from))};width:${span(Math.max(snap, from), node.commit)}"></div>`;
      bar += `<div class="tail" style="left:${pct(node.commit)};width:${span(node.commit, node.last_index)}"></div>`;
      const pr = leader && leader.progress && leader.progress[node.id];
      if (pr && node.id !== leader.id) bar += `<div class="match" style="left:calc(${pct(pr.match)} - 1px)" title="leader knows n${node.id} holds up to ${pr.match}"></div>`;
    }
    bar += "</div>";
    const btns = node.up
      ? `<button class="mini danger" data-act="kill" data-node="${node.id}">kill -9</button><button class="mini danger" data-act="isolate" data-node="${node.id}">isolate</button>`
      : `<button class="mini" data-act="start" data-node="${node.id}">restart</button>`;
    return `<tr>
      <td>n${node.id}</td>
      <td><span class="badge ${role}">${ROLE_LABEL[role]}</span>${minority ? '<span class="badge cutoff">minority</span>' : ""}</td>
      <td class="num">${node.answered ? node.term : "–"}</td>
      <td>${bar}</td>
      <td class="num">${node.answered ? fmt(node.commit) : "–"}</td>
      <td class="num">${node.answered ? fmt(node.last_index) : "–"}</td>
      <td class="num">${node.answered ? fmt(node.keys) : "–"}</td>
      <td class="num">${btns}</td></tr>`;
  }).join("");
}

function renderVerdict(s) {
  const t = s.totals;
  const bad = t.violations > 0;
  $("#verdict").innerHTML = `
    <div class="v"><span class="n ${bad ? "bad" : "good"}">${bad ? fmt(t.violations) : fmt(t.linearizable)}</span><span class="l">${bad ? "windows NOT linearizable" : "windows checked, all linearizable"}</span></div>
    <div class="v"><span class="n">${fmt(t.ops)}</span><span class="l">operations checked</span></div>
    <div class="v"><span class="n">${fmt(t.unknown)}</span><span class="l">gave up: outcome unknown</span></div>`;
}

function renderWindows(s) {
  const slots = innerWidth <= 600 ? 36 : 60;
  const done = s.windows.slice(-slots + s.pending.length);
  let html = "";
  const pad = slots - done.length - s.pending.length;
  for (let i = 0; i < pad; i++) html += '<div class="w"></div>';
  for (const w of done) {
    const cls = w.undecided ? "undecided" : w.linearizable ? "ok" : "bad";
    const verdict = w.undecided ? "checker ran out of time" : w.linearizable ? "linearizable" : "NOT linearizable";
    html += `<div class="w ${cls}${w.unknown ? " unk" : ""}" data-tip="window ${w.window} · ${verdict}|${w.ops} operations, ${w.unknown} unknown|checked in ${w.check_ms.toFixed(1)} ms"></div>`;
  }
  for (const p of s.pending) html += `<div class="w pending" data-tip="window ${p.window} · not checked yet|${p.ops} answered, ${p.in_flight} in flight"></div>`;
  $("#windows").innerHTML = html;
  const t = s.totals;
  $("#lin-sum").textContent = t.windows ? `${fmt(t.windows)} windows since the dashboard started` : "first window checks after ~15 s";
}

document.addEventListener("mousemove", (e) => {
  const w = e.target.closest && e.target.closest("[data-tip]");
  if (!w) { if (!e.target.closest || !e.target.closest(".timeline")) hideTip(); return; }
  const [h, ...rows] = w.dataset.tip.split("|");
  showTip(`<div class="h">${esc(h)}</div>${rows.map((r) => `<div>${esc(r)}</div>`).join("")}`, e.clientX, e.clientY);
});

function renderRate(s) {
  const el = $("#rate");
  el.replaceChildren();
  const W = 520, H = 150, pad = 4;
  const data = s.rate;
  const maxOps = Math.max(10, ...data.map((d) => d.ok + d.unknown));
  const lats0 = data.filter((d) => d.p50).map((d) => d.p50).sort((a, b) => a - b);
  const typical = lats0.length ? lats0[Math.floor(lats0.length / 2)] : 5;
  const maxLat = Math.max(20, typical * 4); // spikes above this are drawn at the top
  const bw = W / data.length;
  for (const f of [0.5, 1]) svg("line", { x1: 0, x2: W, y1: H - (H - pad) * f, y2: H - (H - pad) * f, stroke: css("--line"), "stroke-width": 1, "vector-effect": "non-scaling-stroke" }, el);
  data.forEach((d, i) => {
    const x = i * bw + 1;
    const hOk = ((H - pad) * d.ok) / maxOps, hU = ((H - pad) * d.unknown) / maxOps;
    if (d.ok) svg("rect", { x, width: bw - 2, y: H - hOk, height: hOk, fill: "rgba(107,143,214,0.75)" }, el);
    if (d.unknown) svg("rect", { x, width: bw - 2, y: H - hOk - hU, height: hU, fill: css("--bad") }, el);
  });
  const pts = data.map((d, i) => (d.p50 ? `${i * bw + bw / 2},${H - ((H - pad) * Math.min(d.p50, maxLat)) / maxLat}` : null));
  let path = "", pen = false;
  for (const p of pts) { if (p) { path += (pen ? "L" : "M") + p; pen = true; } else pen = false; }
  if (path) svg("path", { d: path, fill: "none", stroke: css("--candidate"), "stroke-width": 2, "vector-effect": "non-scaling-stroke" }, el);
  const recent = data.slice(-10);
  const ops = recent.reduce((a, d) => a + d.ok, 0) / recent.length;
  const lats = recent.filter((d) => d.p50).map((d) => d.p50);
  const lat = lats.length ? lats.sort((a, b) => a - b)[Math.floor(lats.length / 2)] : null;
  $("#rate-sum").textContent = `${ops.toFixed(1)} answered/s · median ${lat == null ? "–" : lat.toFixed(1) + " ms"} · bar scale ${maxOps}/s, line ${Math.round(maxLat)} ms`;
}

function renderEvents(ol, events, live) {
  if (!events.length) { ol.innerHTML = `<li class="empty">${live ? "Nothing yet. Break something." : "No events recorded."}</li>`; return; }
  ol.innerHTML = events.map((e) => `<li class="${esc(e.kind)}"><span class="t">${e.t.toFixed(1)} s</span><span class="x">${esc(e.text)}</span></li>`).join("");
}

function renderOps(s) {
  $("#ops tbody").innerHTML = s.recent.slice().reverse().map((o) => {
    const what = o.kind === "get" ? "get" : o.kind === "cas" ? `cas ∅→${o.value}` : `${o.kind} ${o.value}`;
    return `<tr><td>c${o.client}${o.pinned ? ` <span class="pin">→n${o.pinned}</span>` : ""}</td><td>${esc(what)}</td><td>${esc(o.key)}</td>
      <td class="${o.unknown ? "unk" : ""}">${o.unknown ? "unknown" : esc(o.out || "")}</td><td class="num">${o.ms.toFixed(1)}</td></tr>`;
  }).join("");
}

// ===================================================================== runs
let runList = null, current = null, view = null;

async function openRuns(name) {
  if (!runList) {
    runList = await (await fetch("api/runs")).json();
    $("#run-picker").innerHTML = (runList || []).map((r) => {
      const s = r.summary;
      return `<button class="chip" data-run="${esc(r.name)}"><b>${esc(r.name)}</b><small>seed ${s.seed} · ${fmt(s.operations)} ops · ${s.linearizable ? "linearizable" : "NOT linearizable"}</small></button>`;
    }).join("") || '<span class="muted">No recorded runs in the results directory.</span>';
  }
  if (!runList || !runList.length) return;
  if (!name || !runList.some((r) => r.name === name)) name = runList[0].name;
  for (const c of document.querySelectorAll(".chip")) c.setAttribute("aria-pressed", c.dataset.run === name);
  if (current && current.name === name) return;
  current = await (await fetch("api/runs/" + encodeURIComponent(name))).json();
  for (const k of ["events", "timeline", "buckets"]) current[k] = current[k] || [];
  view = null;
  renderRun();
}

document.addEventListener("click", (e) => {
  const c = e.target.closest(".chip");
  if (c) location.hash = "runs/" + c.dataset.run;
});

function renderRun() {
  const r = current, s = r.summary;
  const faults = Object.values(s.faults || {}).reduce((a, b) => a + b, 0);
  const commits = s.final_commit_indexes ? Object.keys(s.final_commit_indexes) : null;
  const agree = commits ? (commits.length === 1 ? fmt(commits[0]) : "disagree") : "–";
  const agreeLabel = commits && commits.length === 1 ? `commit index on all ${s.final_commit_indexes[commits[0]]} nodes after the final restart` : "commit index after the final restart";
  const stat = (n, l, cls = "") => `<div class="stat"><div class="n ${cls}">${n}</div><div class="l">${l}</div></div>`;
  $("#run-stats").innerHTML =
    stat(s.linearizable ? "yes" : "NO", "linearizable", s.linearizable ? "good" : "bad") +
    stat(fmt(s.operations), `operations, ${fmt(s.unknown)} unknown`) +
    stat(fmt(faults), Object.entries(s.faults || {}).map(([k, v]) => `${v} ${k}`).join(", ")) +
    stat(fmt(Math.round(s.operations / s.duration_s)), "operations per second") +
    stat(r.timeline.length ? fmt(leaderChanges(r)) : "–", "leaders elected") +
    stat(agree, agreeLabel, commits && commits.length === 1 ? "" : "bad");
  $("#run-title").textContent = `Timeline · ${r.name}`;
  renderEvents($("#run-events"), r.events || [], false);
  drawTimeline();
}

function leaderChanges(r) {
  const seen = new Set();
  for (const smp of r.timeline) smp.nodes.forEach((n, i) => { if (n.s === "leader") seen.add(n.term + ":" + i); });
  return seen.size;
}

// Segments of constant state per node, from the samples.
function segments(r) {
  const out = [];
  const T = r.timeline;
  if (!T.length) return out;
  const n = T[0].nodes.length;
  for (let i = 0; i < n; i++) {
    const segs = [];
    let cur = null;
    for (let k = 0; k < T.length; k++) {
      const p = T[k].nodes[i];
      const st = p.s === "" ? "quiet" : p.s;
      const term = p.term || 0;
      if (!cur || cur.s !== st || (st === "leader" && cur.term !== term)) {
        if (cur) cur.t1 = T[k].t;
        cur = { s: st, term, t0: T[k].t, t1: T[k].t };
        segs.push(cur);
      }
    }
    if (cur) cur.t1 = T[T.length - 1].t + 0.25;
    out.push(segs);
  }
  return out;
}

// Partition periods: from a partition event to the next heal or partition.
function partitions(r) {
  const out = [];
  let open = null;
  for (const e of r.events || []) {
    if (open && (e.kind === "heal" || e.kind === "partition" || e.kind === "restart-all")) { open.t1 = e.t; open = null; }
    if (e.kind === "partition") { open = { t0: e.t, t1: null, groups: e.groups, text: e.text }; out.push(open); }
  }
  if (open) open.t1 = r.timeline.length ? r.timeline[r.timeline.length - 1].t : open.t0 + 1;
  return out;
}

function drawTimeline() {
  const r = current;
  const host = $("#timeline");
  host.replaceChildren();
  const width = host.clientWidth || 1000;
  const segs = segments(r);
  const parts = partitions(r);
  const n = segs.length || r.summary.nodes || 5;
  const tEnd = Math.max(r.timeline.length ? r.timeline[r.timeline.length - 1].t + 0.25 : 0, ...r.buckets.map((b) => b.t + r.bucket_s));
  if (!view) view = [0, tEnd];
  const [v0, v1] = view;
  const L = 46, R = 12;
  const x = (t) => L + ((t - v0) / (v1 - v0)) * (width - L - R);
  const laneH = 22, laneGap = 6, top = 22;
  const lanesBottom = top + n * (laneH + laneGap);
  const opsTop = lanesBottom + 26, opsH = 90;
  const latTop = opsTop + opsH + 28, latH = 70;
  const H = latTop + latH + 26;
  const el = svg("svg", { width, height: H, viewBox: `0 0 ${width} ${H}` }, host);
  const defs = svg("defs", {}, el);
  const pat = svg("pattern", { id: "minority", width: 6, height: 6, patternUnits: "userSpaceOnUse", patternTransform: "rotate(45)" }, defs);
  svg("rect", { width: 2.2, height: 6, fill: "rgba(242,103,107,0.55)" }, pat);
  const clip = svg("clipPath", { id: "plot" }, defs);
  svg("rect", { x: L, y: 0, width: width - L - R, height: H }, clip);

  // time grid
  const span = v1 - v0;
  const step = [1, 2, 5, 10, 15, 30, 60].find((s) => span / s <= Math.max(4, Math.floor((width - L) / 90))) || 60;
  const grid = svg("g", {}, el);
  for (let t = Math.ceil(v0 / step) * step; t <= v1; t += step) {
    svg("line", { x1: x(t), x2: x(t), y1: top - 6, y2: H - 20, class: "grid-line" }, grid);
    svg("text", { x: x(t), y: H - 6, "text-anchor": "middle" }, grid).textContent = t + " s";
  }

  const plot = svg("g", { "clip-path": "url(#plot)" }, el);
  const color = { leader: css("--leader"), follower: "#3c5a94", candidate: css("--candidate"), quiet: "#5b6678", down: "#161c26" };
  svg("text", { x: L, y: top - 9, class: "row-title" }, el).textContent = "nodes";
  for (let i = 0; i < n; i++) {
    const y = top + i * (laneH + laneGap);
    svg("text", { x: L - 10, y: y + laneH / 2 + 4, "text-anchor": "end", class: "lane-label" }, el).textContent = "n" + (i + 1);
    svg("rect", { x: L, y, width: width - L - R, height: laneH, fill: "#0c1018", rx: 3 }, plot);
    for (const sg of segs[i] || []) {
      if (sg.t1 < v0 || sg.t0 > v1) continue;
      const x0 = x(sg.t0), w = Math.max(0.6, x(sg.t1) - x0);
      svg("rect", { x: x0, y: y + (sg.s === "down" ? 0 : 0), width: w, height: laneH, fill: color[sg.s] || "#333", stroke: sg.s === "down" ? "#2d3746" : "none", "stroke-dasharray": sg.s === "down" ? "3 3" : "" }, plot);
      if (sg.s === "leader" && w > 26) {
        svg("text", { x: x0 + w / 2, y: y + laneH / 2 + 4, "text-anchor": "middle", fill: css("--leader-ink"), "font-weight": 700 }, plot).textContent = (w > 60 ? "term " : "t") + sg.term;
      }
    }
    for (const p of parts) {
      const side = sideOf(p.groups, i + 1);
      const minority = side < 0 || p.groups[side].length * 2 <= n;
      if (!minority || p.t1 < v0 || p.t0 > v1) continue;
      svg("rect", { x: x(p.t0), y, width: Math.max(1, x(p.t1) - x(p.t0)), height: laneH, fill: "url(#minority)" }, plot);
    }
  }

  // ops per second
  svg("text", { x: L, y: opsTop - 8, class: "row-title" }, el).textContent = "answered per second · gave up";
  const bs = r.bucket_s;
  const vis = r.buckets.filter((b) => b.t + bs >= v0 && b.t <= v1);
  const maxOps = Math.max(1, ...vis.map((b) => (b.ok + b.unknown) / bs));
  svg("line", { x1: L, x2: width - R, y1: opsTop + opsH, y2: opsTop + opsH, class: "grid-line" }, el);
  svg("text", { x: L - 8, y: opsTop + 8, "text-anchor": "end" }, el).textContent = Math.round(maxOps);
  svg("text", { x: L - 8, y: opsTop + opsH, "text-anchor": "end" }, el).textContent = "0";
  for (const b of vis) {
    const x0 = x(b.t), w = Math.max(0.8, x(b.t + bs) - x0 - (x(b.t + bs) - x0 > 3 ? 1 : 0));
    const hOk = (opsH * b.ok) / bs / maxOps, hU = (opsH * b.unknown) / bs / maxOps;
    svg("rect", { x: x0, y: opsTop + opsH - hOk, width: w, height: hOk, fill: "rgba(107,143,214,0.75)" }, plot);
    if (b.unknown) svg("rect", { x: x0, y: opsTop + opsH - hOk - Math.max(1.5, hU), width: w, height: Math.max(1.5, hU), fill: css("--bad") }, plot);
  }

  // latency, log scale
  svg("text", { x: L, y: latTop - 8, class: "row-title" }, el).textContent = "latency · median and p99, log scale";
  const lo = 1, hi = 10000;
  const ly = (ms) => latTop + latH - (Math.log10(Math.max(lo, Math.min(hi, ms))) / Math.log10(hi)) * latH;
  for (const ms of [1, 10, 100, 1000, 10000]) {
    svg("line", { x1: L, x2: width - R, y1: ly(ms), y2: ly(ms), class: "grid-line", "stroke-dasharray": ms === 1 ? "" : "2 4" }, el);
    svg("text", { x: L - 8, y: ly(ms) + 4, "text-anchor": "end" }, el).textContent = ms >= 1000 ? ms / 1000 + "s" : ms;
  }
  const line = (key, col, wdt) => {
    let d = "";
    for (const b of vis) if (b[key]) d += (d ? "L" : "M") + (x(b.t + bs / 2)).toFixed(1) + "," + ly(b[key]).toFixed(1);
    if (d) svg("path", { d, fill: "none", stroke: col, "stroke-width": wdt, "stroke-linejoin": "round" }, plot);
  };
  line("p99", css("--bad"), 1.3);
  line("p50", css("--candidate"), 1.6);

  // fault markers above the lanes
  for (const e of r.events || []) {
    if (e.t < v0 || e.t > v1 || !["kill", "partition", "restart-all"].includes(e.kind)) continue;
    const c = e.kind === "kill" ? css("--bad") : e.kind === "partition" ? "#f6a3a5" : css("--candidate");
    svg("path", { d: `M${x(e.t) - 4},${top - 12} L${x(e.t) + 4},${top - 12} L${x(e.t)},${top - 5}Z`, fill: c }, plot);
  }

  $("#run-range").textContent = v0 > 0.01 || v1 < tEnd - 0.01 ? `${v0.toFixed(1)}–${v1.toFixed(1)} s · double-click to zoom out` : `${tEnd.toFixed(0)} s, drag to zoom`;

  // interaction
  const cursor = svg("line", { y1: top - 6, y2: H - 20, class: "cursor", visibility: "hidden" }, el);
  const brush = svg("rect", { y: top - 6, height: H - top - 14, class: "brush", visibility: "hidden" }, el);
  const tAt = (px) => v0 + ((px - L) / (width - L - R)) * (v1 - v0);
  let drag = null;
  const local = (e) => e.clientX - el.getBoundingClientRect().left;
  el.addEventListener("mousedown", (e) => { const px = local(e); if (px >= L) drag = px; });
  el.addEventListener("mousemove", (e) => {
    const px = local(e);
    if (px < L || px > width - R) { cursor.setAttribute("visibility", "hidden"); hideTip(); return; }
    cursor.setAttribute("x1", px); cursor.setAttribute("x2", px); cursor.setAttribute("visibility", "visible");
    if (drag != null) {
      brush.setAttribute("x", Math.min(drag, px)); brush.setAttribute("width", Math.abs(px - drag)); brush.setAttribute("visibility", "visible");
    }
    showTip(tipAt(r, tAt(px), segs, parts), e.clientX, e.clientY);
  });
  el.addEventListener("mouseleave", () => { cursor.setAttribute("visibility", "hidden"); hideTip(); });
  addEventListener("mouseup", function up(e) {
    removeEventListener("mouseup", up);
    if (drag == null) return;
    const px = Math.max(L, Math.min(width - R, local(e)));
    const a = tAt(Math.min(drag, px)), b = tAt(Math.max(drag, px));
    drag = null;
    if (b - a > 0.5) { view = [a, b]; hideTip(); drawTimeline(); }
    else brush.setAttribute("visibility", "hidden");
  });
  el.addEventListener("dblclick", () => { view = null; drawTimeline(); });
}

function tipAt(r, t, segs, parts) {
  const T = r.timeline;
  let k = 0, lo = 0, hi = T.length - 1;
  while (lo <= hi) { const m = (lo + hi) >> 1; if (T[m].t <= t) { k = m; lo = m + 1; } else hi = m - 1; }
  const smp = T[k];
  let html = `<div class="h">${t.toFixed(2)} s</div>`;
  if (smp) smp.nodes.forEach((p, i) => {
    const st = p.s === "" ? "not answering" : p.s === "down" ? "killed" : `${p.s} · term ${p.term} · commit ${fmt(p.commit)}`;
    html += `<div class="r"><span>n${i + 1}</span><span>${esc(st)}</span></div>`;
  });
  const part = parts.find((p) => p.t0 <= t && t < p.t1);
  if (part) html += `<div class="r"><span>net</span><span style="color:#f6a3a5">${esc(part.text)}</span></div>`;
  const b = r.buckets.find((b) => b.t <= t && t < b.t + r.bucket_s);
  if (b) html += `<div class="r"><span>ops</span><span>${fmt(Math.round(b.ok / r.bucket_s))}/s, ${b.unknown} gave up</span></div><div class="r"><span>latency</span><span>p50 ${b.p50} · p99 ${b.p99} ms</span></div>`;
  return html;
}

let resizeTimer;
addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => { if (current && !$("#view-runs").hidden) drawTimeline(); }, 120);
});

// ===================================================================== boot
(async () => {
  try {
    const cfg = await (await fetch("api/config")).json();
    liveAvailable = cfg.live;
  } catch { liveAvailable = false; }
  if (!liveAvailable) $('.tabs a[data-tab="live"]').style.display = "none";
  route();
})();
