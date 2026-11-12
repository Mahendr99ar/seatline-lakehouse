"use strict";

// Reads data.json (written by `python -m lakehouse.gold export`) and draws
// every chart as plain SVG. No chart library: the page has to load from a
// static file server with nothing else installed.

const NS = "http://www.w3.org/2000/svg";
const IST = "Asia/Kolkata";

const el = (tag, attrs = {}, parent) => {
  const n = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
  if (parent) parent.appendChild(n);
  return n;
};
const html = (tag, attrs = {}, text) => {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
  if (text !== undefined) n.textContent = text;
  return n;
};
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

// Lake timestamps are UTC without an offset.
const utc = (s) => new Date(s.endsWith("Z") ? s : s + "Z");
const inr = (paise) => "₹" + Math.round(paise / 100).toLocaleString("en-IN");
const lakh = (paise) => (paise / 1e7).toFixed(1);
const pct = (x) => Math.round(x * 100) + "%";
const when = (d, opts = {}) =>
  d.toLocaleString("en-IN", { timeZone: IST, day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", hour12: false, ...opts });
const dayLabel = (iso) => new Date(iso + "T00:00:00Z").toLocaleDateString("en-IN", { timeZone: "UTC", day: "numeric", month: "short" });

function scale([d0, d1], [r0, r1]) {
  const f = (v) => r0 + ((v - d0) / (d1 - d0 || 1)) * (r1 - r0);
  f.invert = (p) => d0 + ((p - r0) / (r1 - r0 || 1)) * (d1 - d0);
  return f;
}

function niceTicks(max, count = 4) {
  const raw = max / count;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= raw);
  const ticks = [];
  for (let v = 0; v <= max + step * 0.001; v += step) ticks.push(+v.toFixed(10));
  if (ticks[ticks.length - 1] < max) ticks.push(ticks[ticks.length - 1] + step);
  return ticks;
}

function frame(host, width, height) {
  host.replaceChildren();
  return el("svg", { viewBox: `0 0 ${width} ${height}`, role: "img" }, host);
}

function legend(host, items) {
  const ul = html("ul", { class: "legend" });
  for (const [label, color] of items) {
    const li = html("li");
    const sw = html("i");
    sw.style.background = color;
    li.append(sw, label);
    ul.append(li);
  }
  host.prepend(ul);
}

function table(host, head, rows, numeric = []) {
  const t = html("table");
  const tr = html("tr");
  head.forEach((h, i) => tr.append(html("th", numeric.includes(i) ? { class: "num" } : {}, h)));
  t.append(tr);
  for (const r of rows) {
    const row = html("tr");
    r.forEach((c, i) => row.append(html("td", numeric.includes(i) ? { class: "num" } : {}, c)));
    t.append(row);
  }
  host.replaceChildren(t);
}

// ---- tooltip -----------------------------------------------------------

const tip = document.getElementById("tip");
function showTip(evt, lines) {
  tip.replaceChildren();
  lines.forEach((line, i) => {
    if (i) tip.append(html("br"));
    tip.append(i === 0 ? html("b", {}, line) : line);
  });
  tip.hidden = false;
  const pad = 14;
  const { innerWidth: w, innerHeight: h } = window;
  const r = tip.getBoundingClientRect();
  let x = evt.clientX + pad;
  let y = evt.clientY + pad;
  if (x + r.width > w - 8) x = evt.clientX - r.width - pad;
  if (y + r.height > h - 8) y = evt.clientY - r.height - pad;
  tip.style.left = x + "px";
  tip.style.top = y + "px";
}
const hideTip = () => (tip.hidden = true);

function hover(node, lines) {
  node.addEventListener("pointermove", (e) => showTip(e, lines()));
  node.addEventListener("pointerleave", hideTip);
}

// ---- hero: one fare's life ------------------------------------------------

function drawStory(story) {
  const host = document.getElementById("story");
  const departs = utc(story.departs_at);
  const versions = story.versions.map((v, i) => ({
    i,
    from: utc(v.valid_from),
    to: v.valid_to.startsWith("9999") ? departs : new Date(Math.min(utc(v.valid_to), departs)),
    price: v.price_paise,
    sold: v.seats_sold,
    total: v.seats_total,
  }));

  document.getElementById("hero-count").textContent = `all ${versions.length} of them`;
  const cap = document.getElementById("story-caption");
  cap.replaceChildren(`${story.flight}, ${story.route.replace("-", " to ")}, economy`);
  cap.append(html("span", {}, `Departed ${when(departs)} IST. Each step is one SCD2 version in lake.history.fares.`));

  const W = 700, H = 320, m = { t: 18, r: 18, b: 34, l: 64 };
  const svg = frame(host, W, H);
  svg.setAttribute("aria-label", `Price of ${story.flight} economy over time, ${versions.length} versions`);

  const t0 = versions[0].from, t1 = departs;
  const prices = versions.map((v) => v.price);
  const lo = Math.min(...prices), hi = Math.max(...prices);
  const padY = (hi - lo) * 0.25 || hi * 0.1;
  const x = scale([+t0, +t1], [m.l, W - m.r]);
  const y = scale([lo - padY, hi + padY], [H - m.b, m.t]);

  const g = el("g", { class: "grid" }, svg);
  const step = 50000; // ₹500
  for (let p = Math.ceil((lo - padY) / step) * step; p <= hi + padY; p += step) {
    el("line", { x1: m.l, x2: W - m.r, y1: y(p), y2: y(p) }, g);
    el("text", { x: m.l - 8, y: y(p) + 4, "text-anchor": "end" }, svg).textContent = inr(p);
  }
  el("line", { class: "axis", x1: m.l, x2: W - m.r, y1: H - m.b, y2: H - m.b }, svg);
  for (let d = new Date(t0); d <= t1; d = new Date(+d + 12 * 3600e3)) {
    const tx = x(+d);
    el("line", { class: "axis", x1: tx, x2: tx, y1: H - m.b, y2: H - m.b + 5 }, svg);
    el("text", { x: tx, y: H - m.b + 19, "text-anchor": "middle" }, svg).textContent = when(d);
  }

  // What the source still holds: the last version only.
  const last = versions[versions.length - 1];
  el("rect", {
    x: x(+last.from), y: m.t, width: Math.max(x(+last.to) - x(+last.from), 2), height: H - m.b - m.t,
    fill: css("--history"), opacity: 0.12,
  }, svg);
  const srcLabel = el("text", { x: x(+last.from) - 6, y: m.t + 14, "text-anchor": "end", class: "lbl" }, svg);
  srcLabel.textContent = "all the source still has";

  let d = "";
  versions.forEach((v, i) => {
    d += `${i ? "L" : "M"}${x(+v.from)},${y(v.price)}H${x(+v.to)}`;
  });
  el("path", { d, fill: "none", stroke: css("--s1"), "stroke-width": 2.5, "stroke-linejoin": "round" }, svg);
  for (const v of versions) {
    el("circle", { cx: x(+v.from), cy: y(v.price), r: 4.5, fill: css("--s1"), stroke: css("--surface"), "stroke-width": 2 }, svg);
  }

  const cursor = el("g", { class: "cursor", opacity: 0 }, svg);
  const cline = el("line", { y1: m.t, y2: H - m.b, stroke: css("--ink"), "stroke-width": 1, "stroke-dasharray": "3 3" }, cursor);
  const cdot = el("circle", { r: 6, fill: css("--surface"), stroke: css("--ink"), "stroke-width": 2 }, cursor);

  const readout = document.getElementById("readout");
  const show = (t) => {
    const v = versions.find((v) => t >= +v.from && t < +v.to) || last;
    readout.replaceChildren();
    const add = (k, val) => readout.append(html("dt", {}, k), html("dd", {}, val));
    readout.append(html("div", { class: "now" }, `As of ${when(new Date(t))} IST`));
    add("Price", inr(v.price));
    add("Seats sold", `${v.sold} of ${v.total}`);
    add("Version", `${v.i + 1} of ${versions.length}`);
    return v;
  };

  const move = (e) => {
    const pt = svg.createSVGPoint();
    pt.x = e.clientX;
    pt.y = e.clientY;
    const p = pt.matrixTransform(svg.getScreenCTM().inverse());
    const t = Math.max(+t0, Math.min(+t1 - 1, x.invert(p.x)));
    const v = show(t);
    cline.setAttribute("x1", x(t));
    cline.setAttribute("x2", x(t));
    cdot.setAttribute("cx", x(t));
    cdot.setAttribute("cy", y(v.price));
    cursor.setAttribute("opacity", 1);
  };
  host.onpointermove = move;
  host.onpointerdown = move;
  host.onpointerleave = () => {
    cursor.setAttribute("opacity", 0);
    show(+last.from);
  };

  // Keyboard: left and right step through versions.
  host.tabIndex = 0;
  let k = versions.length - 1;
  host.onkeydown = (e) => {
    if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
    e.preventDefault();
    k = Math.max(0, Math.min(versions.length - 1, k + (e.key === "ArrowRight" ? 1 : -1)));
    const v = versions[k];
    show(+v.from);
    cline.setAttribute("x1", x(+v.from));
    cline.setAttribute("x2", x(+v.from));
    cdot.setAttribute("cx", x(+v.from));
    cdot.setAttribute("cy", y(v.price));
    cursor.setAttribute("opacity", 1);
  };
  show(+last.from);
}

// ---- pipeline strip ------------------------------------------------------

function drawFlow(summary, marts) {
  const ops = summary.by_op;
  const versions = Object.values(summary.versions).reduce((a, b) => a + b, 0);
  const n = (v) => v.toLocaleString("en-IN");
  const steps = [
    ["Postgres", "3 tables", "flights, fares and bookings, logical replication on", "--silver"],
    ["Debezium to Kafka", n(summary.changes), `change events: ${n(ops.c || 0)} inserts, ${n(ops.u || 0)} updates, ${n(ops.d || 0)} deletes`, "--ops"],
    ["Bronze", n(summary.changes), "rows, append-only, exactly as they arrived", "--bronze"],
    ["Silver", "MERGE", "current state per key, stale and replayed events ignored", "--silver"],
    ["History", n(versions), "SCD2 versions rebuilt from bronze", "--history"],
    ["Gold", String(marts), "tables, rebuilt by Airflow every 15 minutes", "--gold"],
  ];
  const ol = document.getElementById("flow");
  ol.replaceChildren();
  for (const [name, big, small, color] of steps) {
    const li = html("li");
    li.style.setProperty("--c", `var(${color})`);
    li.append(html("b", {}, name), html("span", { class: "n" }, big), html("small", {}, small));
    ol.append(li);
  }
}

// ---- fare curve: two lines ------------------------------------------------

function drawCurve(rows) {
  const host = document.getElementById("curve");
  const W = 640, H = 260, m = { t: 12, r: 70, b: 34, l: 44 };
  const svg = frame(host, W, H);
  svg.setAttribute("aria-label", "Average price index by hours before departure, economy and business");
  const cabins = [["economy", "--s1"], ["business", "--s2"]];
  const maxH = Math.max(...rows.map((r) => r.hours_before));
  const vals = rows.map((r) => r.price_index);
  const lo = Math.floor(Math.min(...vals) * 20) / 20, hi = Math.ceil(Math.max(...vals) * 20) / 20;
  const x = scale([maxH, 0], [m.l, W - m.r]);
  const y = scale([lo, hi], [H - m.b, m.t]);

  const g = el("g", { class: "grid" }, svg);
  for (let v = lo; v <= hi + 1e-9; v += 0.05) {
    el("line", { x1: m.l, x2: W - m.r, y1: y(v), y2: y(v) }, g);
    el("text", { x: m.l - 6, y: y(v) + 4, "text-anchor": "end" }, svg).textContent = v.toFixed(2) + "×";
  }
  el("line", { class: "axis", x1: m.l, x2: W - m.r, y1: H - m.b, y2: H - m.b }, svg);
  for (let h = Math.floor(maxH / 24) * 24; h >= 0; h -= 24) {
    el("text", { x: x(h), y: H - m.b + 18, "text-anchor": "middle" }, svg).textContent = h === 0 ? "departure" : `${h} h`;
  }

  for (const [cabin, color] of cabins) {
    const pts = rows.filter((r) => r.cabin === cabin).sort((a, b) => b.hours_before - a.hours_before);
    const d = pts.map((r, i) => `${i ? "L" : "M"}${x(r.hours_before)},${y(r.price_index)}`).join("");
    el("path", { d, fill: "none", stroke: css(color), "stroke-width": 2, "stroke-linejoin": "round" }, svg);
    const end = pts[pts.length - 1];
    const label = el("text", { x: W - m.r + 6, y: y(end.price_index) + (cabin === "business" ? -2 : 12), class: "lbl" }, svg);
    label.textContent = `${cabin} ${end.price_index.toFixed(2)}×`;
  }

  // One hit strip per hour, wider than the lines.
  const hits = el("g", {}, svg);
  const by = new Map();
  for (const r of rows) by.set(r.hours_before, { ...(by.get(r.hours_before) || {}), [r.cabin]: r });
  const cursor = el("line", { y1: m.t, y2: H - m.b, stroke: css("--ink"), "stroke-dasharray": "3 3", opacity: 0 }, svg);
  const colW = (W - m.l - m.r) / maxH;
  for (const [h, pair] of by) {
    const r = el("rect", { x: x(h) - colW / 2, y: m.t, width: colW, height: H - m.b - m.t, fill: "transparent" }, hits);
    r.addEventListener("pointerenter", () => {
      cursor.setAttribute("x1", x(h));
      cursor.setAttribute("x2", x(h));
      cursor.setAttribute("opacity", 0.6);
    });
    r.addEventListener("pointerleave", () => cursor.setAttribute("opacity", 0));
    hover(r, () => [
      h === 0 ? "Final hour" : `${h} h before departure`,
      ...cabins.filter(([c]) => pair[c]).map(([c]) => `${c}: ${pair[c].price_index.toFixed(3)}× (${pair[c].samples} fare-hours)`),
    ]);
  }
  legend(host, cabins.map(([c, color]) => [c, css(color)]));

  table(
    document.getElementById("curve-table"),
    ["Hours before", "Economy", "Business"],
    [...by.entries()].filter(([h]) => h % 12 === 0).map(([h, p]) => [
      String(h),
      p.economy ? p.economy.price_index.toFixed(3) : "",
      p.business ? p.business.price_index.toFixed(3) : "",
    ]),
    [0, 1, 2],
  );
}

// ---- load factor: dot plot per route ----------------------------------------

function drawLoad(rows) {
  const host = document.getElementById("load");
  const marks = [[72, "--ord-1", "72 h before"], [24, "--ord-2", "24 h before"], [0, "--ord-3", "at departure"]];
  const routes = [...new Set(rows.map((r) => r.route))];
  const at = (route, h) => rows.find((r) => r.route === route && r.hours_before === h);
  routes.sort((a, b) => (at(b, 0)?.load_factor ?? 0) - (at(a, 0)?.load_factor ?? 0));

  const rowH = 22, m = { t: 6, r: 16, b: 28, l: 70 };
  const W = 460, H = m.t + routes.length * rowH + m.b;
  const svg = frame(host, W, H);
  svg.setAttribute("aria-label", "Load factor by route at three points before departure");
  const max = Math.ceil(Math.max(...rows.map((r) => r.load_factor)) * 10) / 10;
  const x = scale([0, max], [m.l, W - m.r]);
  const g = el("g", { class: "grid" }, svg);
  for (let v = 0; v <= max + 1e-9; v += 0.1) {
    el("line", { x1: x(v), x2: x(v), y1: m.t, y2: H - m.b }, g);
    el("text", { x: x(v), y: H - m.b + 16, "text-anchor": "middle" }, svg).textContent = pct(v);
  }

  routes.forEach((route, i) => {
    const cy = m.t + i * rowH + rowH / 2;
    el("text", { x: m.l - 8, y: cy + 4, "text-anchor": "end", class: "lbl" }, svg).textContent = route;
    const pts = marks.map(([h]) => at(route, h)).filter(Boolean);
    const xs = pts.map((p) => x(p.load_factor));
    el("line", { x1: Math.min(...xs), x2: Math.max(...xs), y1: cy, y2: cy, stroke: css("--axis"), "stroke-width": 2 }, svg);
    for (const [h, color, label] of marks) {
      const p = at(route, h);
      if (!p) continue;
      const c = el("circle", { cx: x(p.load_factor), cy, r: 5, fill: css(color), stroke: css("--surface"), "stroke-width": 2 }, svg);
      const hit = el("circle", { cx: x(p.load_factor), cy, r: 10, fill: "transparent" }, svg);
      hover(hit, () => [route, `${label}: ${pct(p.load_factor)} sold`, `${p.flights} departed flights`]);
      c.setAttribute("aria-hidden", "true");
    }
  });
  legend(host, marks.map(([, color, label]) => [label, css(color)]));
}

// ---- revenue heatmap -------------------------------------------------------

function mix(a, b, t) {
  const p = (h) => [1, 3, 5].map((i) => parseInt(h.slice(i, i + 2), 16));
  const [ca, cb] = [p(a), p(b)];
  return "#" + ca.map((v, i) => Math.round(v + (cb[i] - v) * t).toString(16).padStart(2, "0")).join("");
}

function luminance(hex) {
  const [r, g, b] = [1, 3, 5].map((i) => {
    const c = parseInt(hex.slice(i, i + 2), 16) / 255;
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function drawHeat(rows) {
  const host = document.getElementById("heat");
  const days = [...new Set(rows.map((r) => r.day))].sort();
  const totals = new Map();
  for (const r of rows) totals.set(r.route, (totals.get(r.route) || 0) + r.revenue_paise);
  const routes = [...totals.keys()].sort((a, b) => totals.get(b) - totals.get(a));
  const cell = new Map(rows.map((r) => [`${r.route}|${r.day}`, r]));
  const max = Math.max(...rows.map((r) => r.revenue_paise));

  const m = { t: 24, r: 90, b: 6, l: 72 };
  const cw = 104, ch = 22, gap = 2;
  const W = m.l + days.length * (cw + gap) + m.r, H = m.t + routes.length * (ch + gap) + m.b;
  const svg = frame(host, W, H);
  svg.setAttribute("aria-label", "Confirmed revenue by route and day");
  const c0 = css("--seq-0"), c1 = css("--seq-1");

  days.forEach((d, j) => {
    const t = el("text", { x: m.l + j * (cw + gap) + cw / 2, y: m.t - 8, "text-anchor": "middle" }, svg);
    t.textContent = dayLabel(d) + (j === 0 || j === days.length - 1 ? " (part)" : "");
  });
  el("text", { x: W - m.r + 10, y: m.t - 8 }, svg).textContent = "week";

  routes.forEach((route, i) => {
    const cy = m.t + i * (ch + gap);
    el("text", { x: m.l - 8, y: cy + ch / 2 + 4, "text-anchor": "end", class: "lbl" }, svg).textContent = route;
    days.forEach((day, j) => {
      const r = cell.get(`${route}|${day}`);
      const v = r ? r.revenue_paise : 0;
      const t = Math.sqrt(v / max); // sqrt keeps the quieter routes readable
      const rect = el("rect", { x: m.l + j * (cw + gap), y: cy, width: cw, height: ch, rx: 2, fill: mix(c0, c1, t) }, svg);
      const txt = el("text", { x: m.l + j * (cw + gap) + cw / 2, y: cy + ch / 2 + 4, "text-anchor": "middle" }, svg);
      txt.textContent = lakh(v);
      txt.style.fill = luminance(mix(c0, c1, t)) < 0.35 ? "#fff" : "#13233a";
      txt.style.pointerEvents = "none";
      hover(rect, () => [`${route}, ${dayLabel(day)}`, `${inr(v)} confirmed`, r ? `${r.bookings} bookings, ${r.seats} seats` : "no bookings"]);
    });
    const total = el("text", { x: W - m.r + 10, y: cy + ch / 2 + 4, class: "lbl-strong" }, svg);
    total.textContent = `₹${lakh(totals.get(route))} L`;
  });
}

// ---- funnel: stacked bars per day -----------------------------------------

function drawFunnel(rows) {
  const host = document.getElementById("funnel");
  const parts = [
    ["confirmed and kept", "--s1", (r) => r.confirmed - r.cancelled_after_confirm],
    ["cancelled after confirming", "--s2", (r) => r.cancelled_after_confirm],
    ["hold expired", "--s3", (r) => r.expired],
  ];
  const W = 520, H = 250, m = { t: 10, r: 10, b: 30, l: 46 };
  const svg = frame(host, W, H);
  svg.setAttribute("aria-label", "Outcome of booking holds per day");
  const max = Math.max(...rows.map((r) => r.held));
  const ticks = niceTicks(max);
  const y = scale([0, ticks[ticks.length - 1]], [H - m.b, m.t]);
  const band = (W - m.l - m.r) / rows.length;
  const bw = Math.min(44, band * 0.62);

  const g = el("g", { class: "grid" }, svg);
  for (const v of ticks) {
    el("line", { x1: m.l, x2: W - m.r, y1: y(v), y2: y(v) }, g);
    el("text", { x: m.l - 6, y: y(v) + 4, "text-anchor": "end" }, svg).textContent = v.toLocaleString("en-IN");
  }
  rows.forEach((r, i) => {
    const cx = m.l + band * i + band / 2;
    let base = 0;
    parts.forEach(([label, color, f], k) => {
      const v = f(r);
      const top = k === parts.length - 1;
      const y0 = y(base), y1 = y(base + v);
      // 2 px of surface between segments; only the top segment gets rounded ends.
      const h = Math.max(y0 - y1 - (k ? 2 : 0), 0);
      const path = top
        ? `M${cx - bw / 2},${y0 - (k ? 2 : 0)}V${y1 + 4}q0,-4 4,-4H${cx + bw / 2 - 4}q4,0 4,4V${y0 - (k ? 2 : 0)}Z`
        : `M${cx - bw / 2},${y0 - (k ? 2 : 0)}v${-h}h${bw}v${h}Z`;
      const seg = el("path", { d: path, fill: css(color) }, svg);
      hover(seg, () => [
        `${dayLabel(r.day)}: ${r.held.toLocaleString("en-IN")} holds`,
        `${label}: ${v.toLocaleString("en-IN")} (${pct(v / r.held)})`,
      ]);
      base += v;
    });
    el("text", { x: cx, y: H - m.b + 18, "text-anchor": "middle" }, svg).textContent = dayLabel(r.day);
  });
  el("line", { class: "axis", x1: m.l, x2: W - m.r, y1: H - m.b, y2: H - m.b }, svg);
  legend(host, parts.map(([label, color]) => [label, css(color)]));

  table(
    document.getElementById("funnel-table"),
    ["Day", "Holds", "Confirmed", "Cancelled later", "Expired"],
    rows.map((r) => [dayLabel(r.day), r.held, r.confirmed, r.cancelled_after_confirm, r.expired].map(String)),
    [1, 2, 3, 4],
  );
}

// ---- delays table with inline bars ------------------------------------------

function drawDelays(rows) {
  const host = document.getElementById("delays");
  const sorted = [...rows].sort((a, b) => b.delayed / b.flights - a.delayed / a.flights);
  const max = Math.max(...sorted.map((r) => r.delayed / r.flights));
  const t = html("table");
  const head = html("tr");
  ["Route", "Flights", "Delayed", "", "Avg delay"].forEach((h, i) =>
    head.append(html("th", [1, 2, 4].includes(i) ? { class: "num" } : {}, h)),
  );
  t.append(head);
  for (const r of sorted) {
    const tr = html("tr", { class: "delay-row" });
    const share = r.delayed / r.flights;
    const bar = html("td", { class: "bar-cell" });
    const span = html("span");
    span.style.width = max ? `${(share / max) * 100}%` : "0";
    span.title = `${pct(share)} of flights delayed`;
    bar.append(span);
    tr.append(
      html("td", {}, r.route),
      html("td", { class: "num" }, String(r.flights)),
      html("td", { class: "num" }, String(r.delayed)),
      bar,
      html("td", { class: "num" }, r.avg_delay_min == null ? "none" : `${Math.round(r.avg_delay_min)} min`),
    );
    t.append(tr);
  }
  host.replaceChildren(t);
}

// ---- change volume: stacked bars per hour ------------------------------------

function drawVolume(rows) {
  const host = document.getElementById("volume");
  const ops = [["c", "inserts", "--s1"], ["u", "updates", "--s2"], ["d", "deletes", "--s3"]];
  const hours = new Map();
  for (const r of rows) {
    const h = hours.get(r.hour) || { c: 0, u: 0, d: 0, tables: {} };
    h[r.op] += r.count;
    h.tables[r.table] = (h.tables[r.table] || 0) + r.count;
    hours.set(r.hour, h);
  }
  const keys = [...hours.keys()].sort();
  const W = 1100, H = 250, m = { t: 10, r: 10, b: 30, l: 46 };
  const svg = frame(host, W, H);
  svg.setAttribute("aria-label", "Change events per hour by operation");
  const max = Math.max(...keys.map((k) => hours.get(k).c + hours.get(k).u + hours.get(k).d));
  const ticks = niceTicks(max);
  const y = scale([0, ticks[ticks.length - 1]], [H - m.b, m.t]);
  const band = (W - m.l - m.r) / keys.length;

  const g = el("g", { class: "grid" }, svg);
  for (const v of ticks) {
    el("line", { x1: m.l, x2: W - m.r, y1: y(v), y2: y(v) }, g);
    el("text", { x: m.l - 6, y: y(v) + 4, "text-anchor": "end" }, svg).textContent = v.toLocaleString("en-IN");
  }
  keys.forEach((k, i) => {
    const h = hours.get(k);
    const x0 = m.l + band * i;
    let base = 0;
    for (const [op, , color] of ops) {
      if (!h[op]) continue;
      const y0 = y(base), y1 = y(base + h[op]);
      el("rect", { x: x0, y: y1, width: Math.max(band - 1, 0.5), height: Math.max(y0 - y1, 0.5), fill: css(color) }, svg);
      base += h[op];
    }
    const hit = el("rect", { x: x0, y: m.t, width: band, height: H - m.b - m.t, fill: "transparent" }, svg);
    hover(hit, () => [
      `${dayLabel(k.slice(0, 10))}, ${k.slice(11, 16)} IST`,
      ops.map(([op, label]) => `${h[op]} ${label}`).join(", "),
      Object.entries(h.tables).map(([t, n]) => `${t} ${n}`).join(", "),
    ]);
    if (k.slice(11, 13) === "00") {
      el("text", { x: x0, y: H - m.b + 18, "text-anchor": "middle" }, svg).textContent = dayLabel(k.slice(0, 10));
    }
  });
  el("line", { class: "axis", x1: m.l, x2: W - m.r, y1: H - m.b, y2: H - m.b }, svg);
  legend(host, ops.map(([, label, color]) => [label, css(color)]));
}

// ---- quality checks ------------------------------------------------------------

const ICONS = {
  pass: ["--good", "M3 8.5l3 3 7-7"],
  warn: ["--warning", "M8 3v6M8 12v1"],
  fail: ["--critical", "M4 4l8 8M12 4l-8 8"],
};

function drawChecks(checks) {
  const ul = document.getElementById("checks");
  ul.replaceChildren();
  const order = { fail: 0, warn: 1, pass: 2 };
  for (const c of [...checks].sort((a, b) => order[a.status] - order[b.status])) {
    const li = html("li");
    const st = html("span", { class: "st" });
    const [color, d] = ICONS[c.status];
    const svg = el("svg", { width: 16, height: 16, viewBox: "0 0 16 16", "aria-hidden": "true" });
    el("path", { d, fill: "none", stroke: css(color), "stroke-width": 2.4, "stroke-linecap": "round", "stroke-linejoin": "round" }, svg);
    st.append(svg, c.status);
    const what = html("span", { class: "what" }, `${c.check.replaceAll("_", " ")}, ${c.table}`);
    what.append(html("span", { class: "why" }, c.detail));
    li.append(st, what);
    ul.append(li);
  }
}

// ---- page -------------------------------------------------------------------

let data;

function render() {
  hideTip();
  const s = data.summary;
  const from = utc(s.business_from), to = utc(s.business_to);
  const fmt = (d) => d.toLocaleDateString("en-IN", { timeZone: IST, day: "numeric", month: "long" });
  document.getElementById("span").textContent =
    `Simulated bookings from ${fmt(from)} to ${fmt(to)} 2026, as captured from the source's write-ahead log`;
  document.getElementById("generated").textContent =
    `Exported from the lake at ${when(new Date(data.generated_at))} IST.`;

  drawStory(data.fare_story);
  drawFlow(s, 6);
  drawCurve(data.fare_curve);
  drawLoad(data.load_factor);
  drawHeat(data.route_daily);
  drawFunnel(data.booking_funnel);
  drawDelays(data.delays);
  drawVolume(data.cdc_volume);
  drawChecks(data.checks);
}

function setTheme(theme) {
  document.documentElement.dataset.theme = theme;
  document.getElementById("theme").textContent = theme === "dark" ? "Light" : "Dark";
  try { localStorage.setItem("lakehouse-theme", theme); } catch { /* storage may be blocked */ }
  if (data) render();
}

let saved = null;
try { saved = localStorage.getItem("lakehouse-theme"); } catch { /* ignore */ }
setTheme(saved || (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light"));
document.getElementById("theme").onclick = () =>
  setTheme(document.documentElement.dataset.theme === "dark" ? "light" : "dark");

fetch("data.json", { cache: "no-store" })
  .then((r) => {
    if (!r.ok) throw new Error(`data.json returned ${r.status}`);
    return r.json();
  })
  .then((d) => {
    data = d;
    render();
  })
  .catch((err) => {
    document.getElementById("span").textContent =
      `Could not load data.json (${err.message}). Run the export step in Airflow, or devtools/local_run.py.`;
  });
