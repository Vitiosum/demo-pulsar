// Departures: the browser side. It only renders what the server streams
// (Server-Sent Events); every change comes from a message read in Pulsar.
"use strict";
(() => {
  const $ = (sel) => document.querySelector(sel);
  const reduced = matchMedia("(prefers-reduced-motion: reduce)").matches;
  const DRUM = " ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'+-.:/";
  const ROWS = 10;
  const FLIP_MS = 72;
  const WIDTH = { time: 5, train: 7, dest: 14, plat: 2, status: 12 };
  const TZ = "Europe/Paris";
  const hm = new Intl.DateTimeFormat("fr-FR", { hour: "2-digit", minute: "2-digit", timeZone: TZ });
  const hms = new Intl.DateTimeFormat("fr-FR", { hour: "2-digit", minute: "2-digit", second: "2-digit", fractionalSecondDigits: 3, timeZone: TZ });
  const num = new Intl.NumberFormat("fr-FR");
  const KIND = { announced: "Annonce", platform: "Voie", delayed: "Retard", boarding: "Embarquement", departed: "Départ", cancelled: "Suppression" };

  const state = { rows: [], link: {}, stats: null, topic: "", subscription: "", archive: "", offset: 0, selected: "", picked: false, replaying: false, queue: 0, pending: null };

  // ------------------------------------------------------------ sound (off by default)
  const sound = {
    on: false, ctx: null, buf: null, last: 0,
    toggle() {
      this.on = !this.on;
      if (this.on && !this.ctx) {
        this.ctx = new AudioContext();
        const len = Math.floor(this.ctx.sampleRate * 0.007);
        this.buf = this.ctx.createBuffer(1, len, this.ctx.sampleRate);
        const data = this.buf.getChannelData(0);
        for (let i = 0; i < len; i++) data[i] = (Math.random() * 2 - 1) * Math.pow(1 - i / len, 3);
      }
      return this.on;
    },
    click() {
      if (!this.on || !this.ctx) return;
      const now = this.ctx.currentTime;
      if (now - this.last < 0.012) return;
      this.last = now;
      const src = this.ctx.createBufferSource();
      const filter = this.ctx.createBiquadFilter();
      const gain = this.ctx.createGain();
      src.buffer = this.buf;
      filter.type = "bandpass";
      filter.frequency.value = 1500 + Math.random() * 900;
      gain.gain.value = 0.05 + Math.random() * 0.04;
      src.connect(filter).connect(gain).connect(this.ctx.destination);
      src.start(now);
    },
  };

  // ------------------------------------------------------------ one flap
  const clean = (s) => s.toUpperCase().normalize("NFD").replace(/[̀-ͯ]/g, "");

  class Flap {
    constructor() {
      this.el = document.createElement("span");
      this.el.className = "f";
      this.el.innerHTML = '<span class="f__h f__h--top"><i> </i></span><span class="f__h f__h--bot"><i> </i></span>' +
        '<span class="f__leaf"><span class="f__h f__h--top"><i> </i></span><span class="f__h f__h--bot f__back"><i> </i></span></span>';
      const [top, bot, front, back] = this.el.querySelectorAll("i");
      Object.assign(this, { top, bot, front, back, ch: " ", target: " ", timer: 0 });
    }
    set(ch, delay) {
      if (!DRUM.includes(ch)) ch = " ";
      if (ch === this.target) return;
      this.target = ch;
      if (reduced) {
        this.ch = ch;
        this.top.textContent = this.bot.textContent = ch;
        return;
      }
      if (!this.timer) this.timer = setTimeout(() => this.step(), delay);
    }
    // One turn of the drum: the upper leaf falls and becomes the lower half.
    step() {
      if (this.ch === this.target) {
        this.timer = 0;
        this.el.classList.remove("is-flipping");
        return;
      }
      const at = DRUM.indexOf(this.ch), to = DRUM.indexOf(this.target);
      const gap = (to - at + DRUM.length) % DRUM.length;
      const next = DRUM[gap > 9 ? (to - 6 - Math.floor(Math.random() * 3) + DRUM.length) % DRUM.length : (at + 1) % DRUM.length];
      this.top.textContent = next;
      this.front.textContent = this.ch;
      this.back.textContent = next;
      this.el.classList.add("is-flipping");
      this.el.classList.toggle("odd");
      sound.click();
      this.timer = setTimeout(() => {
        this.bot.textContent = next;
        this.ch = next;
        this.step();
      }, FLIP_MS);
    }
  }

  // ------------------------------------------------------------ the board
  const statusText = (t) => ({
    on_time: "A L'HEURE", delayed: `RETARD +${t.delay}'`, boarding: "EMBARQUEMENT", departed: "PARTI", cancelled: "SUPPRIME",
  })[t.status] || "";
  const trainText = (t) => `${t.service.padEnd(2)} ${t.number}`;
  const label = (t) => `${hm.format(new Date(t.scheduled))} ${t.service} ${t.number} ${t.destination}`;

  // A slot is one line of the board: its flaps show whichever train sits at that position.
  class Slot {
    constructor() {
      this.id = "";
      this.li = document.createElement("li");
      this.li.className = "row is-blank";
      this.li.innerHTML = '<span class="row__lamp"></span>';
      this.cells = {};
      for (const [name, width] of Object.entries(WIDTH)) {
        const cell = document.createElement("span");
        cell.className = `cell cell--${name}`;
        this.cells[name] = Array.from({ length: width }, () => {
          const flap = new Flap();
          cell.appendChild(flap.el);
          return flap;
        });
        this.li.appendChild(cell);
      }
      this.li.addEventListener("click", () => this.id && select(this.id, true));
      this.li.addEventListener("keydown", (e) => {
        if ((e.key === "Enter" || e.key === " ") && this.id) { e.preventDefault(); select(this.id, true); }
      });
    }
    show(t, base) {
      this.id = t ? t.id : "";
      const text = t ? {
        time: hm.format(new Date(t.scheduled)), train: trainText(t), dest: clean(t.destination).slice(0, WIDTH.dest),
        plat: (t.platform || "").padStart(2), status: statusText(t),
      } : {};
      let col = 0;
      for (const [name, width] of Object.entries(WIDTH)) {
        const s = (text[name] || "").padEnd(width).slice(0, width);
        this.cells[name].forEach((flap, i) => flap.set(s[i], base + (col + i) * 14)); // ripple left to right
        col += width;
      }
      this.li.dataset.status = t ? t.status : "";
      if (t && t.platform) this.li.dataset.platform = t.platform;
      else delete this.li.dataset.platform;
      this.li.classList.toggle("is-blank", !t);
      this.li.classList.toggle("is-selected", !!t && state.picked && t.id === state.selected);
      if (t) {
        this.li.tabIndex = 0;
        this.li.setAttribute("role", "button");
        this.li.setAttribute("aria-label", `${label(t)}, voie ${t.platform || "non affichée"}, ${statusText(t).toLowerCase()}`);
      } else {
        this.li.removeAttribute("tabindex");
        this.li.removeAttribute("role");
        this.li.removeAttribute("aria-label");
      }
    }
    flash(byMaster) {
      clearTimeout(this.fade);
      this.li.classList.add("is-fresh");
      this.li.classList.toggle("by-master", byMaster);
      this.fade = setTimeout(() => this.li.classList.remove("is-fresh", "by-master"), 2600);
    }
  }

  const rowsEl = $("#rows");
  const slots = Array.from({ length: ROWS }, () => {
    const slot = new Slot();
    rowsEl.appendChild(slot.li);
    return slot;
  });

  function renderBoard(rows, fast) {
    slots.forEach((slot, i) => slot.show(rows[i], fast ? 0 : i * 45));
  }

  function flashTrain(id, byMaster) {
    const slot = slots.find((s) => s.id === id);
    if (slot) slot.flash(byMaster);
  }

  // ------------------------------------------------------------ station clock
  const clock = $("#clock");
  const ticks = $("#clock-ticks");
  for (let i = 0; i < 60; i++) {
    const a = (i * Math.PI) / 30, h = i % 5 === 0;
    const r1 = h ? 40 : 45, r2 = 48;
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", 60 + r1 * Math.sin(a)); line.setAttribute("y1", 60 - r1 * Math.cos(a));
    line.setAttribute("x2", 60 + r2 * Math.sin(a)); line.setAttribute("y2", 60 - r2 * Math.cos(a));
    line.setAttribute("class", h ? "clock__tick clock__tick--h" : "clock__tick");
    ticks.appendChild(line);
  }
  const hands = { h: $("#hand-h"), m: $("#hand-m"), s: $("#hand-s") };
  const parisOffset = (d) => { // minutes between Paris wall time and UTC at instant d
    const p = Object.fromEntries(new Intl.DateTimeFormat("en-GB", { timeZone: TZ, hourCycle: "h23", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }).formatToParts(d).map((x) => [x.type, x.value]));
    return (Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute) - Math.floor(d.getTime() / 60000) * 60000) / 60000;
  };
  function setClock(ms) { // cumulative angles, so hands never spin backwards by accident
    const local = ms / 1000 + parisOffset(new Date(ms)) * 60;
    const s = Math.floor(local);
    hands.s.style.transform = `rotate(${s * 6}deg)`;
    hands.m.style.transform = `rotate(${(local / 10) % 360 + Math.floor(local / 3600) * 360}deg)`;
    hands.h.style.transform = `rotate(${(local / 120) % 360 + Math.floor(local / 43200) * 360}deg)`;
  }
  setInterval(() => { if (!state.replaying) setClock(Date.now() + state.offset); }, 1000);
  setClock(Date.now());

  // ------------------------------------------------------------ signal box
  const SVG = "http://www.w3.org/2000/svg";
  const tco = { pulses: $("#tco-pulses"), queue: $("#tco-queue") };
  const track = (id) => document.getElementById(id);
  const node = (id) => document.getElementById(id);

  function light(el, ms = 500) {
    el.classList.add("is-lit");
    clearTimeout(el._lit);
    el._lit = setTimeout(() => el.classList.remove("is-lit"), ms);
  }
  function travel(path, color, duration, done) {
    if (reduced) { done && done(); return; }
    const dot = document.createElementNS(SVG, "circle");
    dot.setAttribute("r", 5);
    dot.setAttribute("fill", color);
    tco.pulses.appendChild(dot);
    const len = path.getTotalLength();
    const t0 = performance.now();
    light(path, duration + 200);
    const frame = (now) => {
      const k = Math.min(1, (now - t0) / duration);
      const p = path.getPointAtLength(len * (k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2));
      dot.setAttribute("cx", p.x);
      dot.setAttribute("cy", p.y);
      if (k < 1) requestAnimationFrame(frame);
      else { dot.remove(); done && done(); }
    };
    requestAnimationFrame(frame);
  }
  // A message: producer → topic → every subscription that is listening.
  function pulse(byMaster) {
    const color = byMaster ? "#f57461" : "#ffd76d";
    light(node(byMaster ? "n-master" : "n-clock"));
    travel(track(byMaster ? "t-master" : "t-clock"), color, 520, () => {
      light(node("n-topic"), 300);
      setTimeout(() => {
        travel(track("t-board"), color, 420, () => light(node("n-board")));
        travel(track("t-archive"), color, 380, () => { state.queue++; drawQueue(); });
      }, 140);
    });
  }
  function drawQueue() { // the archive acknowledges in batches: its backlog waits on the siding
    tco.queue.replaceChildren();
    const n = Math.max(0, state.queue);
    for (let i = 0; i < Math.min(n, 8); i++) {
      const c = document.createElementNS(SVG, "circle");
      c.setAttribute("cx", 462 - i * 11);
      c.setAttribute("cy", 120);
      c.setAttribute("r", 4);
      tco.queue.appendChild(c);
    }
    if (n > 8) {
      const t = document.createElementNS(SVG, "text");
      t.setAttribute("x", 376); t.setAttribute("y", 110);
      t.textContent = `+${n - 8}`;
      tco.queue.appendChild(t);
    }
    if (n === 0) light(node("n-archive"), 700);
  }

  const kb = (b) => (b < 1024 ? `${b} o` : b < 1048576 ? `${(b / 1024).toFixed(1).replace(".", ",")} Ko` : `${(b / 1048576).toFixed(1).replace(".", ",")} Mo`);

  function renderStats(stats) {
    if (!stats) return;
    $("#stat-in").textContent = num.format(stats.msgInCounter);
    $("#stat-storage").textContent = kb(stats.storageSize);
    const subs = Object.entries(stats.subscriptions || {});
    const rank = (n) => (n === state.subscription ? 0 : n.startsWith("board-") ? 1 : n === state.archive ? 2 : 3);
    subs.sort((a, b) => rank(a[0]) - rank(b[0]) || a[0].localeCompare(b[0]));
    const body = $("#subs");
    body.replaceChildren(...subs.map(([name, s]) => {
      const tr = document.createElement("tr");
      if (name === state.subscription) tr.className = "is-mine";
      const role = name === state.subscription ? "ce tableau" : name.startsWith("board-") ? "tableau d'une autre instance"
        : name === state.archive ? "acquitte toutes les 45 s" : name.startsWith("late-reader") ? "relecture en cours" : "";
      tr.innerHTML = `<td></td><td></td><td><span class="backlog"><i></i><span></span></span></td>`;
      tr.children[0].textContent = name;
      if (role) { const small = document.createElement("small"); small.textContent = role; tr.children[0].appendChild(small); }
      tr.children[1].textContent = `${s.type || "?"}${s.isDurable ? ", durable" : ", non durable"}`;
      tr.querySelector(".backlog i").style.width = `${Math.min(110, s.msgBacklog * 9)}px`;
      tr.querySelector(".backlog span").textContent = num.format(s.msgBacklog);
      return tr;
    }));
    if (!subs.length) body.innerHTML = '<tr><td colspan="3">Aucun abonnement actif.</td></tr>';
    const archive = (stats.subscriptions || {})[state.archive];
    if (archive) { state.queue = archive.msgBacklog; drawQueue(); }
  }

  function messageItem(d, fresh) {
    const li = document.createElement("li");
    const e = d.event, t = e.train;
    if (e.by === "station-master") li.classList.add("by-master");
    if (fresh) li.classList.add("is-new");
    li.innerHTML = '<time></time><span class="k"></span><span class="t"><code></code></span><span class="lat"></span>';
    li.querySelector("time").textContent = hms.format(new Date(d.publishTime));
    li.querySelector(".k").textContent = KIND[e.kind] || e.kind;
    li.querySelector(".t").prepend(`${t.service} ${t.number} ${t.destination}`);
    li.querySelector("code").textContent = d.id;
    li.querySelector(".lat").textContent = d.latencyMs ? `${Math.round(d.latencyMs)} ms` : "relu";
    li.title = `${e.by === "station-master" ? "Chef de gare" : "Horloge"}, producteur ${d.producer}`;
    return li;
  }
  function renderMessages(list) {
    $("#messages").replaceChildren(...list.slice(0, 8).map((d) => messageItem(d, false)));
  }
  function addMessage(d) {
    const ol = $("#messages");
    ol.prepend(messageItem(d, true));
    while (ol.children.length > 8) ol.lastChild.remove();
  }

  // ------------------------------------------------------------ link and texts
  function renderLink(link) {
    state.link = link;
    const el = $("#link");
    const text = $("#link-text");
    if (!link.configured) { el.dataset.state = "none"; text.textContent = "Aucun add-on Pulsar lié"; }
    else if (link.live) { el.dataset.state = "live"; text.textContent = "En direct du topic"; }
    else { el.dataset.state = "down"; text.textContent = link.error ? "Liaison Pulsar interrompue, reprise…" : "Connexion au topic…"; }
    $("#retention").textContent = link.retention ? `Rétention du namespace : ${link.retention}` : "";
    const pill = $("#clock-pill");
    pill.lastChild.textContent = link.clock ? "Cette instance tient l'horloge de gare" : "Instance en écoute";
    if (link.rebuiltAt) {
      const foot = $("#board-foot");
      foot.textContent = `Reconstruit au démarrage depuis Pulsar : ${num.format(link.rebuilt)} messages relus en ${Math.round(link.rebuiltMs)} ms, puis abonnement `;
      const code = document.createElement("code");
      code.textContent = state.subscription;
      foot.append(code, " (non durable).");
      $("#late-proof").textContent = `Ce tableau l'a fait lui-même au démarrage de l'instance : ${num.format(link.rebuilt)} messages relus en ${Math.round(link.rebuiltMs)} ms.`;
    }
  }

  // ------------------------------------------------------------ station master console
  const trainSelect = $("#order-train");
  const platformSelect = $("#order-platform");
  for (let p = 1; p <= 12; p++) platformSelect.add(new Option(`Voie ${p}`, String(p)));

  function select(id, picked) {
    state.selected = id;
    if (picked) state.picked = true;
    trainSelect.value = id;
    slots.forEach((s) => s.li.classList.toggle("is-selected", state.picked && s.id === id));
    const t = state.rows.find((r) => r.id === id);
    if (t && t.platform) platformSelect.value = t.platform;
  }
  function renderTrains(rows) {
    const open = rows.filter((t) => t.status !== "departed" && t.status !== "cancelled");
    const keep = open.some((t) => t.id === state.selected) ? state.selected : open[0] ? open[0].id : "";
    trainSelect.replaceChildren(...(open.length ? open.map((t) => new Option(label(t), t.id)) : [new Option("Aucun train au tableau", "")]));
    if (keep) select(keep);
  }
  trainSelect.addEventListener("change", () => select(trainSelect.value, true));

  const journey = Object.fromEntries([...document.querySelectorAll("#journey li")].map((li) => [li.dataset.step, li]));
  function step(name, status, detail) {
    const li = journey[name];
    li.classList.remove("is-on", "is-done");
    if (status) li.classList.add(status);
    li.querySelector("span").textContent = detail;
  }

  document.querySelectorAll("[data-action]").forEach((btn) => btn.addEventListener("click", async () => {
    const err = $("#order-error");
    err.hidden = true;
    if (!state.selected) { err.textContent = "Choisissez d'abord un train."; err.hidden = false; return; }
    const order = { train: state.selected, action: btn.dataset.action };
    if (order.action === "delay") order.minutes = Number(btn.dataset.minutes);
    if (order.action === "platform") order.platform = platformSelect.value;
    const keys = document.querySelectorAll("[data-action]");
    keys.forEach((k) => (k.disabled = true));
    setTimeout(() => keys.forEach((k) => (k.disabled = false)), 2000);
    const t0 = performance.now();
    step("sent", "is-on", "POST /api/orders");
    step("stored", "", "…");
    step("delivered", "", "—");
    step("shown", "", "—");
    try {
      const res = await fetch("/api/orders", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(order) });
      const body = await res.json();
      if (!res.ok) throw new Error(body.error || `Erreur ${res.status}`);
      step("sent", "is-done", `producteur ${body.receipt.producer}`);
      step("stored", "is-done", `message ${body.receipt.id}, accusé en ${Math.round(body.receipt.ackMs)} ms`);
      step("delivered", "is-on", `en route vers ${state.subscription}`);
      state.pending = { id: body.receipt.id, t0 };
    } catch (e) {
      step("sent", "", "refusé");
      step("stored", "", "—");
      err.textContent = e.message;
      err.hidden = false;
    }
  }));

  function settlePending(d) {
    if (!state.pending || state.pending.id !== d.id) return;
    step("delivered", "is-done", `reçu par ${state.subscription}, ${Math.round(d.latencyMs)} ms après publication`);
    step("shown", "is-on", "les volets tournent…");
    const t0 = state.pending.t0;
    state.pending = null;
    setTimeout(() => step("shown", "is-done", `volets retournés, ${(Math.round((performance.now() - t0) / 100) / 10).toLocaleString("fr-FR")} s après le clic, animation comprise`), 900);
  }

  // ------------------------------------------------------------ live stream
  function onSnapshot(s) {
    state.offset = new Date(s.serverTime).getTime() - Date.now();
    Object.assign(state, { topic: s.topic, subscription: s.subscription, archive: s.archive });
    $("#topic-name").textContent = s.topic;
    renderLink(s.link);
    state.rows = s.rows || [];
    if (!state.replaying) renderBoard(state.rows);
    renderTrains(state.rows);
    renderMessages(s.recent || []);
    renderStats(s.stats);
  }

  const es = new EventSource("/api/stream");
  es.addEventListener("hello", (e) => onSnapshot(JSON.parse(e.data)));
  es.addEventListener("tick", (e) => {
    const s = JSON.parse(e.data);
    state.offset = new Date(s.serverTime).getTime() - Date.now();
    renderLink(s.link);
    state.rows = s.rows || [];
    if (!state.replaying) renderBoard(state.rows);
    renderTrains(state.rows);
    renderStats(s.stats);
  });
  es.addEventListener("msg", (e) => {
    const { msg, rows } = JSON.parse(e.data);
    const byMaster = msg.event.by === "station-master";
    state.rows = rows || [];
    pulse(byMaster);
    addMessage(msg);
    setTimeout(() => {
      if (!state.replaying) {
        renderBoard(state.rows);
        flashTrain(msg.event.train.id, byMaster);
      }
      renderTrains(state.rows);
      settlePending(msg);
    }, reduced ? 0 : 900); // the board turns when the pulse reaches it
  });
  es.onerror = () => {
    $("#link").dataset.state = "down";
    $("#link-text").textContent = "Flux interrompu, reconnexion…";
  };

  // ------------------------------------------------------------ the latecomer: replay
  const replayBtn = $("#replay"), stopBtn = $("#replay-stop");
  let replayTimer = 0;

  function endReplay(message) {
    clearTimeout(replayTimer);
    state.replaying = false;
    $("#board").classList.remove("is-replay");
    clock.classList.remove("is-replay");
    track("t-reader").classList.remove("is-active");
    node("n-reader").classList.remove("is-active");
    $("#replay-bar").hidden = true;
    $("#board-title").firstChild.textContent = "Départs ";
    replayBtn.hidden = false;
    stopBtn.hidden = true;
    if (message) $("#late-result").textContent = message;
    renderBoard(state.rows);
    setClock(Date.now() + state.offset);
  }

  replayBtn.addEventListener("click", async () => {
    replayBtn.disabled = true;
    replayBtn.textContent = "Lecture du topic…";
    let r;
    try {
      const res = await fetch("/api/replay");
      r = await res.json();
      if (!res.ok) throw new Error(r.error || `Erreur ${res.status}`);
    } catch (e) {
      $("#late-result").textContent = e.message;
      return;
    } finally {
      replayBtn.disabled = false;
      replayBtn.textContent = "Rejouer les 30 dernières minutes";
    }
    state.replaying = true;
    $("#board").classList.add("is-replay");
    clock.classList.add("is-replay");
    track("t-reader").classList.add("is-active");
    node("n-reader").classList.add("is-active");
    $("#replay-bar").hidden = false;
    $("#board-title").firstChild.textContent = "Relecture ";
    replayBtn.hidden = true;
    stopBtn.hidden = false;
    const first = r.first ? ` Le plus ancien message conservé date de ${hm.format(new Date(r.first.publishTime))} (id ${r.first.id}).` : "";
    $("#late-result").textContent = `Un reader relit le topic depuis ${hm.format(new Date(r.from))} : ${num.format(r.count)} messages lus en ${Math.round(r.readMs)} ms.${first}`;
    renderBoard([], true);
    let i = 0;
    const play = () => {
      const f = r.frames[i];
      if (!f) {
        endReplay(`Relecture terminée : ${num.format(r.count)} messages relus en ${Math.round(r.readMs)} ms ont reconstruit le tableau, sans rien demander au producteur.${first}`);
        return;
      }
      renderBoard(f.rows, true);
      setClock(new Date(f.at).getTime());
      $("#replay-label").textContent = `Relecture ${hm.format(new Date(f.at))}`;
      $("#replay-progress").style.width = `${((i + 1) / r.frames.length) * 100}%`;
      for (let k = 0; k < Math.min(f.events, 3); k++) setTimeout(() => travel(track("t-reader"), "#f6b33c", 300, () => light(node("n-reader"), 200)), k * 110);
      i++;
      replayTimer = setTimeout(play, 420);
    };
    replayTimer = setTimeout(play, 700);
  });
  stopBtn.addEventListener("click", () => endReplay(""));

  // ------------------------------------------------------------ sound toggle
  $("#sound").addEventListener("click", (e) => e.currentTarget.setAttribute("aria-pressed", String(sound.toggle())));
})();
