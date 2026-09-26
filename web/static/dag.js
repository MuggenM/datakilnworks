/* DKWDag: a small, dependency-free graph canvas for workflow tasks (Jobs & Pipelines).
 *
 * Nodes are HTML (so they can use the icon font and CSS), edges are one SVG layer behind them; both live in a "world" element that is panned and
 * zoomed with a CSS transform. The canvas never talks to the server: the host (Alpine) owns the task array. Tasks are {id, name, type,
 * depends_on: [id], position?: {x, y}}. The canvas mutates `position` and `depends_on` of the tasks it was given and reports what it did:
 *
 *   const dag = new DKWDag(el, { editable, onSelect(id|null), onEdge(edge|null), onChange(kind, detail), onDeleteRequest(id) })
 *   dag.setTasks(tasks, { fit: true });   dag.setStatuses({ taskId: {status, duration_sec, attempts} });   dag.setEditable(bool)
 *   dag.fit(); dag.zoomBy(1.2); dag.autoLayout(); dag.select(id); dag.viewCenter() -> {x, y} (world coordinates); dag.refresh(); dag.destroy()
 *
 * Editing gestures: drag a node to move it; drag from the round port on a node's right (or left) edge to another node to add a dependency
 * (a cycle, a duplicate or a self reference is refused); click an edge and press Delete (or its x button) to remove it; Delete removes the
 * selected node after the host confirmed (onDeleteRequest). Read-only mode (run view) keeps pan, zoom and selection.
 */
(function (global) {
  'use strict';

  var W = 210, H = 62, GAP_X = 96, GAP_Y = 30;
  var TYPES = {
    sql: { icon: 'ph-database', label: 'SQL', color: '#38bdf8' },
    notebook: { icon: 'ph-notebook', label: 'Notebook', color: '#a78bfa' },
    ingest: { icon: 'ph-download-simple', label: 'Ingest', color: '#34d399' },
    optimize: { icon: 'ph-broom', label: 'Optimize', color: '#fbbf24' },
    dbt: { icon: 'ph-cube', label: 'dbt', color: '#fb7185' }
  };
  var STATUS = {
    SUCCESS: { icon: 'ph-check-circle', cls: 'ok' }, FAILED: { icon: 'ph-x-circle', cls: 'fail' }, RUNNING: { icon: 'ph-spinner', cls: 'run' },
    SKIPPED: { icon: 'ph-minus-circle', cls: 'skip' }, CANCELLED: { icon: 'ph-prohibit', cls: 'skip' }, PENDING: { icon: 'ph-clock', cls: 'pend' }
  };
  var SVGNS = 'http://www.w3.org/2000/svg';

  function el(tag, cls, attrs) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (attrs) for (var k in attrs) e.setAttribute(k, attrs[k]);
    return e;
  }
  function svg(tag, attrs) {
    var e = document.createElementNS(SVGNS, tag);
    if (attrs) for (var k in attrs) e.setAttribute(k, attrs[k]);
    return e;
  }
  function raw(x) { return (global.Alpine && global.Alpine.raw) ? global.Alpine.raw(x) : x; }

  /* ------------------------------------------------------------------ layout (layered, left to right) */
  function layout(tasks) {
    var ids = tasks.map(function (t) { return t.id; }), byId = {};
    tasks.forEach(function (t) { byId[t.id] = t; });
    var rank = {};
    function rankOf(id, seen) {
      if (rank[id] != null) return rank[id];
      if (seen[id]) return 0;                                   // a cycle (not valid, but never loop)
      seen[id] = true;
      var r = 0;
      ((byId[id] && byId[id].depends_on) || []).forEach(function (d) { if (byId[d]) r = Math.max(r, rankOf(d, seen) + 1); });
      rank[id] = r;
      return r;
    }
    ids.forEach(function (id) { rankOf(id, {}); });
    var cols = [];
    ids.forEach(function (id) { (cols[rank[id]] = cols[rank[id]] || []).push(id); });
    var order = {};
    cols.forEach(function (c, ci) { c.forEach(function (id, i) { order[id] = i; }); });
    for (var sweep = 0; sweep < 3; sweep++) {                   // barycentre ordering reduces edge crossings
      for (var ci = 1; ci < cols.length; ci++) {
        var col = cols[ci] || [];
        var bary = {};
        col.forEach(function (id) {
          var ds = (byId[id].depends_on || []).filter(function (d) { return byId[d] && rank[d] === ci - 1; });
          bary[id] = ds.length ? ds.reduce(function (a, d) { return a + order[d]; }, 0) / ds.length : order[id];
        });
        col.sort(function (a, b) { return bary[a] - bary[b] || order[a] - order[b]; });
        col.forEach(function (id, i) { order[id] = i; });
      }
    }
    var pos = {}, maxRows = Math.max.apply(null, [1].concat(cols.map(function (c) { return (c || []).length; })));
    cols.forEach(function (col, ci) {
      col = col || [];
      var off = (maxRows - col.length) * (H + GAP_Y) / 2;
      col.forEach(function (id, i) { pos[id] = { x: ci * (W + GAP_X), y: off + i * (H + GAP_Y) }; });
    });
    return pos;
  }

  /* ------------------------------------------------------------------ the canvas */
  function DKWDag(container, opts) {
    this.opts = opts || {};
    this.root = container;
    this.tasks = [];
    this.statuses = {};
    this.editable = !!this.opts.editable;
    this.view = { x: 40, y: 40, k: 1 };
    this.selected = null;            // node id
    this.selectedEdge = null;        // {from, to}
    this.pos = {};
    this.nodeEls = {};
    this._build();
  }

  DKWDag.prototype._build = function () {
    var self = this;
    this.root.classList.add('dkw-dag');
    this.root.setAttribute('tabindex', '0');
    this.root.innerHTML = '';
    this.world = el('div', 'dag-world');
    this.edgeSvg = svg('svg', { class: 'dag-edges' });
    var defs = svg('defs');
    ['', '-ok', '-fail', '-sel'].forEach(function (s) {
      var m = svg('marker', { id: 'dag-arrow' + s + '-' + (self.opts.uid || 'x'), viewBox: '0 0 10 10', refX: '9', refY: '5', markerWidth: '7', markerHeight: '7', orient: 'auto-start-reverse' });
      m.appendChild(svg('path', { d: 'M0,1 L10,5 L0,9 z', class: 'dag-arrowhead' + s }));
      defs.appendChild(m);
    });
    this.edgeSvg.appendChild(defs);
    this.edgeLayer = svg('g');
    this.tempPath = svg('path', { class: 'dag-edge dag-edge-temp', d: '' });
    this.edgeSvg.appendChild(this.edgeLayer);
    this.edgeSvg.appendChild(this.tempPath);
    this.nodeLayer = el('div', 'dag-nodes');
    this.edgeDel = el('button', 'dag-edge-del', { type: 'button', title: 'Remove this dependency', 'data-testid': 'dag-edge-delete' });
    this.edgeDel.textContent = '×';
    this.edgeDel.style.display = 'none';
    this.edgeDel.addEventListener('click', function (e) { e.stopPropagation(); self.removeSelectedEdge(); });
    this.world.appendChild(this.edgeSvg);
    this.world.appendChild(this.nodeLayer);
    this.world.appendChild(this.edgeDel);
    this.root.appendChild(this.world);

    // toolbar (zoom / fit / auto layout), bottom right like the Databricks canvas
    this.toolbar = el('div', 'dag-toolbar');
    var btn = function (icon, title, fn, tid) {
      var b = el('button', 'dag-tool', { type: 'button', title: title, 'data-testid': tid });
      b.innerHTML = '<i class="ph ' + icon + '"></i>';
      b.addEventListener('click', function (e) { e.stopPropagation(); fn(); });
      self.toolbar.appendChild(b);
      return b;
    };
    btn('ph-corners-out', 'Fit to view', function () { self.fit(); }, 'dag-fit');
    btn('ph-magnifying-glass-plus', 'Zoom in', function () { self.zoomBy(1.2); }, 'dag-zoom-in');
    btn('ph-magnifying-glass-minus', 'Zoom out', function () { self.zoomBy(1 / 1.2); }, 'dag-zoom-out');
    this.layoutBtn = btn('ph-tree-structure', 'Arrange automatically', function () { self.autoLayout(); }, 'dag-auto-layout');
    this.root.appendChild(this.toolbar);

    // minimap
    this.mini = el('div', 'dag-mini', { 'data-testid': 'dag-minimap' });
    this.miniSvg = svg('svg', { width: '160', height: '100' });
    this.mini.appendChild(this.miniSvg);
    this.root.appendChild(this.mini);
    this.mini.addEventListener('pointerdown', function (e) { self._miniPan(e); });

    // interactions on the background: pan, deselect, zoom
    this.root.addEventListener('pointerdown', function (e) {
      if (e.target.closest && (e.target.closest('.dag-node') || e.target.closest('.dag-toolbar') || e.target.closest('.dag-mini') || e.target.closest('.dag-edge-del') || e.target.classList.contains('dag-edge-hit'))) return;
      self.root.focus();
      self._startPan(e);
    });
    this.root.addEventListener('wheel', function (e) {
      e.preventDefault();
      var r = self.root.getBoundingClientRect();
      self.zoomBy(e.deltaY < 0 ? 1.1 : 1 / 1.1, e.clientX - r.left, e.clientY - r.top);
    }, { passive: false });
    this.root.addEventListener('keydown', function (e) {
      if (!self.editable) return;
      if (e.key !== 'Delete' && e.key !== 'Backspace') return;
      var tag = (e.target && e.target.tagName) || '';
      if (/INPUT|TEXTAREA|SELECT/.test(tag)) return;
      if (self.selectedEdge) { e.preventDefault(); self.removeSelectedEdge(); }
      else if (self.selected && self.opts.onDeleteRequest) { e.preventDefault(); self.opts.onDeleteRequest(self.selected); }
    });
    this._ro = (typeof ResizeObserver !== 'undefined') ? new ResizeObserver(function () {
      var r = self.root.getBoundingClientRect();
      if (self._autoFit && r.width && r.height) self.fit(); else self._miniRender();      // still "fit to view" until the user pans or zooms
    }) : null;
    if (this._ro) this._ro.observe(this.root);
    this._applyView();
  };

  DKWDag.prototype.destroy = function () {
    if (this._ro) this._ro.disconnect();
    this.root.innerHTML = '';
    this.root.classList.remove('dkw-dag');
  };

  DKWDag.prototype.setEditable = function (on) {
    this.editable = !!on;
    this.root.classList.toggle('dag-editable', this.editable);
    this.layoutBtn.style.display = this.editable ? '' : 'none';
    if (!this.editable) { this.selectedEdge = null; }
    this.render();
  };

  DKWDag.prototype.setTasks = function (tasks, o) {
    this.tasks = tasks || [];
    if (this.selected && !this.tasks.some(function (t) { return t.id === this.selected; }, this)) this.selected = null;
    this.render();
    if (o && o.fit) this.fit();
  };

  DKWDag.prototype.setStatuses = function (map) {
    this.statuses = map || {};
    this.render();
  };

  DKWDag.prototype.refresh = function () { this.render(); };

  DKWDag.prototype.position = function (t) {
    return t.position ? { x: +t.position.x, y: +t.position.y } : (this.pos[t.id] || { x: 0, y: 0 });
  };

  /* ---------------------------------------------------------------- rendering */
  DKWDag.prototype.render = function () {
    var self = this, tasks = this.tasks.map(raw);
    this.pos = layout(tasks);
    this.nodeLayer.innerHTML = '';
    this.nodeEls = {};
    this.root.classList.toggle('dag-editable', this.editable);
    tasks.forEach(function (t) { self._buildNode(t); });
    this._renderEdges();
    this._miniRender();
  };

  DKWDag.prototype._buildNode = function (t) {
    var self = this, meta = TYPES[t.type] || TYPES.sql, st = this.statuses[t.id], stName = st ? st.status : null, sm = stName ? (STATUS[stName] || STATUS.PENDING) : null;
    var n = el('div', 'dag-node' + (this.selected === t.id ? ' selected' : '') + (sm ? ' st-' + sm.cls : ''), { 'data-id': t.id, 'data-testid': 'dag-node', 'data-status': stName || '' });
    n.style.width = W + 'px';
    n.style.height = H + 'px';
    var p = this.position(t);
    n.style.left = p.x + 'px';
    n.style.top = p.y + 'px';
    n.style.setProperty('--accent', meta.color);
    var icon = el('div', 'dag-node-icon'); icon.innerHTML = '<i class="ph ' + meta.icon + '"></i>';
    var body = el('div', 'dag-node-body');
    var title = el('div', 'dag-node-title'); title.textContent = t.name || t.id; title.title = (t.name || t.id) + ' (' + t.id + ')';
    var sub = el('div', 'dag-node-sub');
    var subText = meta.label;
    if (st && st.duration_sec != null && stName !== 'PENDING' && stName !== 'RUNNING') subText += ' · ' + fmtDuration(st.duration_sec);
    if (st && st.attempts > 1) subText += ' · ' + st.attempts + ' attempts';
    if (stName === 'RUNNING') subText += ' · running';
    if (stName === 'PENDING') subText += ' · waiting';
    sub.textContent = subText;
    body.appendChild(title); body.appendChild(sub);
    n.appendChild(icon); n.appendChild(body);
    if (sm) {
      var badge = el('div', 'dag-node-status ' + sm.cls); badge.innerHTML = '<i class="ph ' + sm.icon + (stName === 'RUNNING' ? ' dag-spin' : '') + '"></i>';
      n.appendChild(badge);
    }
    var pin = el('div', 'dag-port dag-port-in', { 'data-testid': 'dag-port-in', title: 'Drag here from another task to make this task depend on it' });
    var pout = el('div', 'dag-port dag-port-out', { 'data-testid': 'dag-port-out', title: 'Drag to another task to make it depend on this one' });
    n.appendChild(pin); n.appendChild(pout);
    pout.addEventListener('pointerdown', function (e) { if (self.editable) self._startConnect(e, t.id, 'out'); });
    pin.addEventListener('pointerdown', function (e) { if (self.editable) self._startConnect(e, t.id, 'in'); });
    n.addEventListener('pointerdown', function (e) {
      if (e.target.classList.contains('dag-port')) return;
      self._startNodeDrag(e, t);
    });
    this.nodeLayer.appendChild(n);
    this.nodeEls[t.id] = n;
  };

  DKWDag.prototype._edges = function () {
    var byId = {}, out = [];
    this.tasks.forEach(function (t) { byId[raw(t).id] = raw(t); });
    Object.keys(byId).forEach(function (id) {
      (byId[id].depends_on || []).forEach(function (d) { if (byId[d]) out.push({ from: d, to: id }); });
    });
    return out;
  };

  DKWDag.prototype._portPoint = function (id, side) {
    var t = this.tasks.map(raw).filter(function (x) { return x.id === id; })[0];
    if (!t) return { x: 0, y: 0 };
    var p = this.position(t);
    return { x: p.x + (side === 'out' ? W : 0), y: p.y + H / 2 };
  };

  function curve(a, b) {
    var dx = Math.max(46, Math.abs(b.x - a.x) / 2);
    return 'M' + a.x + ',' + a.y + ' C' + (a.x + dx) + ',' + a.y + ' ' + (b.x - dx) + ',' + b.y + ' ' + b.x + ',' + b.y;
  }

  DKWDag.prototype._renderEdges = function () {
    var self = this, uid = this.opts.uid || 'x';
    this.edgeLayer.innerHTML = '';
    var sel = this.selectedEdge, mid = null;
    this._edges().forEach(function (e) {
      var a = self._portPoint(e.from, 'out'), b = self._portPoint(e.to, 'in'), d = curve(a, b);
      var fs = self.statuses[e.from] && self.statuses[e.from].status, isSel = sel && sel.from === e.from && sel.to === e.to;
      var cls = 'dag-edge' + (fs === 'SUCCESS' ? ' ok' : fs === 'FAILED' ? ' fail' : '') + (isSel ? ' selected' : '');
      var marker = isSel ? '-sel' : fs === 'SUCCESS' ? '-ok' : fs === 'FAILED' ? '-fail' : '';
      var g = svg('g', { 'data-from': e.from, 'data-to': e.to, 'data-testid': 'dag-edge' });
      var hit = svg('path', { d: d, class: 'dag-edge-hit' });
      var path = svg('path', { d: d, class: cls, 'marker-end': 'url(#dag-arrow' + marker + '-' + uid + ')' });
      hit.addEventListener('pointerdown', function (ev) {
        ev.stopPropagation();
        self.root.focus();
        self.selectEdge(e);
      });
      g.appendChild(path); g.appendChild(hit);
      self.edgeLayer.appendChild(g);
      if (isSel) mid = { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 };
    });
    if (mid && this.editable) {
      this.edgeDel.style.display = '';
      this.edgeDel.style.left = mid.x + 'px';
      this.edgeDel.style.top = mid.y + 'px';
    } else {
      this.edgeDel.style.display = 'none';
    }
  };

  /* ---------------------------------------------------------------- view */
  DKWDag.prototype._applyView = function () {
    this.world.style.transform = 'translate(' + this.view.x + 'px,' + this.view.y + 'px) scale(' + this.view.k + ')';
    this.root.style.setProperty('--dag-k', this.view.k);
    this.root.style.backgroundPosition = this.view.x + 'px ' + this.view.y + 'px';
    this.root.style.backgroundSize = (24 * this.view.k) + 'px ' + (24 * this.view.k) + 'px';
    this._miniRender();
  };

  DKWDag.prototype.bounds = function () {
    var self = this, ts = this.tasks.map(raw);
    if (!ts.length) return { x: 0, y: 0, w: W, h: H };
    var x0 = 1e9, y0 = 1e9, x1 = -1e9, y1 = -1e9;
    ts.forEach(function (t) { var p = self.position(t); x0 = Math.min(x0, p.x); y0 = Math.min(y0, p.y); x1 = Math.max(x1, p.x + W); y1 = Math.max(y1, p.y + H); });
    return { x: x0, y: y0, w: x1 - x0, h: y1 - y0 };
  };

  DKWDag.prototype.fit = function () {
    var r = this.root.getBoundingClientRect(), b = this.bounds(), pad = 48;
    this._autoFit = true;
    if (!r.width || !r.height) { this._pendingFit = true; return; }
    var k = Math.max(0.25, Math.min(1.2, (r.width - pad * 2) / b.w, (r.height - pad * 2) / b.h));
    this.view.k = k;
    this.view.x = (r.width - b.w * k) / 2 - b.x * k;
    this.view.y = (r.height - b.h * k) / 2 - b.y * k;
    this._applyView();
  };

  DKWDag.prototype.zoomBy = function (f, cx, cy) {
    var r = this.root.getBoundingClientRect();
    this._autoFit = false;
    if (cx == null) { cx = r.width / 2; cy = r.height / 2; }
    var k0 = this.view.k, k1 = Math.max(0.25, Math.min(2, k0 * f));
    this.view.x = cx - (cx - this.view.x) * (k1 / k0);
    this.view.y = cy - (cy - this.view.y) * (k1 / k0);
    this.view.k = k1;
    this._applyView();
  };

  DKWDag.prototype.viewCenter = function () {
    var r = this.root.getBoundingClientRect();
    return { x: (r.width / 2 - this.view.x) / this.view.k - W / 2, y: (r.height / 2 - this.view.y) / this.view.k - H / 2 };
  };

  DKWDag.prototype._toWorld = function (e) {
    var r = this.root.getBoundingClientRect();
    return { x: (e.clientX - r.left - this.view.x) / this.view.k, y: (e.clientY - r.top - this.view.y) / this.view.k };
  };

  DKWDag.prototype._startPan = function (e) {
    var self = this, sx = e.clientX, sy = e.clientY, vx = this.view.x, vy = this.view.y, moved = false;
    this.root.classList.add('panning');
    function move(ev) {
      if (Math.abs(ev.clientX - sx) + Math.abs(ev.clientY - sy) > 3) { moved = true; self._autoFit = false; }
      self.view.x = vx + ev.clientX - sx; self.view.y = vy + ev.clientY - sy;
      self._applyView();
    }
    function up() {
      window.removeEventListener('pointermove', move); window.removeEventListener('pointerup', up);
      self.root.classList.remove('panning');
      if (!moved) { self.select(null); self.selectEdge(null); }
    }
    window.addEventListener('pointermove', move); window.addEventListener('pointerup', up);
  };

  DKWDag.prototype._miniRender = function () {
    if (!this.miniSvg) return;
    var self = this, b = this.bounds(), r = this.root.getBoundingClientRect();
    var vx = -this.view.x / this.view.k, vy = -this.view.y / this.view.k, vw = r.width / this.view.k, vh = r.height / this.view.k;
    var x0 = Math.min(b.x, vx), y0 = Math.min(b.y, vy), x1 = Math.max(b.x + b.w, vx + vw), y1 = Math.max(b.y + b.h, vy + vh);
    var s = Math.min(160 / (x1 - x0 || 1), 100 / (y1 - y0 || 1));
    this._mini = { x0: x0, y0: y0, s: s };
    while (this.miniSvg.firstChild) this.miniSvg.removeChild(this.miniSvg.firstChild);
    this.tasks.map(raw).forEach(function (t) {
      var p = self.position(t), st = self.statuses[t.id], cls = st ? (STATUS[st.status] || STATUS.PENDING).cls : 'none';
      self.miniSvg.appendChild(svg('rect', { x: (p.x - x0) * s, y: (p.y - y0) * s, width: W * s, height: H * s, rx: 2, class: 'dag-mini-node ' + cls }));
    });
    this.miniSvg.appendChild(svg('rect', { x: (vx - x0) * s, y: (vy - y0) * s, width: vw * s, height: vh * s, class: 'dag-mini-view' }));
  };

  DKWDag.prototype._miniPan = function (e) {
    var self = this;
    function go(ev) {
      if (!self._mini) return;
      var r = self.mini.getBoundingClientRect(), m = self._mini, rr = self.root.getBoundingClientRect();
      var wx = (ev.clientX - r.left) / m.s + m.x0, wy = (ev.clientY - r.top) / m.s + m.y0;
      self._autoFit = false;
      self.view.x = rr.width / 2 - wx * self.view.k; self.view.y = rr.height / 2 - wy * self.view.k;
      self._applyView();
    }
    go(e);
    function up() { window.removeEventListener('pointermove', go); window.removeEventListener('pointerup', up); }
    window.addEventListener('pointermove', go); window.addEventListener('pointerup', up);
    e.preventDefault();
  };

  /* ---------------------------------------------------------------- selection */
  DKWDag.prototype.select = function (id) {
    if (this.selected === id) return;
    this.selected = id;
    if (id) this.selectedEdge = null;
    var self = this;
    Object.keys(this.nodeEls).forEach(function (k) { self.nodeEls[k].classList.toggle('selected', k === id); });
    this._renderEdges();
    if (this.opts.onSelect) this.opts.onSelect(id);
  };

  DKWDag.prototype.selectEdge = function (edge) {
    this.selectedEdge = edge ? { from: edge.from, to: edge.to } : null;
    if (edge) this.select(null);
    this._renderEdges();
    if (this.opts.onEdge) this.opts.onEdge(this.selectedEdge);
  };

  /* ---------------------------------------------------------------- editing */
  DKWDag.prototype._task = function (id) { return this.tasks.map(raw).filter(function (t) { return t.id === id; })[0]; };

  DKWDag.prototype._reaches = function (fromId, targetId) {    // does `fromId` (transitively) depend on `targetId`?
    var self = this, seen = {}, stack = [fromId];
    while (stack.length) {
      var id = stack.pop();
      if (id === targetId) return true;
      if (seen[id]) continue;
      seen[id] = true;
      var t = self._task(id);
      ((t && t.depends_on) || []).forEach(function (d) { stack.push(d); });
    }
    return false;
  };

  /* can `to` be made to depend on `from`? returns null or the reason it cannot */
  DKWDag.prototype.connectProblem = function (from, to) {
    if (from === to) return 'A task cannot depend on itself.';
    var t = this._task(to);
    if (!t || !this._task(from)) return 'Unknown task.';
    if ((t.depends_on || []).indexOf(from) >= 0) return 'That dependency already exists.';
    if (this._reaches(from, to)) return 'That would make the tasks depend on each other in a circle.';
    return null;
  };

  DKWDag.prototype.connect = function (from, to) {
    var why = this.connectProblem(from, to);
    if (why) { if (this.opts.onRefuse) this.opts.onRefuse(why); return false; }
    var t = this._task(to);
    t.depends_on = (t.depends_on || []).concat([from]);
    this.render();
    if (this.opts.onChange) this.opts.onChange('connect', { from: from, to: to });
    return true;
  };

  DKWDag.prototype.disconnect = function (from, to) {
    var t = this._task(to);
    if (!t) return;
    t.depends_on = (t.depends_on || []).filter(function (d) { return d !== from; });
    if (this.selectedEdge && this.selectedEdge.from === from && this.selectedEdge.to === to) this.selectedEdge = null;
    this.render();
    if (this.opts.onChange) this.opts.onChange('disconnect', { from: from, to: to });
  };

  DKWDag.prototype.removeSelectedEdge = function () {
    if (this.selectedEdge) this.disconnect(this.selectedEdge.from, this.selectedEdge.to);
  };

  DKWDag.prototype.autoLayout = function () {
    this.tasks.map(raw).forEach(function (t) { delete t.position; });
    this.render();
    this.fit();
    if (this.opts.onChange) this.opts.onChange('layout', {});
  };

  DKWDag.prototype._startNodeDrag = function (e, t) {
    var self = this;
    this.root.focus();
    this.select(t.id);
    if (!this.editable) return;
    e.preventDefault();
    var start = this._toWorld(e), moved = false, task = this._task(t.id), base = this.position(task), all = null;
    function move(ev) {
      var w = self._toWorld(ev), dx = w.x - start.x, dy = w.y - start.y;
      if (!moved && Math.abs(dx) + Math.abs(dy) < 3 / self.view.k) return;
      if (!moved) {                                            // freeze the automatic positions so nothing else jumps
        moved = true;
        self._autoFit = false;
        all = {};
        self.tasks.map(raw).forEach(function (x) { if (!x.position) { var p = self.position(x); x.position = { x: p.x, y: p.y }; } });
      }
      task.position = { x: Math.round((base.x + dx) / 4) * 4, y: Math.round((base.y + dy) / 4) * 4 };
      var n = self.nodeEls[t.id];
      n.style.left = task.position.x + 'px'; n.style.top = task.position.y + 'px';
      self._renderEdges();
      self._miniRender();
    }
    function up() {
      window.removeEventListener('pointermove', move); window.removeEventListener('pointerup', up);
      if (moved && self.opts.onChange) self.opts.onChange('move', { id: t.id });
    }
    window.addEventListener('pointermove', move); window.addEventListener('pointerup', up);
  };

  DKWDag.prototype._startConnect = function (e, id, side) {
    var self = this;
    e.preventDefault(); e.stopPropagation();
    this.root.focus();
    this.root.classList.add('connecting');
    var anchor = this._portPoint(id, side);
    function targetAt(ev) {
      var hit = document.elementFromPoint(ev.clientX, ev.clientY), n = hit && hit.closest ? hit.closest('.dag-node') : null;
      return n && self.root.contains(n) ? n.getAttribute('data-id') : null;
    }
    function markTargets(hoverId) {
      Object.keys(self.nodeEls).forEach(function (k) {
        var from = side === 'out' ? id : k, to = side === 'out' ? k : id, ok = k !== id && !self.connectProblem(from, to);
        self.nodeEls[k].classList.toggle('dag-target-ok', ok);
        self.nodeEls[k].classList.toggle('dag-target-hover', ok && k === hoverId);
      });
    }
    markTargets(null);
    function move(ev) {
      var w = self._toWorld(ev);
      self.tempPath.setAttribute('d', side === 'out' ? curve(anchor, w) : curve(w, anchor));
      markTargets(targetAt(ev));
    }
    function up(ev) {
      window.removeEventListener('pointermove', move); window.removeEventListener('pointerup', up);
      self.root.classList.remove('connecting');
      self.tempPath.setAttribute('d', '');
      Object.keys(self.nodeEls).forEach(function (k) { self.nodeEls[k].classList.remove('dag-target-ok', 'dag-target-hover'); });
      var other = targetAt(ev);
      if (other && other !== id) side === 'out' ? self.connect(id, other) : self.connect(other, id);
      else if (other === id && self.opts.onRefuse) self.opts.onRefuse('A task cannot depend on itself.');
    }
    window.addEventListener('pointermove', move); window.addEventListener('pointerup', up);
  };

  function fmtDuration(s) {
    s = Number(s) || 0;
    if (s < 1) return Math.round(s * 1000) + 'ms';
    if (s < 60) return (s < 10 ? s.toFixed(1) : Math.round(s)) + 's';
    var m = Math.floor(s / 60), r = Math.round(s % 60);
    return m + 'm ' + (r < 10 ? '0' : '') + r + 's';
  }

  DKWDag.layout = layout;
  DKWDag.fmtDuration = fmtDuration;
  DKWDag.TYPES = TYPES;
  DKWDag.NODE = { W: W, H: H };
  global.DKWDag = DKWDag;
})(window);
