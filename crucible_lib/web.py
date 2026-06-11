from __future__ import annotations

import copy
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .core import *
from .status import configured_coordinator_runs
from .tasks import parse_coordinator, shell_join


_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Crucible Status</title>
  <style>
    :root {
      --bg: #0e0e10; --bg2: #16161a; --bg3: #1e1e24;
      --text: #c8c8d2; --dim: #767680; --border: #232329;
      --yel: #d4a822; --grn: #4db36a; --red: #d95f5f; --blu: #5a9ecf;
      --font: 'SF Mono','JetBrains Mono','Fira Code',ui-monospace,monospace;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: var(--bg); color: var(--text); font-family: var(--font);
           font-size: 14px; line-height: 1.4; }
    header { display: flex; align-items: center; gap: 10px; padding: 7px 14px;
             border-bottom: 1px solid var(--border); position: sticky; top: 0;
             background: var(--bg); z-index: 10; font-size: 13px; }
    #dot { width: 8px; height: 8px; border-radius: 50%; background: var(--grn); flex-shrink: 0; }
    #dot.err { background: var(--red); }
    #coord { color: var(--dim); }
    #ts { color: var(--dim); margin-left: auto; }
    #sound { background: none; border: 1px solid var(--border); border-radius: 4px;
             color: var(--dim); font-family: var(--font); font-size: 11px;
             padding: 1px 7px; cursor: pointer; }
    #sound.on { color: var(--yel); border-color: rgba(212,168,34,.45); }
    #sound:hover { color: var(--text); border-color: var(--dim); }
    #err { display: none; padding: 6px 14px; background: rgba(217,95,95,.1);
           color: var(--red); border-bottom: 1px solid rgba(217,95,95,.2); font-size: 13px; }
    #runs { padding: 10px 12px; display: flex; flex-direction: column; gap: 6px; }

    /* run card */
    .run { border: 1px solid var(--border); border-radius: 7px; overflow: hidden; }
    .run.current { border-color: #2a3848; }
    .run.failed  { border-color: #3d2424; }
    .run.done    { border-color: var(--border); opacity: .65; }

    /* run header — always visible */
    .run-hdr { cursor: pointer; user-select: none; background: var(--bg2); padding: 8px 12px; }
    .run-hdr:hover { background: var(--bg3); }
    .run-hdr-top { display: flex; align-items: center; gap: 8px; }
    .arrow { color: var(--dim); font-size: 10px; flex-shrink: 0; transition: transform .15s; }
    .arrow.open { transform: rotate(90deg); }
    .run-name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis;
                white-space: nowrap; font-weight: 600; }
    .badge { font-size: 11px; font-weight: 700; padding: 2px 8px; border-radius: 10px; flex-shrink: 0; }
    .badge.current { background: rgba(212,168,34,.15); color: var(--yel); }
    .badge.failed  { background: rgba(217,95,95,.15);  color: var(--red); }
    .badge.done    { background: rgba(77,179,106,.12);  color: var(--grn); }
    .badge.queued  { background: rgba(118,118,128,.15); color: var(--dim); }
    .badge.empty   { background: rgba(118,118,128,.12); color: var(--dim); }
    /* summary line below the run name — always visible */
    .run-summary { display: flex; align-items: center; gap: 12px; margin-top: 4px;
                   padding-left: 18px; font-size: 13px; flex-wrap: wrap; }
    .run-desc  { color: var(--dim); }
    .run-fields { color: var(--text); }
    .run-eta   { color: var(--yel); font-weight: 600; }
    .elo-pos { color: var(--grn); font-weight: 600; }
    .elo-neu { color: var(--yel); font-weight: 600; }
    .elo-neg { color: var(--red); font-weight: 600; }

    /* progress bar */
    .pbar-wrap { height: 2px; background: var(--bg3); }
    .pbar-fill { height: 100%; transition: width .6s ease; }
    .current .pbar-fill { background: var(--yel); }
    .failed  .pbar-fill { background: var(--red); }
    .done    .pbar-fill { background: var(--grn); }

    /* body — collapsible */
    .run-body { display: none; border-top: 1px solid var(--border); }
    .run-body.open { display: block; }

    /* sections */
    .section { padding: 6px 12px; }
    .section + .section { border-top: 1px solid var(--border); }
    .section-hdr { display: flex; align-items: center; gap: 6px; cursor: pointer;
                   user-select: none; padding: 2px 0; }
    .section-hdr:hover .section-title { color: var(--text); }
    .section-title { font-size: 13px; }
    .section-title.yel { color: var(--yel); }
    .section-title.red { color: var(--red); }
    .section-title.dim { color: var(--dim); }
    .section-detail { display: none; padding-top: 4px; }
    .section-detail.open { display: block; }

    /* worker summary chips */
    .ws-chips { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 6px; }
    .chip { font-size: 12px; color: var(--dim); background: var(--bg3);
            padding: 2px 7px; border-radius: 4px; }
    .chip.hi { color: var(--yel); }

    /* task table */
    .task-table { border-collapse: collapse; font-size: 13px; white-space: nowrap; }
    .task-th { color: var(--dim); font-size: 11px; font-weight: normal;
               padding: 2px 14px 4px 0; position: relative;
               border-bottom: 1px solid var(--border); user-select: none; }
    .task-td { padding: 2px 14px 2px 0; }
    .task-row:hover { background: var(--bg3); }
    .td-worker { color: var(--blu); }
    .td-label  { color: var(--dim); }
    .td-id     { color: var(--dim); }
    .td-progress { color: var(--text); }
    .td-output { color: var(--text); }
    .td-rate   { color: var(--dim); }
    .td-eta    { color: var(--yel); }
    .col-resize { position: absolute; right: 0; top: 0; bottom: 0; width: 8px;
                  cursor: col-resize; }
    .col-resize:hover::after, .col-resize.active::after {
      content: ''; position: absolute; left: 3px; top: 0; bottom: 0;
      width: 2px; background: var(--dim); border-radius: 1px; }
    .task-th { cursor: pointer; }
    .task-th.sorted-asc .sort-ind::after  { content: ' ▲'; font-size: 9px; color: var(--yel); }
    .task-th.sorted-desc .sort-ind::after { content: ' ▼'; font-size: 9px; color: var(--yel); }

    /* pending / fail groups */
    .group-row { font-size: 13px; color: var(--dim); padding: 2px 0; }

    .empty { color: var(--dim); padding: 10px 0; font-size: 13px; }

    /* action buttons */
    .run-actions { display: flex; gap: 5px; flex-shrink: 0; }
    .act { background: none; border: 1px solid var(--border); border-radius: 4px;
           color: var(--dim); font-family: var(--font); font-size: 11px;
           padding: 1px 7px; cursor: pointer; }
    .act:hover { color: var(--text); border-color: var(--dim); }
    .act.stop   { border-color: rgba(217,95,95,.4);  color: var(--red); }
    .act.stop:hover   { background: rgba(217,95,95,.1); }
    .act.resume { border-color: rgba(77,179,106,.4); color: var(--grn); }
    .act.resume:hover { background: rgba(77,179,106,.1); }
  </style>
</head>
<body>
  <div id="err"></div>
  <header>
    <div id="dot"></div>
    <span id="coord"></span>
    <span id="ts">Loading…</span>
    <button id="sound" type="button" onclick="toggleSound()" title="Toggle job start sound" aria-pressed="false">ding off</button>
  </header>
  <div id="runs"></div>
  <script>
    function esc(s) {
      return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    function fmtRanges(vals) {
      const ints = (vals||[]).filter(v=>Number.isInteger(v)).sort((a,b)=>a-b);
      if (!ints.length) return (vals||[]).map(String).join(',');
      const parts=[]; let s=ints[0],p=ints[0];
      for(let i=1;i<ints.length;i++){
        if(ints[i]===p+1){p=ints[i];continue;}
        parts.push(s===p?`${s}`:`${s}-${p}`); s=p=ints[i];
      }
      parts.push(s===p?`${s}`:`${s}-${p}`);
      return parts.join(',');
    }

    function etaFromFields(fields) {
      const f = (fields||[]).find(f=>f.startsWith('eta='));
      return f ? f.slice(4) : null;
    }

    function eloClass(value) {
      const n = parseFloat(String(value).replace(/%$/, ''));
      if (Number.isNaN(n) || n === 0) return ' elo-neu';
      return n > 0 ? ' elo-pos' : ' elo-neg';
    }

    function eloFieldClass(field) {
      const m = String(field).match(/^elo~?=(.*)$/i);
      return m ? eloClass(m[1]) : '';
    }

    function renderField(field) {
      return `<span class="field${eloFieldClass(field)}">${esc(field)}</span>`;
    }

    function fmtAge(s) {
      if (s < 0) return '';
      if (s < 60) return Math.round(s) + 's';
      if (s < 3600) return Math.floor(s/60) + 'm';
      return Math.floor(s/3600) + 'h' + String(Math.floor((s%3600)/60)).padStart(2,'0') + 'm';
    }

    function parseEtaStr(str) {
      if (!str || str === 'unknown') return null;
      let s = 0;
      const h = str.match(/(\d+)h/); if (h) s += parseInt(h[1]) * 3600;
      const m = str.match(/(\d+)m/); if (m) s += parseInt(m[1]) * 60;
      const sec = str.match(/(\d+)s/); if (sec) s += parseInt(sec[1]);
      return s > 0 ? s : null;
    }

    function runEtaS(r) {
      let max=null;
      for(const t of (r.sections?.active?.tasks||[])){
        const e=remainingEtaS(t.progress_data);
        if(typeof e==='number'&&(max===null||e>max)) max=e;
      }
      return max;
    }

    function statePri(r) {
      return {current:0,failed:1,queued:2,done:3,empty:4}[r.state]??5;
    }

    let soundEnabled = localStorage.getItem('crucible.sound') === '1';
    let audioCtx = null;
    let activeJobKeys = null;

    function updateSoundButton() {
      const btn = document.getElementById('sound');
      if (!btn) return;
      btn.classList.toggle('on', soundEnabled);
      btn.setAttribute('aria-pressed', soundEnabled ? 'true' : 'false');
      btn.textContent = soundEnabled ? 'notify on' : 'ding off';
    }

    function ensureAudioContext() {
      const Audio = window.AudioContext || window.webkitAudioContext;
      if (!Audio) return null;
      if (!audioCtx) audioCtx = new Audio();
      if (audioCtx.state === 'suspended') audioCtx.resume().catch(()=>{});
      return audioCtx;
    }

    function toggleSound() {
      soundEnabled = !soundEnabled;
      localStorage.setItem('crucible.sound', soundEnabled ? '1' : '0');
      if (soundEnabled) ensureAudioContext();
      updateSoundButton();
    }

    function playDing() {
      if (!soundEnabled) return;
      const ctx = ensureAudioContext();
      if (!ctx) return;
      const now = ctx.currentTime;
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = 'sine';
      osc.frequency.setValueAtTime(880, now);
      osc.frequency.exponentialRampToValueAtTime(1320, now + 0.08);
      gain.gain.setValueAtTime(0.0001, now);
      gain.gain.exponentialRampToValueAtTime(0.16, now + 0.01);
      gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.18);
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.start(now);
      osc.stop(now + 0.2);
    }

    function runningJobKeys(data) {
      const keys = new Set();
      for (const run of (data.runs||[])) {
        for (const task of (run.sections?.active?.tasks||[])) {
          if ((task.state||'running') !== 'running') continue;
          keys.add([run.run, task.id ?? task.index, task.worker || ''].join('\x1f'));
        }
      }
      return keys;
    }

    function notifyStartedJobs(data) {
      const next = runningJobKeys(data);
      if (activeJobKeys === null) {
        activeJobKeys = next;
        return;
      }
      let started = 0;
      for (const key of next) {
        if (!activeJobKeys.has(key)) started += 1;
      }
      activeJobKeys = next;
      if (started > 0) playDing();
    }

    // URL hash persistence
    function saveToHash() {
      try {
        location.replace('#' + btoa(JSON.stringify({o: openState, s: taskSort, c: taskColWidths})));
      } catch(e) {}
    }
    function loadFromHash() {
      try {
        if (!location.hash) return;
        const st = JSON.parse(atob(location.hash.slice(1)));
        if (st.o) Object.assign(openState, st.o);
        if (st.s) Object.assign(taskSort, st.s);
        if (st.c) taskColWidths = st.c;
      } catch(e) {}
    }

    // Column widths and sort state — persisted across refreshes
    let taskColWidths = null;
    let taskSort = {col: 'eta', dir: 1};

    const BASE_TASK_COLS = ['worker','label','task','progress','output','rate'];
    const FIELD_TASK_COLS = [
      {name: 'elo',  keys: ['elo','elo~','Elo']},
      {name: 'ci',   keys: ['ci','err','error','+/-','elo_ci']},
      {name: 'los',  keys: ['los','LOS']},
      {name: 'draw', keys: ['draw','DrawRatio','draw_ratio']},
    ];

    function sortByCol(col) {
      if (taskSort.col === col) taskSort.dir *= -1;
      else { taskSort.col = col; taskSort.dir = 1; }
      applySortToAllTables();
      saveToHash();
    }

    function applySortToAllTables() {
      document.querySelectorAll('.task-table').forEach(tbl => {
        const headers = [...tbl.querySelectorAll('.task-th')];
        const ci = headers.findIndex(th => th.dataset.col === taskSort.col);
        if (ci < 0) return;
        const tbody = tbl.querySelector('tbody');
        if (!tbody) return;
        const rows = [...tbody.querySelectorAll('tr')];
        rows.sort((a, b) => {
          const av = a.cells[ci]?.dataset.sort ?? '';
          const bv = b.cells[ci]?.dataset.sort ?? '';
          const an = parseFloat(av), bn = parseFloat(bv);
          if (!isNaN(an) && !isNaN(bn)) return (an - bn) * taskSort.dir;
          return av.localeCompare(bv) * taskSort.dir;
        });
        rows.forEach(r => tbody.appendChild(r));
        tbl.querySelectorAll('.task-th').forEach((th, i) => {
          th.classList.toggle('sorted-asc',  i === ci && taskSort.dir ===  1);
          th.classList.toggle('sorted-desc', i === ci && taskSort.dir === -1);
        });
      });
    }

    function initResizableTables() {
      document.querySelectorAll('.task-table').forEach(tbl => {
        const colEls = tbl.querySelectorAll('col');
        if (taskColWidths) {
          taskColWidths.forEach((w, i) => { if (colEls[i]) colEls[i].style.width = w + 'px'; });
        }
        tbl.querySelectorAll('.col-resize').forEach((handle, col) => {
          handle.addEventListener('mousedown', e => {
            e.preventDefault();
            e.stopPropagation();
            handle.classList.add('active');
            const startX = e.clientX;
            const startW = colEls[col] ? colEls[col].offsetWidth : 80;
            const onMove = e => {
              const w = Math.max(20, startW + e.clientX - startX);
              document.querySelectorAll('.task-table col:nth-child('+(col+1)+')').forEach(c => c.style.width = w+'px');
              if (!taskColWidths) taskColWidths = [...colEls].map(c=>c.offsetWidth);
              taskColWidths[col] = w;
            };
            const onUp = () => {
              handle.classList.remove('active');
              document.removeEventListener('mousemove', onMove);
              document.removeEventListener('mouseup', onUp);
              saveToHash();
            };
            document.addEventListener('mousemove', onMove);
            document.addEventListener('mouseup', onUp);
          });
        });
      });
    }

    // Stable open-state memory: maps element id -> bool
    // null means "not yet seen" (use default), true/false means user has interacted
    const openState = {};

    function sid(run, section) {
      return 'sd_' + (run + '_' + section).replace(/[^a-zA-Z0-9]/g, '_');
    }
    function rbid(run) {
      return 'rb_' + run.replace(/[^a-zA-Z0-9]/g, '_');
    }

    function saveState() {
      document.querySelectorAll('.run-body, .section-detail').forEach(el => {
        if (el.id) openState[el.id] = el.classList.contains('open');
      });
    }

    function resolveOpen(id, defaultOpen) {
      return id in openState ? openState[id] : defaultOpen;
    }

    function toggleId(id) {
      const el = document.getElementById(id);
      if (!el) return;
      const nowOpen = !el.classList.contains('open');
      el.classList.toggle('open');
      openState[id] = nowOpen;
      const hdr = el.previousElementSibling;
      if (hdr) { const a = hdr.querySelector('.arrow'); if (a) a.classList.toggle('open', nowOpen); }
      saveToHash();
    }

    function makeSection(run, section, titleHtml, titleCls, detailHtml, defaultOpen) {
      const id = sid(run, section);
      const open = resolveOpen(id, defaultOpen);
      return `<div class="section">
        <div class="section-hdr" onclick="toggleId('${id}')">
          <span class="arrow${open?' open':''}">&#9654;</span>
          <span class="section-title ${titleCls||''}">${titleHtml}</span>
        </div>
        <div class="section-detail${open?' open':''}" id="${id}">${detailHtml}</div>
      </div>`;
    }

    function fmtEta(s) {
      if (s == null || s < 0) return '';
      if (s < 60)   return Math.round(s) + 's';
      if (s < 3600) { const m=Math.floor(s/60),r=Math.round(s%60); return `${m}m${String(r).padStart(2,'0')}s`; }
      const h=Math.floor(s/3600),m=Math.floor((s%3600)/60); return `${h}h${String(m).padStart(2,'0')}m`;
    }

    function progressEtaFromRate(pd) {
      if (!pd || typeof pd.done !== 'number' || typeof pd.total !== 'number') return null;
      if (pd.total <= 0 || pd.done >= pd.total) return null;
      if (typeof pd.rate !== 'number' || pd.rate <= 0) return null;
      return Math.max(0, (pd.total - pd.done) / pd.rate);
    }

    function remainingEtaS(pd) {
      if (!pd) return null;
      if (typeof pd.eta_s !== 'number') return progressEtaFromRate(pd);
      const base = lastFetchTime ?? Date.now()/1000;
      let remaining = pd.eta_s;
      if (typeof pd.eta_epoch === 'number') {
        remaining -= Math.max(0, base - pd.eta_epoch);
      }
      if (pd.eta_overdue && remaining <= 0) return null;
      return Math.max(0, remaining);
    }

    function fmtRate(r, unit) {
      if (r == null || r <= 0) return '';
      const u = unit || '';
      if (r >= 100)  return Math.round(r) + (u ? ' '+u+'/s' : '/s');
      if (r >= 1)    return r.toFixed(1)  + (u ? ' '+u+'/s' : '/s');
      return r.toFixed(2) + (u ? ' '+u+'/s' : '/s');
    }

    function firstProgressField(pd, keys) {
      const fields = pd?.fields || {};
      for (const key of keys) {
        if (fields[key] != null) return String(fields[key]);
      }
      return '';
    }

    function taskProgressText(t) {
      const pd = t.progress_data;
      if (!pd) return t.progress || '';
      const unit = pd.unit || 'items';
      if (typeof pd.global_done === 'number' && typeof pd.global_total === 'number') {
        const local = (typeof pd.done === 'number' && typeof pd.total === 'number')
          ? ` (${pd.done}/${pd.total})` : '';
        return `${unit}=${pd.global_done}/${pd.global_total}${local}`;
      }
      if (typeof pd.done === 'number' && typeof pd.total === 'number') {
        return `${unit}=${pd.done}/${pd.total}`;
      }
      return t.progress || '';
    }

    function taskColumns(tasks) {
      const cols = BASE_TASK_COLS.slice();
      for (const field of FIELD_TASK_COLS) {
        if ((tasks||[]).some(t => firstProgressField(t.progress_data, field.keys))) {
          cols.push(field.name);
        }
      }
      cols.push('eta');
      return cols;
    }

    function taskCell(t, col) {
      const pd = t.progress_data;
      const field = FIELD_TASK_COLS.find(f => f.name === col);
      if (field) {
        const value = firstProgressField(pd, field.keys);
        if (field.name === 'elo') return `<span class="${eloClass(value).trim()}">${esc(value)}</span>`;
        return esc(value);
      }
      if (col === 'worker') {
        const hbTime = t.state==='stale' && pd?.eta_epoch!=null && pd?.eta_s!=null
          ? pd.eta_epoch - pd.eta_s : null;
        const stTag = t.state&&t.state!=='running'
          ? ` <span style="color:var(--dim);font-size:11px"${hbTime!=null?` data-hbt="${hbTime}"`:''}>[${esc(t.state)}${hbTime!=null?' '+fmtAge(Date.now()/1000-hbTime):''}]</span>` : '';
        return `${esc(t.worker||'?')}${stTag}`;
      }
      if (col === 'label') return esc(t.label||'');
      if (col === 'task') return String(t.index);
      if (col === 'progress') return esc(taskProgressText(t));
      if (col === 'output') return esc(pd?.output!=null ? String(pd.output) : '');
      if (col === 'rate') return esc(fmtRate(pd?.rate, pd?.unit));
      if (col === 'eta') {
        const etaSec = remainingEtaS(pd);
        return etaSec!=null ? esc(fmtEta(etaSec)) : '';
      }
      return '';
    }

    function taskSortValue(t, col) {
      const pd = t.progress_data;
      const field = FIELD_TASK_COLS.find(f => f.name === col);
      if (field) {
        const value = firstProgressField(pd, field.keys);
        const number = parseFloat(value.replace(/%$/, ''));
        return value && !Number.isNaN(number) ? String(number) : value;
      }
      if (col === 'worker') return t.worker || '';
      if (col === 'label') return t.label || '';
      if (col === 'task') return String(t.index);
      if (col === 'progress') return pd?.global_done ?? pd?.done ?? '';
      if (col === 'output') return pd?.output ?? '';
      if (col === 'rate') return pd?.rate ?? '';
      if (col === 'eta') return remainingEtaS(pd) ?? 1e9;
      return '';
    }

    function taskCellClass(col) {
      if (FIELD_TASK_COLS.some(f => f.name === col)) return 'td-output';
      if (col === 'worker') return 'td-worker';
      if (col === 'label') return 'td-label';
      if (col === 'task') return 'td-id';
      if (col === 'progress') return 'td-progress';
      if (col === 'output') return 'td-output';
      if (col === 'rate') return 'td-rate';
      if (col === 'eta') return 'td-eta';
      return '';
    }

    // Live ETA countdown — tick every second using eta_s relative to fetch time
    let lastFetchTime = null;
    function tickEtas() {
      if (lastFetchTime == null) return;
      const elapsed = Date.now() / 1000 - lastFetchTime;
      const now = Date.now() / 1000;
      document.querySelectorAll('[data-etas]').forEach(el => {
        const remaining = parseFloat(el.dataset.etas) - elapsed;
        el.textContent = remaining > 0 ? fmtEta(remaining) : '—';
      });
      document.querySelectorAll('[data-hbt]').forEach(el => {
        const age = now - parseFloat(el.dataset.hbt);
        el.textContent = '[stale ' + fmtAge(age) + ']';
      });
    }
    setInterval(tickEtas, 1000);

    function renderTasks(tasks) {
      if (!tasks||!tasks.length) return '';
      const cols = taskColumns(tasks);
      const cg = '<colgroup>' + cols.map(()=>'<col>').join('') + '</colgroup>';
      const thead = '<thead><tr>' + cols.map(c =>
        `<th class="task-th" data-col="${esc(c)}" onclick="sortByCol('${c}')">`
        + `<span class="sort-ind">${c}</span>`
        + `<span class="col-resize" onclick="event.stopPropagation()"></span></th>`
      ).join('') + '</tr></thead>';
      const tbody = '<tbody>' + tasks.map(t => {
        const pd = t.progress_data;
        const etaSec  = remainingEtaS(pd);
        const etaAttr = etaSec!=null ? ` data-etas="${etaSec}"` : '';
        return `<tr class="task-row">` + cols.map(c =>
          `<td class="task-td ${taskCellClass(c)}" data-sort="${esc(taskSortValue(t, c))}"${c==='eta'?etaAttr:''}>${taskCell(t, c)}</td>`
        ).join('') + `</tr>`;
      }).join('') + '</tbody>';
      return `<table class="task-table">${cg}${thead}${tbody}</table>`;
    }

    function renderGroups(groups) {
      return Object.entries(groups||{}).sort().map(([lbl, vals]) =>
        `<div class="group-row">${esc(lbl||'unlabeled')} [${vals.length}]: ${fmtRanges(vals)}</div>`
      ).join('');
    }

    function renderChips(str) {
      return '<div class="ws-chips">' + (str||'').split(' ').filter(Boolean).map(tok => {
        const elo = eloFieldClass(tok);
        const hi = tok.startsWith('speedup=') || tok.startsWith('output=') || tok.startsWith('elo=');
        return `<span class="chip${elo || (hi?' hi':'')}">${esc(tok)}</span>`;
      }).join(' ') + '</div>';
    }

    function renderRun(r) {
      const state  = r.state||'empty';
      const active  = r.sections?.active  || {count:0, tasks:[]};
      const pending = r.sections?.pending || {count:0, groups:{}};
      const fail    = r.sections?.fail    || {count:0, groups:{}};
      const display = r.display || {};
      const isCurrent = state === 'current';

      const pct = r.tasks > 0 ? (r.done/r.tasks*100).toFixed(1) : 0;
      const eta = etaFromFields(display.fields);
      const countStr = (() => {
        const parts = (display.fields||[]).filter(f=>!f.startsWith('eta='));
        return parts.join(' ');
      })();

      const rId = rbid(r.run);
      const bodyOpen = resolveOpen(rId, isCurrent);

      const nonEtaFields = (display.fields||[]).filter(f=>!f.startsWith('eta='));
      const descText = display.description || '';

      const actions = [];
      if (state === 'current') actions.push(`<button class="act stop"   onclick="event.stopPropagation();runCmd('${esc(r.run)}','stop',this)">stop</button>`);
      if (state !== 'done' && state !== 'empty') actions.push(`<button class="act resume" onclick="event.stopPropagation();runCmd('${esc(r.run)}','resume',this)">resume</button>`);

      // header — always visible
      const hdr = `
        <div class="run-hdr-top">
          <span class="arrow${bodyOpen?' open':''}">&#9654;</span>
          <span class="run-name">${esc(r.run)}</span>
          <div class="run-actions">${actions.join('')}</div>
          <span class="badge ${state}">${state==='current'?'active':state}</span>
        </div>
        <div class="run-summary">
          ${descText ? `<span class="run-desc">${esc(descText)}</span>` : ''}
          <span class="run-fields">${nonEtaFields.map(renderField).join(' &nbsp;')}</span>
          ${eta ? `<span class="run-eta" data-etas="${parseEtaStr(eta)??''}">${esc(eta)}</span>` : ''}
        </div>`;

      // body sections (no description here — it's in the header now)
      const sections = [];

      // active — open by default for current runs
      if (active.count > 0) {
        const activeFieldText = (display.active_fields || active.fields || []).join(' ');
        const ws = display.active_summary_text ? renderChips(display.active_summary_text) : renderChips(activeFieldText);
        sections.push(makeSection(r.run, 'active',
          `active: <b>${active.count}</b>`, 'yel',
          ws + renderTasks(active.tasks), isCurrent
        ));
      }

      // pending — open by default
      if (pending.count > 0) {
        sections.push(makeSection(r.run, 'pending',
          `pending: <b>${pending.count}</b>`, 'yel',
          renderGroups(pending.groups), true
        ));
      }

      // fail — open if there are failures
      if (fail.count > 0) {
        sections.push(makeSection(r.run, 'fail',
          `fail: <b>${fail.count}</b>`, 'red',
          renderGroups(fail.groups), true
        ));
      }

      return `<div class="run ${state}">
        <div class="run-hdr" onclick="toggleId('${rId}')">${hdr}</div>
        <div class="pbar-wrap"><div class="pbar-fill" style="width:${pct}%"></div></div>
        <div class="run-body${bodyOpen?' open':''}" id="${rId}">${sections.join('')}</div>
      </div>`;
    }

    async function runCmd(run, cmd, btn) {
      btn.disabled = true;
      btn.textContent = '…';
      try {
        const res = await fetch('/api/cmd', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({run, cmd}),
        });
        const data = await res.json();
        if (!data.ok) {
          document.getElementById('err').style.display = 'block';
          document.getElementById('err').textContent = (data.stderr || data.error || 'command failed').trim();
        }
      } catch(e) {
        document.getElementById('err').style.display = 'block';
        document.getElementById('err').textContent = e.message;
      }
      refresh();
    }

    let refreshing = false;
    async function refresh() {
      if (refreshing) return;
      refreshing = true;
      try {
        const res = await fetch('/api/status');
        if (!res.ok) throw new Error('HTTP '+res.status);
        const data = await res.json();
        document.getElementById('err').style.display = 'none';
        document.getElementById('dot').className = '';
        document.getElementById('coord').textContent =
          data.coordinator ? `coordinator: ${data.coordinator}` : '';
        lastFetchTime = data.timestamp ?? Date.now()/1000;
        const lag = Math.round(Date.now()/1000 - lastFetchTime);
        document.getElementById('ts').textContent = new Date().toLocaleTimeString()
          + (lag > 2 ? ` (${lag}s lag)` : '');
        if (data.error) {
          document.getElementById('err').style.display = 'block';
          document.getElementById('err').textContent = data.error;
        }
        if (!data.loading) notifyStartedJobs(data);

        const runs = (data.runs||[]).slice().sort((a,b) => {
          const dp = statePri(a)-statePri(b);
          if (dp) return dp;
          if (a.state==='current') {
            const ea=runEtaS(a)??1e9, eb=runEtaS(b)??1e9;
            if (ea!==eb) return ea-eb;
          }
          return a.run<b.run?-1:a.run>b.run?1:0;
        });

        saveState();
        const el = document.getElementById('runs');
        const parts = [];
        if (data.collapsed_failures_count > 0) {
          const n = data.collapsed_failures_count;
          parts.push(`<div class="empty" style="color:var(--red)">failed: ${n} run${n===1?'':'s'} — use crucible status --failed</div>`);
        }
        if (runs.length) {
          parts.push(...runs.map(renderRun));
        } else if (data.loading) {
          parts.push('<div class="empty">loading Crucible status...</div>');
        } else if (!data.collapsed_failures_count) {
          parts.push('<div class="empty">no active Crucible runs found</div>');
        }
        el.innerHTML = parts.join('');
        initResizableTables();
        applySortToAllTables();
      } catch(e) {
        document.getElementById('dot').className = 'err';
        document.getElementById('err').style.display = 'block';
        document.getElementById('err').textContent = 'Error: '+e.message;
        document.getElementById('ts').textContent = 'failed '+new Date().toLocaleTimeString();
      } finally {
        refreshing = false;
      }
    }

    loadFromHash();
    updateSoundButton();
    document.addEventListener('click', () => {
      if (soundEnabled) ensureAudioContext();
    }, {once: true});
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>"""

_HTML_BYTES = _HTML.encode()

_STATUS_CACHE_SECONDS = 3.0
_STATUS_CACHE_LOCK = threading.Lock()
_STATUS_CACHE: dict[str, Any] | None = None
_STATUS_CACHE_EPOCH = 0.0
_STATUS_CACHE_ERROR: str | None = None
_STATUS_REFRESHING = False
_WEB_STATUS_TIMEOUT_SECONDS = 10.0
_WEB_SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=5",
    "-o", "ServerAliveInterval=10",
    "-o", "ServerAliveCountMax=1",
]

def _collect_status_payload() -> dict[str, Any]:
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs:
        local_payload = _collect_local_status_payload()
        if _status_payload_has_current_run(local_payload):
            return local_payload
        return _collect_coordinator_status_payload(coordinator_runs)
    return _collect_local_status_payload()

def _status_payload_has_current_run(payload: dict[str, Any]) -> bool:
    for run in payload.get("runs") or []:
        counts = run.get("counts") if isinstance(run, dict) else None
        if not isinstance(counts, dict):
            continue
        active = int(counts.get("running") or 0) + int(counts.get("claimed") or 0) + int(counts.get("stale") or 0)
        started = active + int(counts.get("done") or 0) + int(counts.get("fail") or 0)
        if active > 0 or (started > 0 and int(counts.get("pending") or 0) > 0):
            return True
    return False

def _collect_local_status_payload() -> dict[str, Any]:
    env = os.environ.copy()
    env[COORDINATOR_RUNS_ENV] = ""
    result = subprocess.run(
        [sys.executable, str(script_path()), "status", "--json"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
        timeout=_WEB_STATUS_TIMEOUT_SECONDS,
    )
    if result.returncode not in (0, 1):
        raise RuntimeError(result.stderr.strip() or f"crucible status exit {result.returncode}")
    data = json.loads(result.stdout)
    data["coordinator"] = None
    return data

def _collect_coordinator_status_payload(coordinator_runs: str) -> dict[str, Any]:
    host, runs_path = parse_coordinator(coordinator_runs)
    command = ["status", "--json"]
    if host:
        remote = (
            "command -v crucible >/dev/null 2>&1 || "
            "{ echo 'crucible not found in PATH on coordinator' >&2; exit 127; }; "
            f"exec env {shlex.quote(COORDINATOR_RUNS_ENV)}= "
            f"{shlex.quote(COLOR_ENV)}=never "
            f"{shlex.quote(RUNS_DIR_ENV)}={shlex.quote(runs_path)} "
            f"crucible {shell_join(command)}"
        )
        result = subprocess.run(
            ["ssh", *_WEB_SSH_OPTS, host, remote],
            capture_output=True,
            text=True,
            check=False,
            timeout=_WEB_STATUS_TIMEOUT_SECONDS,
        )
    else:
        env = os.environ.copy()
        env[COORDINATOR_RUNS_ENV] = ""
        env[COLOR_ENV] = "never"
        env[RUNS_DIR_ENV] = runs_path
        result = subprocess.run(
            [sys.executable, str(script_path()), *command],
            capture_output=True,
            text=True,
            check=False,
            env=env,
            timeout=_WEB_STATUS_TIMEOUT_SECONDS,
        )
    if result.returncode not in (0, 1):
        raise RuntimeError(result.stderr.strip() or f"crucible status exit {result.returncode}")
    data = json.loads(result.stdout)
    data["coordinator"] = coordinator_runs
    return data

def _refresh_status_cache() -> None:
    global _STATUS_CACHE, _STATUS_CACHE_EPOCH, _STATUS_CACHE_ERROR, _STATUS_REFRESHING
    try:
        data = _collect_status_payload()
        fetched_at = time.time()
        data["timestamp"] = fetched_at
        with _STATUS_CACHE_LOCK:
            _STATUS_CACHE = data
            _STATUS_CACHE_EPOCH = fetched_at
            _STATUS_CACHE_ERROR = None
    except Exception as exc:
        with _STATUS_CACHE_LOCK:
            _STATUS_CACHE_ERROR = str(exc)
    finally:
        with _STATUS_CACHE_LOCK:
            _STATUS_REFRESHING = False

def _start_status_refresh() -> None:
    global _STATUS_REFRESHING
    with _STATUS_CACHE_LOCK:
        if _STATUS_REFRESHING:
            return
        _STATUS_REFRESHING = True
    thread = threading.Thread(target=_refresh_status_cache, daemon=True)
    thread.start()

def _empty_status_payload(*, loading: bool, error: str | None) -> dict[str, Any]:
    data: dict[str, Any] = {
        "runs": [],
        "collapsed_failures": [],
        "collapsed_failures_count": 0,
        "coordinator": configured_coordinator_runs() or None,
        "timestamp": time.time(),
        "loading": loading,
        "refreshing": loading,
    }
    if error:
        data["error"] = error
    return data

def _api_status() -> bytes:
    global _STATUS_CACHE, _STATUS_CACHE_EPOCH, _STATUS_CACHE_ERROR
    now = time.time()
    with _STATUS_CACHE_LOCK:
        cached = copy.deepcopy(_STATUS_CACHE)
        cache_epoch = _STATUS_CACHE_EPOCH
        error = _STATUS_CACHE_ERROR
        refreshing = _STATUS_REFRESHING
    if cached is None:
        try:
            cached = _collect_local_status_payload()
            cached["timestamp"] = now
            with _STATUS_CACHE_LOCK:
                _STATUS_CACHE = copy.deepcopy(cached)
                _STATUS_CACHE_EPOCH = now
                _STATUS_CACHE_ERROR = None
            error = None
        except Exception as exc:
            error = str(exc)
    if not refreshing and (cached is None or now - cache_epoch >= _STATUS_CACHE_SECONDS):
        _start_status_refresh()
        refreshing = True
    if cached is None:
        return json.dumps(_empty_status_payload(loading=True, error=error)).encode()
    cached["refreshing"] = refreshing
    if error:
        cached["error"] = error
    return json.dumps(cached).encode()


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        pass

    def handle_error(self, request: Any, client_address: Any) -> None:
        pass

    def do_POST(self) -> None:
        if self.path == "/api/cmd":
            try:
                length = int(self.headers.get("Content-Length", 0))
                req = json.loads(self.rfile.read(length))
                run = str(req["run"])
                cmd = str(req["cmd"])
                if cmd not in ("stop", "resume", "retry"):
                    raise ValueError(f"unknown command: {cmd}")
                result = subprocess.run(
                    [sys.executable, str(script_path()), cmd, run],
                    capture_output=True, text=True, check=False,
                )
                body = json.dumps({
                    "ok": result.returncode == 0,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }).encode()
                self._respond(200, "application/json", body)
            except Exception as exc:
                body = json.dumps({"ok": False, "error": str(exc)}).encode()
                self._respond(400, "application/json", body)
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self) -> None:
        if self.path in ("/", "/index.html"):
            self._respond(200, "text/html; charset=utf-8", _HTML_BYTES)
        elif self.path == "/api/status":
            try:
                body = _api_status()
                self._respond(200, "application/json", body)
            except Exception as exc:
                body = json.dumps({"error": str(exc)}).encode()
                self._respond(500, "application/json", body)
        else:
            self.send_response(404)
            self.end_headers()

    def _respond(self, code: int, content_type: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _daemonize() -> None:
    if os.fork():
        sys.exit(0)
    os.setsid()
    if os.fork():
        sys.exit(0)
    devnull = os.open(os.devnull, os.O_RDWR)
    for fd in (0, 1, 2):
        os.dup2(devnull, fd)
    os.close(devnull)


def cmd_web(args: argparse.Namespace) -> int:
    host = str(args.host)
    port = int(args.port)
    url = f"http://{host}:{port}"
    try:
        server = ThreadingHTTPServer((host, port), _Handler)
    except OSError as exc:
        if exc.errno == 98 or exc.errno == 48:  # EADDRINUSE
            raise SystemExit(f"port {port} already in use — is crucible web already running?")
        raise
    if getattr(args, "foreground", False):
        print(f"crucible web: {url}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
    else:
        print(f"crucible web: {url}", flush=True)
        _daemonize()
        server.serve_forever()
    return 0
