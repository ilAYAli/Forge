from __future__ import annotations

import copy
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .core import *
from .status import configured_coordinator_runs
from .tasks import parse_coordinator, shell_join

_ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"
_FAVICON_PATH = _ASSETS_DIR / "crucible.png"


_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Crucible Status</title>
  <link rel="icon" type="image/png" href="/favicon.png">
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
    .top-btn { background: none; border: 1px solid var(--border); border-radius: 4px;
             color: var(--dim); font-family: var(--font); font-size: 11px;
             padding: 1px 7px; cursor: pointer; }
    #sound.on { color: var(--yel); border-color: rgba(212,168,34,.45); }
    .top-btn:hover { color: var(--text); border-color: var(--dim); }
    #err { display: none; padding: 6px 14px; background: rgba(217,95,95,.1);
           color: var(--red); border-bottom: 1px solid rgba(217,95,95,.2); font-size: 13px; }
    #runs { padding: 10px 12px; display: flex; flex-direction: column; gap: 6px; }

    /* run card */
    .run { border: 1px solid var(--border); border-radius: 7px; overflow: hidden; }
    .run.current { border-color: #2a3848; }
    .run.failed  { border-color: #3d2424; }
    .run.stopped-early { border-color: rgba(200,142,74,.65); }
    .run.done    { border-color: var(--border); opacity: .65; }

    /* run header — always visible */
    .run-hdr { user-select: text; background: var(--bg2); padding: 8px 12px; cursor: pointer; }
    .run-hdr:hover { background: var(--bg3); }
    .run.stopped-early .run-hdr { background: rgba(200,142,74,.08); }
    .run.stopped-early .run-hdr:hover { background: rgba(200,142,74,.13); }
    .run-hdr-top { display: flex; align-items: center; gap: 8px; }
    .arrow { color: var(--dim); cursor: pointer; font-size: 10px; flex-shrink: 0; transition: transform .15s; user-select: none; }
    .arrow.open { transform: rotate(90deg); }
    .run-date { color: var(--dim); font-size: 12px; font-variant-numeric: tabular-nums; flex-shrink: 0; }
    .run-name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis;
                white-space: nowrap; font-weight: 600; cursor: text; }
    .badge { font-size: 11px; font-weight: 700; padding: 2px 8px; border-radius: 10px; flex-shrink: 0; }
    .badge.current { background: rgba(212,168,34,.15); color: var(--yel); }
    .badge.failed  { background: rgba(217,95,95,.15);  color: var(--red); }
    .badge.done    { background: rgba(77,179,106,.12);  color: var(--grn); }
    .badge.queued  { background: rgba(118,118,128,.15); color: var(--dim); }
    .badge.stopped { background: rgba(200,142,74,.16); color: var(--warn); }
    .badge.empty   { background: rgba(118,118,128,.12); color: var(--dim); }
    /* summary line below the run name — always visible */
    .run-summary,
    .run-meta { display: flex; align-items: center; gap: 12px; margin-top: 4px;
                padding-left: 18px; font-size: 13px; flex-wrap: wrap; cursor: text; }
    .run-desc  { color: var(--dim); }
    .run-comment { color: var(--warn); }
    .run-state { font-weight: 700; }
    .run-state.stopped-early { color: var(--warn); }
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
    .run-body { display: none; border-top: 1px solid var(--border); background: rgba(0,0,0,.08); }
    .run-body.open { display: block; }
    .run-tree { margin-left: 24px; padding: 7px 12px 9px 16px;
                border-left: 1px solid var(--border); }

    /* sections */
    .section { position: relative; padding: 5px 0 6px; }
    .section::before { content: ''; position: absolute; left: -16px; top: 16px;
                       width: 11px; border-top: 1px solid var(--border); }
    .section + .section { border-top: 1px solid rgba(35,35,41,.55); }
    .section-hdr { display: flex; align-items: center; gap: 6px; cursor: pointer;
                   user-select: none; padding: 2px 0; }
    .section-hdr:hover .section-title { color: var(--text); }
    .section-title { font-size: 13px; }
    .section-title.yel { color: var(--yel); }
    .section-title.red { color: var(--red); }
    .section-title.dim { color: var(--dim); }
    .section-detail { display: none; margin-left: 18px; padding: 5px 0 2px 10px;
                      border-left: 1px solid rgba(35,35,41,.7); }
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
    .task-graphs { margin-top: 8px; }
    .task-graph { border-top: 1px solid var(--border); padding-top: 5px;
                  display: grid; gap: 10px; }
    .graph-title { color: var(--dim); font-size: 11px; margin-bottom: 3px;
                   white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .graph-row { display: grid; grid-template-columns: 42px minmax(120px, 1fr) 160px; gap: 8px;
                 align-items: center; color: var(--dim); font-size: 11px; }
    .graph-row svg { width: 100%; height: 132px; display: block; }
    .graph-line { fill: none; stroke-width: 1.1; vector-effect: non-scaling-stroke; }
    .graph-zero { stroke: var(--text); stroke-width: 1; stroke-dasharray: 1 4;
                  stroke-linecap: round; opacity: .55; vector-effect: non-scaling-stroke; }
    .graph-bound { stroke: var(--red); stroke-width: .5; stroke-dasharray: 4 7;
                   opacity: .24; vector-effect: non-scaling-stroke; }
    .graph-hit { fill: transparent; pointer-events: all; }
    .graph-crosshair { opacity: 0; pointer-events: none; transition: opacity .08s linear; }
    .graph-hover-zone:hover .graph-crosshair { opacity: 1; }
    .graph-crosshair-line { stroke: var(--text); stroke-width: .55; stroke-dasharray: 2 3;
                            opacity: .62; vector-effect: non-scaling-stroke; }
    .graph-value { color: var(--dim); white-space: nowrap; font-variant-numeric: tabular-nums; }
    .graph-value.hover { color: var(--text); }

    /* pending / fail groups */
    .group-row { font-size: 13px; color: var(--dim); padding: 2px 0; }
    .cmd-list { display: grid; gap: 8px; }
    .cmd-meta { color: var(--dim); font-size: 12px; margin-bottom: 3px; }
    .cmd-line { margin: 0; padding: 7px 8px; border: 1px solid var(--border);
                border-radius: 4px; background: var(--bg); color: var(--text);
                white-space: pre-wrap; overflow-wrap: normal; word-break: normal; overflow-x: auto;
                line-height: 1.4; user-select: text; }

    .empty { color: var(--dim); padding: 10px 0; font-size: 13px; }

    /* action buttons */
    .run-actions { display: flex; gap: 5px; flex-shrink: 0; }
    .act { background: none; border: 1px solid var(--border); border-radius: 4px;
           color: var(--dim); font-family: var(--font); font-size: 11px;
           padding: 1px 7px; cursor: pointer; }
    .act:hover { color: var(--text); border-color: var(--dim); }
    .act.stop   { border-color: rgba(217,95,95,.4);  color: var(--red); }
    .act.stop:hover   { background: rgba(217,95,95,.1); }
    .act.del    { border-color: rgba(217,95,95,.4);  color: var(--red); }
    .act.del:hover    { background: rgba(217,95,95,.1); }
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
    <button id="restart" class="top-btn" type="button" onclick="restartWeb(this)" title="Restart this web server process">restart</button>
    <button id="sound" class="top-btn" type="button" onclick="toggleSound()" title="Toggle job start sound" aria-pressed="false">ding off</button>
  </header>
  <div id="runs"></div>
  <script>
    function esc(s) {
      return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    function attrEsc(s) {
      return esc(s).replace(/"/g, '&quot;');
    }

    function jsSingle(s) {
      return String(s)
        .replace(/\\/g, '\\\\')
        .replace(/'/g, "\\'")
        .replace(/\n/g, '\\n')
        .replace(/\r/g, '\\r');
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

    function runDescriptionText(description, hasComment) {
      if (hasComment) return '';
      return String(description || '')
        .replace(/^desc=/, '')
        .replace(/^Run\s+/, '')
        .trim();
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

    function runDatePrefix(r) {
      const text = String(r?.created_at || '');
      let m = text.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2})/);
      if (m) return `${m[1]}-${m[2]}:${m[3]}`;
      m = String(r?.run || '').match(/(?:^|-)(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(?:\d{2})?(?:$|-)/);
      return m ? `${m[1]}-${m[2]}-${m[3]}-${m[4]}:${m[5]}` : '';
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
      return {queued:0,current:1,failed:2,stopped:3,done:4,empty:5}[r.state]??6;
    }

    function numericField(value, fallback=0) {
      const n = Number(value);
      return Number.isFinite(n) ? n : fallback;
    }

    function queuedEpoch(r) {
      return numericField(r?.queued_epoch ?? r?.queue?.requested_epoch, 0);
    }

    function completedEpoch(r) {
      return numericField(r?.completed_epoch, 0);
    }

    let soundEnabled = localStorage.getItem('crucible.sound') === '1';
    let audioCtx = null;
    let activeJobKeys = null;
    const hiddenRunNames = new Set();

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
        const active = Number(run.sections?.active?.count || 0);
        if (active > 0) keys.add(String(run.run || ''));
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

    // UI state persistence
    const UI_STATE_KEY = 'crucible.ui.v1';
    function clearOldHashState() {
      if (!location.hash) return;
      try {
        history.replaceState(null, document.title, location.pathname + location.search);
      } catch(e) {
        location.hash = '';
      }
    }
    function saveUiState() {
      try {
        localStorage.setItem(UI_STATE_KEY, JSON.stringify({o: openState, s: taskSort, c: taskColWidths}));
      } catch(e) {}
    }
    function loadUiState() {
      try {
        let raw = localStorage.getItem(UI_STATE_KEY);
        clearOldHashState();
        if (!raw) return;
        const st = JSON.parse(raw);
        if (st.o) Object.assign(openState, st.o);
        if (st.s) Object.assign(taskSort, st.s);
        if (taskSort.col === 'task') taskSort.col = 'shard';
        if (st.c) taskColWidths = st.c;
      } catch(e) { clearOldHashState(); }
    }

    // Column widths and sort state — persisted across refreshes
    let taskColWidths = null;
    let taskSort = {col: 'eta', dir: 1};

    const BASE_TASK_COLS = ['worker','label','shard','progress','output','rate'];
    const FIELD_TASK_COLS = [
      {name: 'elo',  keys: ['elo','elo~','Elo']},
      {name: 'ci',   keys: ['ci','err','error','+/-','elo_ci']},
      {name: 'llr',  keys: ['llr','LLR']},
      {name: 'los',  keys: ['los','LOS']},
      {name: 'draw', keys: ['draw','DrawRatio','draw_ratio']},
    ];

    function sortByCol(col) {
      if (taskSort.col === col) taskSort.dir *= -1;
      else { taskSort.col = col; taskSort.dir = 1; }
      applySortToAllTables();
      saveUiState();
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
              saveUiState();
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
      const seen = new Set();
      document.querySelectorAll('.run-body, .section-detail').forEach(el => {
        if (!el.id) return;
        seen.add(el.id);
        openState[el.id] = el.classList.contains('open');
      });
      if (seen.size > 0) {
        for (const key of Object.keys(openState)) {
          if (!seen.has(key)) delete openState[key];
        }
      }
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
      saveUiState();
    }

    function toggleRunHeader(event, id) {
      if (event.defaultPrevented) return;
      if (event.target.closest('button,a,input,select,textarea,.arrow')) return;
      if (hasTextSelection()) return;
      toggleId(id);
    }

    function hasTextSelection() {
      const selection = window.getSelection && window.getSelection();
      return !!(selection && !selection.isCollapsed && String(selection).trim());
    }
    let selectionRefreshDeferredAt = null;
    function shouldDeferRefreshForSelection() {
      if (!hasTextSelection()) {
        selectionRefreshDeferredAt = null;
        return false;
      }
      const now = Date.now();
      if (selectionRefreshDeferredAt == null) selectionRefreshDeferredAt = now;
      if (now - selectionRefreshDeferredAt < 15000) return true;
      selectionRefreshDeferredAt = null;
      return false;
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

    function fmtRate(r) {
      if (r == null || r <= 0) return '';
      if (r >= 100) return Math.round(r) + '/s';
      if (r >= 1) return r.toFixed(1) + '/s';
      if (r >= 0.01) return r.toFixed(2) + '/s';
      return r.toFixed(4) + '/s';
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
      if (col === 'shard' || col === 'task') return String(t.index);
      if (col === 'progress') return esc(taskProgressText(t));
      if (col === 'output') return esc(pd?.output!=null ? String(pd.output) : '');
      if (col === 'rate') return pd?.fallback_estimate ? '' : esc(fmtRate(pd?.rate));
      if (col === 'eta') {
        if (pd?.fallback_estimate) return '';
        const etaSec = remainingEtaS(pd);
        const prefix = pd?.eta_estimate ? '~' : '';
        return etaSec!=null ? esc(prefix + fmtEta(etaSec)) : '';
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
      if (col === 'shard' || col === 'task') return String(t.index);
      if (col === 'progress') return pd?.done ?? '';
      if (col === 'output') return pd?.output ?? '';
      if (col === 'rate') return pd?.fallback_estimate ? '' : pd?.rate ?? '';
      if (col === 'eta') return pd?.fallback_estimate ? 1e9 : remainingEtaS(pd) ?? 1e9;
      return '';
    }

    function taskCellClass(col) {
      if (FIELD_TASK_COLS.some(f => f.name === col)) return 'td-output';
      if (col === 'worker') return 'td-worker';
      if (col === 'label') return 'td-label';
      if (col === 'shard' || col === 'task') return 'td-id';
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
        const prefix = el.dataset.etaEstimate === '1' ? '~' : '';
        el.textContent = remaining > 0 ? prefix + fmtEta(remaining) : '—';
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
        const etaSec  = pd?.fallback_estimate ? null : remainingEtaS(pd);
        const etaAttr = etaSec!=null
          ? ` data-etas="${etaSec}"${pd?.eta_estimate?' data-eta-estimate="1"':''}`
          : '';
        return `<tr class="task-row">` + cols.map(c =>
          `<td class="task-td ${taskCellClass(c)}" data-sort="${esc(taskSortValue(t, c))}"${c==='eta'?etaAttr:''}>${taskCell(t, c)}</td>`
        ).join('') + `</tr>`;
      }).join('') + '</tbody>';
      return `<table class="task-table">${cg}${thead}${tbody}</table>`;
    }

    const GRAPH_COLORS = ['var(--blu)', 'var(--yel)', 'var(--grn)', 'var(--red)'];

    function graphPoints(series) {
      return (series?.points || [])
        .map(p => ({x: Number(p.x), y: Number(p.y)}))
        .filter(p => Number.isFinite(p.x) && Number.isFinite(p.y))
        .sort((a, b) => a.x - b.x);
    }

    function fmtGraphNumber(v) {
      const n = Number(v);
      if (!Number.isFinite(n)) return '';
      const a = Math.abs(n);
      if (a >= 100) return n.toFixed(0);
      if (a >= 10) return n.toFixed(1);
      if (a >= 0.01) return n.toFixed(2);
      return n.toExponential(1);
    }

    function graphDomain(values, includeZero = false) {
      let min = Math.min(...values), max = Math.max(...values);
      if (includeZero) {
        min = Math.min(min, 0);
        max = Math.max(max, 0);
      }
      if (min === max) {
        const pad = Math.max(1, Math.abs(min) * 0.25);
        return [min - pad, max + pad];
      }
      if (min < 0 && max > 0) {
        const bound = Math.max(Math.abs(min), Math.abs(max));
        return [-bound * 1.08, bound * 1.08];
      }
      const span = max - min;
      const pad = span * 0.12;
      return [min - pad, max + pad];
    }

    function signedGraphSeries(name) {
      return ['elo', 'llr'].includes(String(name || '').toLowerCase());
    }

    function graphSeriesName(name) {
      return String(name || '').toLowerCase();
    }

    function graphColor(name, index) {
      const key = graphSeriesName(name);
      if (key === 'llr') return 'var(--yel)';
      if (key === 'elo') return 'var(--blu)';
      return GRAPH_COLORS[index % GRAPH_COLORS.length];
    }

    function graphDisplayPoints(name, points) {
      const key = graphSeriesName(name);
      if (key !== 'elo' || points.length < 6) return points;
      const filtered = points.filter(p => p.x >= 20);
      return filtered.length >= 3 ? filtered : points;
    }

    function graphYDomain(name, values, extras = []) {
      const all = values.concat(extras).filter(Number.isFinite);
      if (!all.length) return [0, 1];
      if (signedGraphSeries(name)) {
        const bound = Math.max(1e-9, ...all.map(v => Math.abs(v)));
        return [-bound * 1.08, bound * 1.08];
      }
      return graphDomain(all, false);
    }

    function graphScale(name, points, extras = [], w = 180, h = 132) {
      const pad = 4;
      const xs = points.map(p => p.x);
      const ys = points.map(p => p.y);
      const minX = Math.min(...xs), maxX = Math.max(...xs);
      const [minY, maxY] = graphYDomain(name, ys, extras);
      return {
        x: maxX === minX
          ? () => w / 2
          : x => pad + ((x - minX) / (maxX - minX)) * (w - pad * 2),
        y: maxY === minY
          ? () => h / 2
          : y => h - pad - ((y - minY) / (maxY - minY)) * (h - pad * 2),
      };
    }

    function graphY(name, values, y, extras = [], h = 132) {
      const pad = 4;
      const [minY, maxY] = graphYDomain(name, values, extras);
      return maxY === minY
        ? h / 2
        : h - pad - ((y - minY) / (maxY - minY)) * (h - pad * 2);
    }

    function zeroLine(name, points, extras = [], w = 180, h = 132) {
      if (!signedGraphSeries(name) || !points || points.length < 2) return '';
      const y = graphY(name, points.map(p => p.y), 0, extras, h);
      return `<line class="graph-zero" x1="0" y1="${y.toFixed(1)}" x2="${w}" y2="${y.toFixed(1)}"></line>`;
    }

    function graphBoundLines(name, points, options = {}, w = 180, h = 132) {
      if (graphSeriesName(name) !== 'llr' || !points || points.length < 2) return '';
      const bound = Number(options.llrBound);
      if (!Number.isFinite(bound) || bound <= 0) return '';
      const values = points.map(p => p.y);
      const [minY, maxY] = graphYDomain(name, values, []);
      return [-bound, bound].filter(v => v >= minY && v <= maxY).map(v => {
        const y = graphY(name, values, v, [], h);
        return `<line class="graph-bound" x1="0" y1="${y.toFixed(1)}" x2="${w}" y2="${y.toFixed(1)}"></line>`;
      }).join('');
    }

    function sparkPath(name, points, extras = [], w = 180, h = 132) {
      if (!points || points.length < 2) return '';
      const scale = graphScale(name, points, extras, w, h);
      const scaled = points.map(p => ({x: scale.x(p.x), y: scale.y(p.y)}));
      const clampX = (value, a, b) => Math.max(Math.min(a, b), Math.min(Math.max(a, b), value));
      let path = `M${scaled[0].x.toFixed(1)},${scaled[0].y.toFixed(1)}`;
      for (let i = 0; i < scaled.length - 1; i++) {
        const p0 = scaled[Math.max(0, i - 1)];
        const p1 = scaled[i];
        const p2 = scaled[i + 1];
        const p3 = scaled[Math.min(scaled.length - 1, i + 2)];
        const c1x = clampX(p1.x + (p2.x - p0.x) / 6, p1.x, p2.x);
        const c1y = p1.y + (p2.y - p0.y) / 6;
        const c2x = clampX(p2.x - (p3.x - p1.x) / 6, p1.x, p2.x);
        const c2y = p2.y - (p3.y - p1.y) / 6;
        path += ` C${c1x.toFixed(1)},${c1y.toFixed(1)} ${c2x.toFixed(1)},${c2y.toFixed(1)} ${p2.x.toFixed(1)},${p2.y.toFixed(1)}`;
      }
      return path;
    }

    function setGraphReadout(el) {
      const row = el.closest('.graph-row');
      const value = row?.querySelector?.('.graph-value');
      if (!value) return;
      value.textContent = el.getAttribute('data-readout') || value.dataset.default || value.textContent;
      value.classList.add('hover');
    }

    function clearGraphReadout(el) {
      const row = el.closest('.graph-row');
      const value = row?.querySelector?.('.graph-value');
      if (!value) return;
      value.textContent = value.dataset.default || value.textContent;
      value.classList.remove('hover');
    }

    function graphHoverZones(name, points, extras = [], xUnit = 'x', w = 180, h = 132) {
      if (!points || points.length < 2) return '';
      const scale = graphScale(name, points, extras, w, h);
      const scaled = points.map(p => ({
        x: scale.x(p.x),
        y: scale.y(p.y),
        rawX: p.x,
        rawY: p.y,
      }));
      return scaled.map((p, i) => {
        const left = i === 0 ? 0 : (scaled[i - 1].x + p.x) / 2;
        const right = i + 1 === scaled.length ? w : (p.x + scaled[i + 1].x) / 2;
        const label = `${fmtGraphNumber(p.rawX)} ${xUnit} · ${name} ${fmtGraphValue(name, p.rawY)}`;
        return `<g class="graph-hover-zone">`
          + `<rect class="graph-hit" x="${left.toFixed(1)}" y="0" width="${Math.max(1, right - left).toFixed(1)}" height="${h}" data-readout="${attrEsc(label)}" onmouseenter="setGraphReadout(this)" onmousemove="setGraphReadout(this)" onmouseleave="clearGraphReadout(this)"></rect>`
          + `<g class="graph-crosshair">`
          + `<line class="graph-crosshair-line" x1="${p.x.toFixed(1)}" y1="0" x2="${p.x.toFixed(1)}" y2="${h}"></line>`
          + `<line class="graph-crosshair-line" x1="0" y1="${p.y.toFixed(1)}" x2="${w}" y2="${p.y.toFixed(1)}"></line>`
          + `</g>`
          + `</g>`;
      }).join('');
    }

    function fmtGraphValue(name, value) {
      const text = fmtGraphNumber(value);
      if (!text || !signedGraphSeries(name)) return text;
      return Number(value) > 0 ? `+${text}` : text;
    }

    function graphTextSources(r) {
      return []
        .concat(r?.display?.fields || [])
        .concat(r?.display?.active_fields || [])
        .concat([r?.status_line, r?.progress])
        .filter(Boolean)
        .map(String);
    }

    function llrBoundFromRun(r) {
      for (const text of graphTextSources(r)) {
        const match = text.match(/(?:^|\b)llr\s*=\s*[+-]?(?:\d+(?:\.\d+)?|\.\d+)\s*\/\s*([+-]?(?:\d+(?:\.\d+)?|\.\d+))/i)
          || text.match(/(?:^|\b)LLR\s+[+-]?(?:\d+(?:\.\d+)?|\.\d+)\s*\/\s*([+-]?(?:\d+(?:\.\d+)?|\.\d+))/);
        if (match) {
          const bound = Math.abs(Number(match[1]));
          if (Number.isFinite(bound) && bound > 0) return bound;
        }
      }
      return null;
    }

    function graphCard(graph, title, run) {
      const options = {llrBound: llrBoundFromRun(run)};
      const series = (graph?.series || [])
        .map((s, i) => ({
          name: s.name || '',
          color: graphColor(s.name, i),
          points: graphDisplayPoints(s.name, graphPoints(s)),
        }))
        .filter(s => s.points.length >= 2);
      if (!series.length) return '';
      const rows = series.map(s => {
        const path = sparkPath(s.name, s.points);
        const zero = zeroLine(s.name, s.points);
        const bounds = graphBoundLines(s.name, s.points, options);
        const hover = graphHoverZones(s.name, s.points, [], graph?.x_unit || 'x');
        const last = s.points[s.points.length - 1]?.y;
        const lastX = s.points[s.points.length - 1]?.x;
        const lastValue = fmtGraphValue(s.name, last);
        const unit = graph?.x_unit ? ` ${graph.x_unit}` : '';
        const tooltip = `${s.name}: ${lastValue} at${unit} ${fmtGraphNumber(lastX)}`;
        return `<div class="graph-row">`
          + `<span>${esc(s.name)}</span>`
          + `<svg viewBox="0 0 180 132" preserveAspectRatio="none" aria-label="${esc(tooltip)}">`
          + `<title>${esc(tooltip)}</title>`
          + bounds
          + zero
          + `<path class="graph-line" d="${esc(path)}" style="stroke:${esc(s.color)}"></path>`
          + hover
          + `</svg>`
          + `<span class="graph-value" data-default="${attrEsc(lastValue)}">${esc(lastValue)}</span>`
          + `</div>`;
      }).join('');
      const unit = graph?.x_unit ? ` · x: ${graph.x_unit}` : '';
      return `<div class="task-graph"><div class="graph-title">${esc(title + unit)}</div>${rows}</div>`;
    }

    function renderRunGraph(r) {
      const graph = graphCard(r.progress_graph, 'run trend', r);
      return graph ? `<div class="task-graphs">${graph}</div>` : '';
    }

    function renderGroups(groups) {
      return Object.entries(groups||{}).sort().map(([lbl, vals]) =>
        `<div class="group-row">${esc(lbl||'unlabeled')} [${vals.length}]: ${fmtRanges(vals)}</div>`
      ).join('');
    }

    function commandDisplayText(command) {
      return String(command || '').replace(/\s+/g, ' ').trim();
    }

    function shellWords(text) {
      const words = [];
      let word = '';
      let quote = '';
      let escaped = false;
      for (const ch of String(text || '')) {
        if (escaped) {
          word += ch;
          escaped = false;
          continue;
        }
        if (ch === '\\') {
          word += ch;
          escaped = true;
          continue;
        }
        if (quote) {
          word += ch;
          if (ch === quote) quote = '';
          continue;
        }
        if (ch === '"' || ch === "'") {
          word += ch;
          quote = ch;
          continue;
        }
        if (/\s/.test(ch)) {
          if (word) {
            words.push(word);
            word = '';
          }
          continue;
        }
        word += ch;
      }
      if (word) words.push(word);
      return words;
    }

    function commandDisplayLines(command) {
      const words = shellWords(commandDisplayText(command));
      if (!words.length) return [];
      const lines = [];
      let current = [];
      for (const word of words) {
        if (word.startsWith('--') && current.length) {
          lines.push(current.join(' '));
          current = [word];
        } else {
          current.push(word);
        }
      }
      if (current.length) lines.push(current.join(' '));
      return lines.map((line, index) => {
        const prefix = index === 0 ? '' : '  ';
        const suffix = index + 1 < lines.length ? ' \\' : '';
        return prefix + line + suffix;
      });
    }

    function commandDisplayBlock(command) {
      const lines = commandDisplayLines(command);
      return lines.length ? lines.join('\n') : commandDisplayText(command);
    }

    function renderCommands(commands) {
      const rows = (commands || [])
        .filter(c => c && c.command)
        .map(c => {
          const label = c.label || 'task';
          const count = Number(c.count || 0);
          const suffix = count > 1 ? ` [${count}]` : '';
          return `<div class="cmd-row">`
            + `<div class="cmd-meta">${esc(label + suffix)}</div>`
            + `<pre class="cmd-line"><code>${esc(commandDisplayBlock(c.command))}</code></pre>`
            + `</div>`;
        }).join('');
      return rows ? `<div class="cmd-list">${rows}</div>` : '';
    }

    function renderChips(str) {
      return '<div class="ws-chips">' + (str||'').split(' ').filter(Boolean).map(tok => {
        const elo = eloFieldClass(tok);
        const hi = tok.startsWith('speedup=') || tok.startsWith('output=') || tok.startsWith('elo=');
        return `<span class="chip${elo || (hi?' hi':'')}">${esc(tok)}</span>`;
      }).join(' ') + '</div>';
    }

    function runStoppedEarly(r, pending, fail, display) {
      if ((r.state || '') !== 'stopped') return false;
      const pendingCount = Number(pending?.count || r.pending || 0);
      if (pendingCount <= 0) return false;
      if (Number(r.done || 0) > 0 || Number(fail?.count || r.fail || 0) > 0) return true;
      return (display.fields || []).some(f => {
        const m = String(f).match(/^games=(\d+)\/\d+/);
        return m && Number(m[1]) > 0;
      });
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
      const commentText = display.comment || '';
      const descText = runDescriptionText(display.description, Boolean(commentText));
      const runDate = runDatePrefix(r);
      const stoppedEarly = runStoppedEarly(r, pending, fail, display);
      const stateField = stoppedEarly ? '<span class="run-state stopped-early">stopped</span>' : '';
      const stateLabel = state === 'current' ? 'active' : state === 'done' ? 'completed' : state;

      const actions = [];
      const runArg = attrEsc(jsSingle(r.run));
      const activeCount = Number(active.count || 0);
      const failCount = Number(fail.count || r.fail || 0);
      if (state === 'current' || state === 'queued' || state === 'stalled') actions.push(`<button class="act stop"   onclick="event.stopPropagation();runCmd('${runArg}','stop',this)">stop</button>`);
      if (state === 'stopped' || state === 'stalled') actions.push(`<button class="act resume" onclick="event.stopPropagation();runCmd('${runArg}','resume',this)">resume</button>`);
      if (state === 'done') actions.push(`<button class="act" onclick="event.stopPropagation();runCmd('${runArg}','hide',this)">hide</button>`);
      if (state === 'done') actions.push(`<button class="act del" onclick="event.stopPropagation();runCmd('${runArg}','del',this)">del</button>`);
      if (failCount > 0 && activeCount === 0) actions.push(`<button class="act retry" onclick="event.stopPropagation();runCmd('${runArg}','retry',this)">retry</button>`);
      if (failCount > 0 && activeCount === 0) actions.push(`<button class="act del" onclick="event.stopPropagation();runCmd('${runArg}','del',this)">del</button>`);
      if (state === 'stopped' && activeCount === 0) actions.push(`<button class="act del" onclick="event.stopPropagation();runCmd('${runArg}','del',this)">del</button>`);

      // header — always visible
      const meta = descText || commentText ? `
        <div class="run-meta">
          ${descText ? `<span class="run-desc">${esc(descText)}</span>` : ''}
          ${commentText ? `<span class="run-comment">${esc(commentText)}</span>` : ''}
        </div>` : '';
      const hdr = `
        <div class="run-hdr-top">
          <span class="arrow${bodyOpen?' open':''}" onclick="event.stopPropagation();toggleId('${rId}')">&#9654;</span>
          ${runDate ? `<span class="run-date">${esc(runDate)}</span>` : ''}
          <span class="run-name">${esc(r.run)}</span>
          <div class="run-actions">${actions.join('')}</div>
          <span class="badge ${state}">${stateLabel}</span>
        </div>
        ${meta}
        <div class="run-summary">
          ${stateField}
          <span class="run-fields">${nonEtaFields.map(renderField).join(' &nbsp;')}</span>
          ${eta ? `<span class="run-eta" data-etas="${parseEtaStr(eta)??''}">${esc(eta)}</span>` : ''}
        </div>`;

      // body sections (no description here — it's in the header now)
      const sections = [];

      const runGraph = renderRunGraph(r);
      if (runGraph) {
        sections.push(makeSection(r.run, 'graphs',
          'graphs', 'dim',
          runGraph, false
        ));
      }

      const commandRows = renderCommands(r.commands);
      if (commandRows) {
        sections.push(makeSection(r.run, 'commands',
          'commands', 'dim',
          commandRows, false
        ));
      }

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

      return `<div class="run ${state}${stoppedEarly?' stopped-early':''}">
        <div class="run-hdr" onclick="toggleRunHeader(event,'${rId}')">${hdr}</div>
        <div class="pbar-wrap"><div class="pbar-fill" style="width:${pct}%"></div></div>
        <div class="run-body${bodyOpen?' open':''}" id="${rId}"><div class="run-tree">${sections.join('')}</div></div>
      </div>`;
    }

    function removeRunCard(run) {
      hiddenRunNames.add(String(run));
      const body = document.getElementById(rbid(run));
      const card = body?.closest?.('.run');
      if (!card) return null;
      const parent = card.parentNode;
      const next = card.nextSibling;
      card.remove();
      return {card, parent, next};
    }

    function restoreRunCard(run, removed) {
      hiddenRunNames.delete(String(run));
      if (!removed?.card || !removed.parent) return;
      if (removed.next && removed.next.parentNode === removed.parent) {
        removed.parent.insertBefore(removed.card, removed.next);
      } else {
        removed.parent.appendChild(removed.card);
      }
    }

    async function runCmd(run, cmd, btn) {
      const oldText = btn.textContent;
      btn.disabled = true;
      btn.textContent = '…';
      const hidden = (cmd === 'del' || cmd === 'hide') ? removeRunCard(run) : null;
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
          btn.disabled = false;
          btn.textContent = oldText;
          restoreRunCard(run, hidden);
          return;
        }
      } catch(e) {
        document.getElementById('err').style.display = 'block';
        document.getElementById('err').textContent = e.message;
        btn.disabled = false;
        btn.textContent = oldText;
        restoreRunCard(run, hidden);
        return;
      }
      refresh();
    }

    async function restartWeb(btn) {
      const old = btn.textContent;
      btn.disabled = true;
      btn.textContent = 'restarting';
      try {
        const res = await fetch('/api/cmd', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({cmd: 'restart-web'}),
        });
        const data = await res.json();
        if (!data.ok) throw new Error((data.stderr || data.error || 'restart failed').trim());
        await waitForRestart();
      } catch(e) {
        btn.disabled = false;
        btn.textContent = old;
        document.getElementById('err').style.display = 'block';
        document.getElementById('err').textContent = e.message;
      }
    }

    async function waitForRestart() {
      const deadline = Date.now() + 20000;
      while (Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 500));
        try {
          const res = await fetch('/api/status?restart=' + Date.now(), {cache: 'no-store'});
          if (res.ok) {
            location.reload();
            return;
          }
        } catch(e) {}
      }
      throw new Error('restart requested but server did not come back');
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

        const runs = (data.runs||[])
          .filter(r => !hiddenRunNames.has(String(r.run || '')))
          .slice()
          .sort((a,b) => {
            const dp = statePri(a)-statePri(b);
            if (dp) return dp;
            if (a.state==='queued') {
              const qa=queuedEpoch(a), qb=queuedEpoch(b);
              if (qa!==qb) return qa-qb;
            }
            if (a.state==='current') {
              const ea=runEtaS(a)??1e9, eb=runEtaS(b)??1e9;
              if (ea!==eb) return ea-eb;
            }
            if (a.state==='done') {
              const ca=completedEpoch(a), cb=completedEpoch(b);
              if (ca!==cb) return cb-ca;
            }
            return a.run<b.run?-1:a.run>b.run?1:0;
          });

        if (shouldDeferRefreshForSelection()) return;
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
        saveUiState();
      } catch(e) {
        document.getElementById('dot').className = 'err';
        document.getElementById('err').style.display = 'block';
        document.getElementById('err').textContent = 'Error: '+e.message;
        document.getElementById('ts').textContent = 'failed '+new Date().toLocaleTimeString();
      } finally {
        refreshing = false;
      }
    }

    loadUiState();
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

def _favicon_bytes() -> bytes | None:
    try:
        return _FAVICON_PATH.read_bytes()
    except OSError:
        return None

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
_WEB_HOST = "127.0.0.1"
_WEB_PORT = 0
_WEB_RESTART_DELAY_SECONDS = 0.2
_WEB_RESTARTER_WAIT_SECONDS = 15.0
_WEB_STATE_NAME = ".web-state.json"
_WEB_STATE_LOCK = threading.Lock()
_WEB_RESTARTER_CODE = r"""
import json
import os
import subprocess
import sys
import time

payload = json.loads(sys.argv[1])
parent = int(payload["parent"])
deadline = time.time() + float(payload["wait_seconds"])
while time.time() < deadline:
    try:
        os.kill(parent, 0)
    except OSError:
        break
    time.sleep(0.1)
else:
    sys.exit(124)

subprocess.Popen(
    payload["argv"],
    cwd=payload.get("cwd") or None,
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    start_new_session=True,
    close_fds=True,
)
"""

def _collect_status_payload() -> dict[str, Any]:
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs:
        local_payload = _collect_local_status_payload()
        if _status_payload_has_current_run(local_payload):
            _attach_coordinator_web_state(local_payload, coordinator_runs)
            return _merge_lingering_runs(local_payload)
        return _merge_lingering_runs(_collect_coordinator_status_payload(coordinator_runs))
    return _merge_lingering_runs(_collect_local_status_payload())

def _web_state_path() -> Path:
    return runs_dir() / _WEB_STATE_NAME

def _read_web_state() -> dict[str, Any]:
    path = _web_state_path()
    if not path.is_file():
        return {}
    try:
        payload = read_json(path)
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}

def _read_coordinator_web_state(coordinator_runs: str) -> dict[str, Any]:
    try:
        host, runs_path = parse_coordinator(coordinator_runs)
    except ValueError:
        return {}
    state_path = f"{runs_path.rstrip('/')}/{_WEB_STATE_NAME}"
    if host:
        quoted_path = shlex.quote(state_path)
        remote = f"if [ -f {quoted_path} ]; then cat {quoted_path}; else printf '{{}}'; fi"
        result = subprocess.run(
            ["ssh", *_WEB_SSH_OPTS, host, remote],
            capture_output=True,
            text=True,
            check=False,
            timeout=_WEB_STATUS_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            return {}
        raw = result.stdout
    else:
        try:
            raw = Path(state_path).read_text(encoding="utf-8")
        except OSError:
            return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}

def _attach_coordinator_web_state(data: dict[str, Any], coordinator_runs: str) -> None:
    web_state = _read_coordinator_web_state(coordinator_runs)
    data["remote_coordinator"] = coordinator_runs
    remote_lingering_runs = web_state.get("lingering_runs")
    if isinstance(remote_lingering_runs, dict):
        data["remote_lingering_runs"] = remote_lingering_runs
    remote_seen_runs = web_state.get("seen_runs")
    if isinstance(remote_seen_runs, dict):
        data["remote_seen_runs"] = remote_seen_runs
    remote_hidden_runs = web_state.get("hidden_runs")
    if isinstance(remote_hidden_runs, list):
        data["remote_hidden_runs"] = remote_hidden_runs
    remote_deleted_runs = web_state.get("deleted_runs")
    if isinstance(remote_deleted_runs, list):
        data["remote_deleted_runs"] = remote_deleted_runs

def _write_web_state(state: dict[str, Any]) -> None:
    path = _web_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, state)

def _state_dict(state: dict[str, Any], name: str) -> dict[str, Any]:
    value = state.get(name)
    if isinstance(value, dict):
        return value
    value = {}
    state[name] = value
    return value

def _state_list(state: dict[str, Any], name: str) -> list[str]:
    value = state.get(name)
    if not isinstance(value, list):
        value = []
        state[name] = value
    return [str(item) for item in value]

def _path_is_under(path: str, root: str) -> bool:
    try:
        candidate = Path(path).expanduser().resolve(strict=False)
        base = Path(root).expanduser().resolve(strict=False)
    except (OSError, RuntimeError):
        return False
    return candidate == base or base in candidate.parents

def _payload_belongs_to_web_runs(payload: dict[str, Any], coordinator_runs: str | None) -> bool:
    roots = [str(runs_dir())]
    if coordinator_runs:
        try:
            _host, coordinator_root = parse_coordinator(coordinator_runs)
            roots.append(coordinator_root)
        except ValueError:
            pass
    paths = [str(payload.get(key) or "") for key in ("run_dir", "manifest")]
    paths = [path for path in paths if path]
    if not paths:
        return True
    return any(_path_is_under(path, root) for path in paths for root in roots)

def _payload_belongs_to_coordinator_runs(payload: dict[str, Any], coordinator_runs: str | None) -> bool:
    if not coordinator_runs:
        return False
    try:
        _host, coordinator_root = parse_coordinator(coordinator_runs)
    except ValueError:
        return False
    paths = [str(payload.get(key) or "") for key in ("run_dir", "manifest")]
    return any(path and _path_is_under(path, coordinator_root) for path in paths)

def _seen_run_tracks_completion(info: Any, coordinator_runs: str | None) -> bool:
    if not isinstance(info, dict):
        return coordinator_runs is None
    source = str(info.get("source") or "")
    if source in {"remember", "status"}:
        return True
    return coordinator_runs is None

def _payload_local_manifest_missing(payload: dict[str, Any], coordinator_runs: str | None) -> bool:
    roots = [str(runs_dir())]
    if coordinator_runs:
        try:
            host, coordinator_root = parse_coordinator(coordinator_runs)
            if not host:
                roots.append(coordinator_root)
        except ValueError:
            pass
    manifest = str(payload.get("manifest") or "")
    if not manifest:
        run_dir = str(payload.get("run_dir") or "")
        manifest = str(Path(run_dir) / RUN_MANIFEST_NAME) if run_dir else ""
    if not manifest or not any(_path_is_under(manifest, root) for root in roots):
        return False
    return not Path(manifest).expanduser().is_file()

def remember_web_run(run: str, *, coordinator_runs: str | None = None) -> None:
    with _WEB_STATE_LOCK:
        state = _read_web_state()
        deleted = set(_state_list(state, "deleted_runs"))
        if run in deleted:
            return
        hidden = set(_state_list(state, "hidden_runs"))
        hidden.discard(run)
        _state_dict(state, "seen_runs")[run] = {
            "coordinator": coordinator_runs,
            "last_seen": time.time(),
            "source": "remember",
        }
        state["hidden_runs"] = sorted(hidden)
        state["deleted_runs"] = sorted(deleted)
        _write_web_state(state)
    _invalidate_status_cache()

def remember_web_run_payload(payload: dict[str, Any], *, coordinator_runs: str | None = None) -> None:
    run = str(payload.get("run") or "")
    if not run:
        return
    if not _payload_belongs_to_web_runs(payload, coordinator_runs):
        return
    payload = _normalize_web_run_payload(payload)
    with _WEB_STATE_LOCK:
        state = _read_web_state()
        deleted = set(_state_list(state, "deleted_runs"))
        if run in deleted:
            return
        hidden = set(_state_list(state, "hidden_runs"))
        hidden.discard(run)
        _state_dict(state, "seen_runs")[run] = {
            "coordinator": coordinator_runs,
            "last_seen": time.time(),
            "source": "remember",
        }
        if _run_payload_done(payload):
            _state_dict(state, "lingering_runs")[run] = payload
        state["hidden_runs"] = sorted(hidden)
        state["deleted_runs"] = sorted(deleted)
        _write_web_state(state)
    _invalidate_status_cache()

def _run_payload_state(run: dict[str, Any]) -> str:
    return str(run.get("state") or "")

def _run_payload_done(run: dict[str, Any]) -> bool:
    if _run_payload_state(run) == "done":
        return True
    counts = run.get("counts")
    if not isinstance(counts, dict):
        return False
    try:
        return (
            int(counts.get("done") or 0) == int(run.get("tasks") or 0)
            and int(counts.get("fail") or 0) == 0
            and int(counts.get("running") or 0) == 0
            and int(counts.get("claimed") or 0) == 0
            and int(counts.get("stale") or 0) == 0
            and int(counts.get("pending") or 0) == 0
        )
    except (TypeError, ValueError):
        return False

def _normalize_web_run_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not _run_payload_done(payload):
        return payload
    normalized = copy.deepcopy(payload)
    normalized["state"] = "done"
    return normalized

def _commands_need_refresh(payload: dict[str, Any]) -> bool:
    commands = payload.get("commands")
    if not isinstance(commands, list) or not commands:
        return True
    for item in commands:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or "")
        command = str(item.get("command") or "")
        if label.startswith("sprt:") or "--name sprt_" in command or "{output_dir}/sprt_" in command:
            return True
        if re.search(r"(?<!\S)--srand(?:=|\s+)\d+\b", command):
            return True
    return False

def _collect_named_run_payload(run: str, coordinator_runs: str | None) -> dict[str, Any] | None:
    result = _run_web_cli(["status", run, "--json"], coordinator_runs=coordinator_runs)
    if result.returncode not in (0, 1) or not result.stdout.strip():
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("counts"), dict):
        return None
    return payload

def _merge_lingering_runs(payload: dict[str, Any]) -> dict[str, Any]:
    runs = payload.get("runs")
    if not isinstance(runs, list):
        return payload
    remote_lingering_runs = payload.pop("remote_lingering_runs", {})
    remote_hidden_runs = payload.pop("remote_hidden_runs", [])
    remote_deleted_runs = payload.pop("remote_deleted_runs", [])
    remote_seen_runs = payload.pop("remote_seen_runs", {})
    remote_coordinator_runs = payload.pop("remote_coordinator", None)
    coordinator_runs = payload.get("coordinator")
    coordinator = str(coordinator_runs) if coordinator_runs else None
    remote_coordinator = str(remote_coordinator_runs) if remote_coordinator_runs else coordinator
    with _WEB_STATE_LOCK:
        state = _read_web_state()
        seen = _state_dict(state, "seen_runs")
        linger = _state_dict(state, "lingering_runs")
        hidden = set(_state_list(state, "hidden_runs"))
        deleted = set(_state_list(state, "deleted_runs"))
        command_checked = set(_state_list(state, "command_checked_runs"))
        refreshed_done = set()
        missing_named_runs = set()
        if isinstance(remote_hidden_runs, list):
            hidden.update(str(item) for item in remote_hidden_runs)
        if isinstance(remote_deleted_runs, list):
            deleted.update(str(item) for item in remote_deleted_runs)
        hidden.difference_update(deleted)
        runs = [
            item
            for item in runs
            if not (
                isinstance(item, dict)
                and item.get("run") is not None
                and str(item["run"]) in deleted
            )
        ]
        visible_names = {
            str(item.get("run"))
            for item in runs
            if isinstance(item, dict) and item.get("run") is not None
        }

        for item in runs:
            if not isinstance(item, dict) or item.get("run") is None:
                continue
            name = str(item["run"])
            if name in deleted:
                seen.pop(name, None)
                linger.pop(name, None)
                command_checked.discard(name)
                continue
            if _run_payload_done(item):
                if name not in hidden:
                    linger[name] = _normalize_web_run_payload(item)
            else:
                hidden.discard(name)
                command_checked.discard(name)
                seen[name] = {"coordinator": coordinator, "last_seen": time.time(), "source": "status"}
                linger.pop(name, None)

        for name, info in list(seen.items()):
            if name in deleted:
                seen.pop(name, None)
                linger.pop(name, None)
                command_checked.discard(name)
                continue
            if name in visible_names or name in hidden or name in linger:
                continue
            if isinstance(info, dict):
                run_coordinator = info.get("coordinator")
                run_coordinator = str(run_coordinator) if run_coordinator else None
            else:
                run_coordinator = coordinator
            final_payload = _collect_named_run_payload(name, run_coordinator)
            if final_payload is None:
                missing_named_runs.add(name)
                seen.pop(name, None)
                linger.pop(name, None)
                command_checked.discard(name)
                continue
            if _run_payload_done(final_payload):
                linger[name] = _normalize_web_run_payload(final_payload)
                refreshed_done.add(name)
                if not _commands_need_refresh(final_payload):
                    command_checked.discard(name)
                else:
                    command_checked.add(name)
            elif _run_payload_state(final_payload) not in ("", "empty"):
                runs.append(final_payload)
            else:
                seen.pop(name, None)
                linger.pop(name, None)
                command_checked.discard(name)

        if isinstance(remote_lingering_runs, dict):
            for name, item in remote_lingering_runs.items():
                name = str(name)
                if name in deleted:
                    continue
                if name in hidden or name in visible_names or name in linger or not isinstance(item, dict):
                    continue
                if name in missing_named_runs:
                    continue
                remote_seen = remote_seen_runs.get(name) if isinstance(remote_seen_runs, dict) else None
                if (
                    not _seen_run_tracks_completion(seen.get(name), remote_coordinator)
                    and not _seen_run_tracks_completion(remote_seen, remote_coordinator)
                ):
                    continue
                if not _run_payload_done(item):
                    continue
                if not _payload_belongs_to_web_runs(item, remote_coordinator):
                    continue
                final_payload = _collect_named_run_payload(name, remote_coordinator)
                if (
                    final_payload is None
                    or not _run_payload_done(final_payload)
                    or not _payload_belongs_to_web_runs(final_payload, remote_coordinator)
                ):
                    continue
                linger[name] = _normalize_web_run_payload(final_payload)
                refreshed_done.add(name)
                if not _commands_need_refresh(final_payload):
                    command_checked.discard(name)
                else:
                    command_checked.add(name)
                seen[name] = {"coordinator": remote_coordinator, "last_seen": time.time(), "source": "status"}

        for name, item in list(linger.items()):
            if name in deleted:
                linger.pop(name, None)
                seen.pop(name, None)
                command_checked.discard(name)
                continue
            item_seen = seen.get(name)
            if isinstance(item_seen, dict):
                item_coordinator_raw = item_seen.get("coordinator")
                item_coordinator = str(item_coordinator_raw) if item_coordinator_raw else coordinator
            else:
                item_coordinator = coordinator
            if name in hidden:
                linger.pop(name, None)
                command_checked.discard(name)
                continue
            if isinstance(item, dict) and not _payload_belongs_to_web_runs(item, item_coordinator):
                linger.pop(name, None)
                seen.pop(name, None)
                command_checked.discard(name)
                continue
            if (
                isinstance(item, dict)
                and _payload_belongs_to_coordinator_runs(item, item_coordinator)
                and not _seen_run_tracks_completion(seen.get(name), item_coordinator)
            ):
                linger.pop(name, None)
                seen.pop(name, None)
                command_checked.discard(name)
                continue
            if isinstance(item, dict) and _payload_local_manifest_missing(item, item_coordinator):
                linger.pop(name, None)
                seen.pop(name, None)
                command_checked.discard(name)
                continue
            if name not in visible_names and isinstance(item, dict):
                if not _commands_need_refresh(item):
                    command_checked.discard(name)
                if (
                    _run_payload_done(item)
                    and _commands_need_refresh(item)
                    and name not in refreshed_done
                    and name not in command_checked
                ):
                    final_payload = _collect_named_run_payload(name, item_coordinator)
                    command_checked.add(name)
                    if (
                        final_payload is not None
                        and _run_payload_done(final_payload)
                        and _payload_belongs_to_web_runs(final_payload, item_coordinator)
                    ):
                        item = _normalize_web_run_payload(final_payload)
                        linger[name] = item
                        if not _commands_need_refresh(item):
                            command_checked.discard(name)
                runs.append(_normalize_web_run_payload(item))

        state["hidden_runs"] = sorted(hidden)
        state["deleted_runs"] = sorted(deleted)
        state["command_checked_runs"] = sorted(name for name in command_checked if name in linger and name not in hidden)
        _write_web_state(state)
    payload["runs"] = runs
    return payload

def _status_payload_has_current_run(payload: dict[str, Any]) -> bool:
    for run in payload.get("runs") or []:
        counts = run.get("counts") if isinstance(run, dict) else None
        if not isinstance(counts, dict):
            continue
        if _run_payload_state(run) == "stopped":
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
    _attach_coordinator_web_state(data, coordinator_runs)
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
            cached = _collect_status_payload()
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

def _invalidate_status_cache() -> None:
    global _STATUS_CACHE, _STATUS_CACHE_EPOCH, _STATUS_CACHE_ERROR
    with _STATUS_CACHE_LOCK:
        _STATUS_CACHE = None
        _STATUS_CACHE_EPOCH = 0.0
        _STATUS_CACHE_ERROR = None

def _forget_web_run(run: str) -> None:
    with _WEB_STATE_LOCK:
        state = _read_web_state()
        _state_dict(state, "seen_runs").pop(run, None)
        _state_dict(state, "lingering_runs").pop(run, None)
        hidden = [name for name in _state_list(state, "hidden_runs") if name != run]
        deleted = [name for name in _state_list(state, "deleted_runs") if name != run]
        command_checked = [name for name in _state_list(state, "command_checked_runs") if name != run]
        state["hidden_runs"] = hidden
        state["deleted_runs"] = deleted
        state["command_checked_runs"] = command_checked
        _write_web_state(state)
    _invalidate_status_cache()

def _delete_web_run(run: str) -> None:
    with _WEB_STATE_LOCK:
        state = _read_web_state()
        _state_dict(state, "seen_runs").pop(run, None)
        _state_dict(state, "lingering_runs").pop(run, None)
        hidden = [name for name in _state_list(state, "hidden_runs") if name != run]
        deleted = set(_state_list(state, "deleted_runs"))
        deleted.add(run)
        command_checked = [name for name in _state_list(state, "command_checked_runs") if name != run]
        state["hidden_runs"] = hidden
        state["deleted_runs"] = sorted(deleted)
        state["command_checked_runs"] = command_checked
        _write_web_state(state)
    _invalidate_status_cache()

def _hide_web_run(run: str) -> None:
    with _WEB_STATE_LOCK:
        state = _read_web_state()
        hidden = set(_state_list(state, "hidden_runs"))
        hidden.add(run)
        state["hidden_runs"] = sorted(hidden)
        _state_dict(state, "lingering_runs").pop(run, None)
        _write_web_state(state)
    _invalidate_status_cache()

def _payload_run(payload: dict[str, Any], run: str) -> dict[str, Any] | None:
    for item in payload.get("runs") or []:
        if isinstance(item, dict) and str(item.get("run")) == run:
            return item
    return None

def _payload_active_count(run: dict[str, Any]) -> int:
    counts = run.get("counts")
    if isinstance(counts, dict):
        return int(counts.get("running") or 0) + int(counts.get("claimed") or 0) + int(counts.get("stale") or 0)
    sections = run.get("sections")
    active = sections.get("active") if isinstance(sections, dict) else None
    if isinstance(active, dict):
        return int(active.get("count") or 0)
    return int(run.get("running") or 0) + int(run.get("claimed") or 0) + int(run.get("stale") or 0)

def _payload_fail_count(run: dict[str, Any]) -> int:
    sections = run.get("sections")
    fail = sections.get("fail") if isinstance(sections, dict) else None
    if isinstance(fail, dict):
        return int(fail.get("count") or 0)
    counts = run.get("counts")
    if isinstance(counts, dict):
        return int(counts.get("fail") or 0)
    return int(run.get("fail") or 0)

def _validate_web_delete(run: str) -> str | None:
    payload = _collect_status_payload()
    item = _payload_run(payload, run)
    if item is None:
        raise RuntimeError(f"{run}: run is not visible in status")
    if _payload_active_count(item) > 0:
        raise RuntimeError(f"{run}: run has active tasks; stop it before deleting")
    state = _run_payload_state(item)
    if _payload_fail_count(item) <= 0 and not _run_payload_done(item) and state != "stopped":
        raise RuntimeError(f"{run}: delete is only available for inactive runs")
    coordinator = payload.get("coordinator")
    return str(coordinator) if coordinator else None

def _run_web_cli(command: list[str], *, coordinator_runs: str | None = None) -> subprocess.CompletedProcess[str]:
    if coordinator_runs:
        host, runs_path = parse_coordinator(coordinator_runs)
        if host:
            remote = (
                "command -v crucible >/dev/null 2>&1 || "
                "{ echo 'crucible not found in PATH on coordinator' >&2; exit 127; }; "
                f"exec env {shlex.quote(COORDINATOR_RUNS_ENV)}= "
                f"{shlex.quote(COLOR_ENV)}=never "
                f"{shlex.quote(RUNS_DIR_ENV)}={shlex.quote(runs_path)} "
                f"crucible {shell_join(command)}"
            )
            return subprocess.run(
                ["ssh", *_WEB_SSH_OPTS, host, remote],
                capture_output=True,
                text=True,
                check=False,
            )
        env = os.environ.copy()
        env[COORDINATOR_RUNS_ENV] = ""
        env[COLOR_ENV] = "never"
        env[RUNS_DIR_ENV] = runs_path
        return subprocess.run(
            [sys.executable, str(script_path()), *command],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
    return subprocess.run(
        [sys.executable, str(script_path()), *command],
        capture_output=True,
        text=True,
        check=False,
    )

def _web_restart_argv() -> list[str]:
    return [
        sys.executable,
        str(script_path()),
        "web",
        "--host",
        _WEB_HOST,
        "--port",
        str(_WEB_PORT),
        "--foreground",
    ]

def _web_restarter_command() -> list[str]:
    payload = {
        "parent": os.getpid(),
        "argv": _web_restart_argv(),
        "cwd": str(Path.cwd()),
        "wait_seconds": _WEB_RESTARTER_WAIT_SECONDS,
    }
    return [sys.executable, "-c", _WEB_RESTARTER_CODE, json.dumps(payload)]

def _start_web_restarter() -> subprocess.Popen[Any]:
    return subprocess.Popen(
        _web_restarter_command(),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )

def _shutdown_web_after_response(server: ThreadingHTTPServer) -> None:
    time.sleep(_WEB_RESTART_DELAY_SECONDS)
    server.shutdown()
    server.server_close()

def _schedule_web_restart(server: ThreadingHTTPServer) -> None:
    _start_web_restarter()
    thread = threading.Thread(target=_shutdown_web_after_response, args=(server,), daemon=True)
    thread.start()

def _run_api_command(run: str, cmd: str, *, server: ThreadingHTTPServer | None = None) -> subprocess.CompletedProcess[str]:
    if cmd == "restart-web":
        if server is None:
            raise ValueError("restart-web requires a server")
        _schedule_web_restart(server)
        return subprocess.CompletedProcess(["restart-web"], 0, stdout="web restart requested\n", stderr="")
    if cmd == "del":
        coordinator_runs = _validate_web_delete(run)
        result = _run_web_cli(["del", run, "--force"], coordinator_runs=coordinator_runs)
        if result.returncode == 0:
            _delete_web_run(run)
        return result
    if cmd == "hide":
        _hide_web_run(run)
        return subprocess.CompletedProcess(["hide", run], 0, stdout="run hidden\n", stderr="")
    if cmd in ("stop", "resume", "retry"):
        return _run_web_cli([cmd, run])
    raise ValueError(f"unknown command: {cmd}")


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
                cmd = str(req["cmd"])
                run = str(req.get("run", ""))
                result = _run_api_command(run, cmd, server=self.server)
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
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            self._respond(200, "text/html; charset=utf-8", _HTML_BYTES)
        elif path == "/favicon.png":
            body = _favicon_bytes()
            if body is None:
                self.send_response(404)
                self.end_headers()
            else:
                self._respond(200, "image/png", body)
        elif path == "/api/status":
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
    global _WEB_HOST, _WEB_PORT
    host = str(args.host)
    port = int(args.port)
    _WEB_HOST = host
    _WEB_PORT = port
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
