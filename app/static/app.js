'use strict';

// ── State ──────────────────────────────────────────────────────────
const S = {
  files: [],
  sessionId: localStorage.getItem('drSessionId') || null,
  ws: null,
  errorsVisible: false,
};

let _fieldCounter = 0;

// ── DOM helpers ────────────────────────────────────────────────────
const $  = id => document.getElementById(id);
const show = el => el.classList.remove('hidden');
const hide = el => el.classList.add('hidden');

// ── Navigation ─────────────────────────────────────────────────────
function goTo(page) {
  ['config', 'processing', 'results'].forEach(p => {
    $(`page-${p}`).classList.toggle('hidden', p !== page);
  });
  $('nav-config').classList.toggle('active', page === 'config');
  $('nav-config').classList.toggle('done',   page !== 'config');
  $('nav-processing').classList.toggle('active', page === 'processing');
  $('nav-processing').classList.toggle('done',   page === 'results');
  $('nav-results').classList.toggle('active', page === 'results');
}

// ── Folder scan ────────────────────────────────────────────────────
async function scanFolder() {
  const folder = $('folder-path').value.trim();
  if (!folder) { toast('Введите путь к папке'); return; }

  const el = $('scan-result');
  el.innerHTML = '<span style="color:var(--text-muted)">Сканирование…</span>';

  try {
    const res = await fetch('/api/scan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ folder }),
    });

    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Ошибка');

    S.files = data.files;
    if (data.count === 0) {
      el.innerHTML = '<span class="err">PDF файлы не найдены</span>';
    } else {
      el.innerHTML = `<span class="ok">Найдено PDF: <strong>${data.count}</strong></span>`;
    }
  } catch (e) {
    S.files = [];
    el.innerHTML = `<span class="err">Ошибка: ${esc(e.message)}</span>`;
  }

  checkStartEnabled();
}

// ── Field editor ───────────────────────────────────────────────────
function addField(name = '', desc = '') {
  const id = ++_fieldCounter;
  const row = document.createElement('div');
  row.className = 'field-row';
  row.id = `fr-${id}`;
  row.innerHTML = `
    <input type="text" class="input fn" placeholder="Название поля"          value="${esc(name)}"/>
    <input type="text" class="input fd" placeholder="Описание / откуда брать" value="${esc(desc)}"/>
    <button class="btn-icon" onclick="removeField(${id})" title="Удалить">×</button>
  `;
  row.querySelector('.fn').addEventListener('input', checkStartEnabled);
  $('fields-list').appendChild(row);
  checkStartEnabled();
}

function removeField(id) {
  const el = $(`fr-${id}`);
  if (el) el.remove();
  checkStartEnabled();
}

function getFields() {
  return Array.from($('fields-list').querySelectorAll('.field-row'))
    .map(r => ({
      name: r.querySelector('.fn').value.trim(),
      description: r.querySelector('.fd').value.trim(),
    }))
    .filter(f => f.name);
}

// ── Presets ────────────────────────────────────────────────────────
async function loadPresetsList() {
  try {
    const presets = await (await fetch('/api/presets')).json();
    const sel = $('preset-select');
    sel.innerHTML = '<option value="">— Выберите пресет —</option>';
    presets.forEach(p => {
      const opt = document.createElement('option');
      opt.value = p.name;
      opt.textContent = p.name;
      sel.appendChild(opt);
    });
    // store for fast access
    sel._data = presets;
  } catch (_) { /* offline / no presets yet */ }
}

function loadPreset() {
  const sel   = $('preset-select');
  const name  = sel.value;
  const data  = (sel._data || []).find(p => p.name === name);
  if (!data) { toast('Выберите пресет из списка'); return; }

  $('fields-list').innerHTML = '';
  _fieldCounter = 0;
  data.fields.forEach(f => addField(f.name, f.description || ''));
  toast(`Пресет «${name}» загружен`);
}

async function savePreset() {
  const name   = $('preset-name').value.trim();
  const fields = getFields();
  if (!name)           { toast('Введите название пресета');  return; }
  if (!fields.length)  { toast('Добавьте хотя бы одно поле'); return; }

  try {
    const res = await fetch('/api/presets', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, fields }),
    });
    if (!res.ok) throw new Error();
    await loadPresetsList();
    $('preset-name').value = '';
    toast(`Пресет «${name}» сохранён`);
  } catch (_) { toast('Ошибка сохранения пресета'); }
}

async function deletePreset() {
  const name = $('preset-select').value;
  if (!name) { toast('Выберите пресет для удаления'); return; }
  if (!confirm(`Удалить пресет «${name}»?`)) return;

  try {
    await fetch(`/api/presets/${encodeURIComponent(name)}`, { method: 'DELETE' });
    await loadPresetsList();
    toast(`Пресет «${name}» удалён`);
  } catch (_) { toast('Ошибка удаления'); }
}

// ── DB toggle ──────────────────────────────────────────────────────
function toggleDB(on) {
  if (on) show($('db-config')); else hide($('db-config'));
}

// ── Start button guard ─────────────────────────────────────────────
function checkStartEnabled() {
  $('start-btn').disabled = !(S.files.length > 0 && getFields().length > 0);
}

// ── Start processing ───────────────────────────────────────────────
function startProcessing() {
  const fields  = getFields();
  if (!S.files.length || !fields.length) return;

  const config = {
    files:      S.files,
    fields,
    workers:    parseInt($('workers-slider').value, 10),
    db_enabled: $('db-toggle').checked,
    db_schema:  $('db-schema').value.trim(),
    db_table:   $('db-table').value.trim(),
  };

  // Reset processing page
  $('st-done').textContent    = '0';
  $('st-total').textContent   = S.files.length;
  $('st-success').textContent = '0';
  $('st-failed').textContent  = '0';
  $('st-speed').textContent   = '—';
  $('st-eta').textContent     = '—';
  $('progress-fill').style.width = '0%';
  $('progress-pct').textContent  = '0%';
  $('last-file').textContent  = 'Подготовка…';
  $('log-list').innerHTML     = '';

  // Unlock nav
  $('nav-processing').disabled = false;
  goTo('processing');

  // Open WebSocket
  if (S.ws) S.ws.close();
  const ws = new WebSocket(`ws://${location.host}/api/ws/process`);
  S.ws = ws;

  ws.onopen  = () => ws.send(JSON.stringify(config));
  ws.onmessage = e => handleMsg(JSON.parse(e.data));
  ws.onerror = () => appendLog({ success: false, file: 'WebSocket', error: 'Ошибка соединения' });
}

// ── WebSocket messages ─────────────────────────────────────────────
function handleMsg(d) {
  switch (d.type) {
    case 'progress':
      updateProgress(d);
      if (d.last_file) appendLog(d);
      break;

    case 'complete':
      S.sessionId = d.session_id;
      localStorage.setItem('drSessionId', d.session_id);
      updateProgress({ ...d, done: d.total }); // fill bar to 100%
      $('last-file').textContent = 'Обработка завершена';
      $('nav-results').disabled = false;
      showResults(d);
      break;

    case 'error':
      appendLog({ success: false, file: 'Системная ошибка', error: d.message });
      break;

    case 'keepalive':
      break; // no-op
  }
}

// ── Progress update ────────────────────────────────────────────────
function updateProgress(d) {
  if (d.done  !== undefined) $('st-done').textContent    = d.done;
  if (d.total !== undefined) $('st-total').textContent   = d.total;
  if (d.successful !== undefined) $('st-success').textContent = d.successful;
  if (d.failed     !== undefined) $('st-failed').textContent  = d.failed;

  if (d.speed !== undefined && d.speed > 0) {
    $('st-speed').textContent = d.speed.toFixed(1);
    $('st-eta').textContent   = fmtTime(d.eta);
  }

  const pct = d.total > 0 ? Math.round(d.done / d.total * 100) : 0;
  $('progress-fill').style.width = `${pct}%`;
  $('progress-pct').textContent  = `${pct}%`;

  if (d.last_file) $('last-file').textContent = `→ ${d.last_file}`;
}

// ── Log entry ──────────────────────────────────────────────────────
function appendLog(d) {
  const entry = document.createElement('div');
  entry.className = 'log-entry';

  const icon = d.last_success !== undefined ? d.last_success : d.success;
  const name = d.last_file || d.file || '—';
  const err  = d.last_error  || d.error || '';
  const shortName = name.replace(/.*[/\\]/, '');

  entry.innerHTML =
    `<span class="log-icon">${icon ? '✅' : '❌'}</span>` +
    `<span class="log-file">${esc(shortName)}</span>` +
    (err ? `<span class="log-err-txt">— ${esc(err)}</span>` : '');

  const list = $('log-list');
  list.appendChild(entry);
  list.scrollTop = list.scrollHeight;
}

// ── Results page ───────────────────────────────────────────────────
function showResults(d) {
  $('r-total').textContent   = d.total;
  $('r-success').textContent = d.successful;
  $('r-failed').textContent  = d.failed;
  $('r-rate').textContent    = d.total > 0 ? `${Math.round(d.successful / d.total * 100)}%` : '—';

  if (d.failed > 0 && d.error_files?.length) {
    $('r-err-count').textContent = d.failed;
    $('err-list').innerHTML = d.error_files.map(e =>
      `<div class="err-item">
        <span class="err-file">${esc(e.file.replace(/.*[/\\]/, ''))}</span>
        <span class="err-msg">${esc(e.error || 'Неизвестная ошибка')}</span>
       </div>`
    ).join('');
    show($('errors-card'));
    S.errorsVisible = true;
  } else {
    hide($('errors-card'));
  }

  if (d.db_stats) {
    $('r-inserted').textContent = d.db_stats.inserted;
    $('r-db-err').textContent   = d.db_stats.errors;
    show($('db-res-card'));
  } else {
    hide($('db-res-card'));
  }

  goTo('results');
}

// ── Collapsible errors ─────────────────────────────────────────────
function toggleErrors() {
  S.errorsVisible = !S.errorsVisible;
  $('err-list').classList.toggle('hidden', !S.errorsVisible);
  $('err-chevron').textContent = S.errorsVisible ? '▲' : '▼';
}

// ── Download ───────────────────────────────────────────────────────
function downloadJSON() {
  if (!S.sessionId) { toast('Нет данных для скачивания'); return; }
  window.location.href = `/api/download/${S.sessionId}`;
}

// ── Reset ──────────────────────────────────────────────────────────
function resetApp() {
  S.files = [];
  if (S.ws) { S.ws.close(); S.ws = null; }

  $('folder-path').value   = '';
  $('scan-result').innerHTML = '';
  $('fields-list').innerHTML = '';
  _fieldCounter = 0;
  $('db-toggle').checked   = false;
  hide($('db-config'));
  hide($('errors-card'));
  hide($('db-res-card'));

  $('nav-processing').disabled = true;
  $('nav-results').disabled    = true;

  addField(); // start with one empty row
  checkStartEnabled();
  goTo('config');
}

// ── Utilities ──────────────────────────────────────────────────────
function fmtTime(sec) {
  if (!sec || sec <= 0) return '—';
  if (sec < 60)   return `${Math.round(sec)} сек`;
  if (sec < 3600) return `${Math.floor(sec / 60)} мин ${Math.round(sec % 60)} сек`;
  return `${Math.floor(sec / 3600)} ч ${Math.floor((sec % 3600) / 60)} мин`;
}

function esc(s) {
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function toast(msg) {
  const t = $('toast');
  t.textContent = msg;
  t.classList.add('visible');
  clearTimeout(t._tid);
  t._tid = setTimeout(() => t.classList.remove('visible'), 2400);
}

// ── Initialise ─────────────────────────────────────────────────────
loadPresetsList();
addField(); // one empty row to start
