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

// ── Filter toggle ──────────────────────────────────────────────────
function toggleFilter(on) {
  if (on) show($('filter-config')); else hide($('filter-config'));
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
    files:                 S.files,
    fields,
    workers:               parseInt($('workers-slider').value, 10),
    db_enabled:            $('db-toggle').checked,
    db_name:               $('db-name').value.trim(),
    db_user:               $('db-user').value.trim(),
    db_password:           $('db-password').value,
    db_schema:             $('db-schema').value.trim(),
    db_table:              $('db-table').value.trim(),
    db_save_source:        $('db-save-source').checked,
    classification_prompt: $('filter-toggle').checked
      ? $('filter-prompt').value.trim()
      : '',
    per_field: $('per-field-toggle').checked,
  };

  // Reset processing page
  $('st-done').textContent     = '0';
  $('st-total').textContent    = S.files.length;
  $('st-success').textContent  = '0';
  $('st-failed').textContent   = '0';
  $('st-rejected').textContent = '0';
  $('st-speed').textContent    = '—';
  $('st-eta').textContent      = '—';
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
  if (d.successful !== undefined) $('st-success').textContent  = d.successful;
  if (d.failed     !== undefined) $('st-failed').textContent   = d.failed;
  if (d.rejected   !== undefined) $('st-rejected').textContent = d.rejected;

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

  const rejected = d.last_rejected || null;
  const err      = d.last_error  || d.error || '';
  const name     = d.last_file || d.file || '—';
  const shortName = name.replace(/.*[/\\]/, '');

  let icon, suffix;
  if (rejected) {
    icon   = '⏭';
    suffix = `<span class="log-skip-txt">— отклонён: ${esc(rejected)}</span>`;
  } else if (err) {
    icon   = '❌';
    suffix = `<span class="log-err-txt">— ${esc(err)}</span>`;
  } else {
    icon   = '✅';
    suffix = '';
  }

  entry.innerHTML =
    `<span class="log-icon">${icon}</span>` +
    `<span class="log-file">${esc(shortName)}</span>` +
    suffix;

  const list = $('log-list');
  list.appendChild(entry);
  list.scrollTop = list.scrollHeight;
}

// ── Results page ───────────────────────────────────────────────────
function showResults(d) {
  $('r-total').textContent    = d.total;
  $('r-success').textContent  = d.successful;
  $('r-failed').textContent   = d.failed;
  $('r-rejected').textContent = d.rejected ?? 0;
  $('r-rate').textContent     = d.total > 0 ? `${Math.round(d.successful / d.total * 100)}%` : '—';

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
  $('db-toggle').checked       = false;
  $('db-name').value           = '';
  $('db-user').value           = '';
  $('db-password').value       = '';
  $('db-save-source').checked  = true;
  hide($('db-config'));
  $('filter-toggle').checked = false;
  $('filter-prompt').value   = '';
  hide($('filter-config'));
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

// ── Preview modal ──────────────────────────────────────────────────
let _previewData = null;

async function previewResults() {
  if (!S.sessionId) { toast('Нет данных для предпросмотра'); return; }

  const modal = $('preview-modal');
  $('preview-table-wrap').innerHTML = '<div class="preview-loading">Загрузка…</div>';
  $('preview-table-wrap').classList.remove('hidden');
  $('preview-json-wrap').classList.add('hidden');
  $('tab-table').classList.add('active');
  $('tab-json').classList.remove('active');
  show(modal);
  document.body.style.overflow = 'hidden';

  try {
    const res = await fetch(`/api/preview/${S.sessionId}`);
    if (!res.ok) throw new Error('Ошибка загрузки данных');
    _previewData = await res.json();
    renderPreviewTable(_previewData);
  } catch (e) {
    $('preview-table-wrap').innerHTML =
      `<div class="preview-error">Ошибка: ${esc(e.message)}</div>`;
  }
}

function closePreview() {
  hide($('preview-modal'));
  document.body.style.overflow = '';
}

function switchPreviewTab(tab) {
  if (!_previewData) return;
  const isTable = tab === 'table';
  $('tab-table').classList.toggle('active', isTable);
  $('tab-json').classList.toggle('active', !isTable);
  $('preview-table-wrap').classList.toggle('hidden', isTable ? false : true);
  $('preview-json-wrap').classList.toggle('hidden', isTable ? true : false);
  if (!isTable) $('preview-json-code').textContent = JSON.stringify(_previewData, null, 2);
}

function renderPreviewTable(data) {
  const fieldNames = [];
  const seen = new Set();
  for (const row of data) {
    if (row.status === 'ok' && row.data) {
      for (const k of Object.keys(row.data)) {
        if (!seen.has(k)) { seen.add(k); fieldNames.push(k); }
      }
    }
  }

  const hasNonOk = data.some(r => r.status !== 'ok');
  const nf = fieldNames.length;

  let html = '<table class="preview-table"><thead><tr>';
  html += '<th>#</th><th>Файл</th>';
  for (const f of fieldNames) html += `<th>${esc(f)}</th>`;
  if (hasNonOk) html += '<th>Статус</th>';
  html += '</tr></thead><tbody>';

  data.forEach((row, i) => {
    const fname = (row.file || '').replace(/.*[/\\]/, '');
    if (row.status === 'ok') {
      html += `<tr><td class="td-num">${i + 1}</td><td class="td-file">${esc(fname)}</td>`;
      for (const f of fieldNames) html += `<td>${fmtCell(row.data?.[f])}</td>`;
      if (hasNonOk) html += '<td><span class="status-ok">✓</span></td>';
    } else if (row.status === 'error') {
      html += `<tr class="tr-error"><td class="td-num">${i + 1}</td><td class="td-file">${esc(fname)}</td>`;
      if (nf > 0) html += `<td colspan="${nf}" class="td-err-msg">${esc(row.error || 'Ошибка')}</td>`;
      if (hasNonOk) html += '<td><span class="status-err">Ошибка</span></td>';
    } else {
      html += `<tr class="tr-rejected"><td class="td-num">${i + 1}</td><td class="td-file">${esc(fname)}</td>`;
      if (nf > 0) html += `<td colspan="${nf}" class="td-skip-msg">${esc(row.reason || 'Отклонён')}</td>`;
      if (hasNonOk) html += '<td><span class="status-skip">Отклонён</span></td>';
    }
    html += '</tr>';
  });

  html += '</tbody></table>';
  $('preview-table-wrap').innerHTML = html;
}

function fmtCell(val) {
  if (val === null || val === undefined) return '<span class="cell-null">—</span>';
  if (Array.isArray(val)) {
    return val.map(v => `<span class="cell-tag">${esc(String(v))}</span>`).join(' ');
  }
  return esc(String(val));
}

document.addEventListener('keydown', e => {
  if (e.key === 'Escape') closePreview();
});

// ── Initialise ─────────────────────────────────────────────────────
loadPresetsList();
addField(); // one empty row to start
