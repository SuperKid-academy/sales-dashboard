// ============================================================
// AmoCRM → Google Sheets Sync (Детская прямая pipeline)
// ============================================================

const CONFIG = {
  AMO_DOMAIN: 'superkid.amocrm.ru',
  // Долгосрочный токен (Bearer) — выпускается разово в самой интеграции AmoCRM,
  // раздел «Ключи и доступы» → «Сгенерировать токен». Живёт годами; заменяет
  // весь OAuth-обмен (authorization code → access/refresh) — тыкается напрямую
  // в Authorization: Bearer.
  LONG_TOKEN: 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImFjMDhmYWQxYTcyZjEwNDk4NjY5ZDQ2OTIwZjU0ZGQ3NDY0YzE1YTVhZDFkNGI3MWJkMjZjZjA3ODNhMWY1NTk0YTFkZTczNzM1OGNhOWU3In0.eyJhdWQiOiIzYmNiZTE3OC03NzNhLTQzMTktOTM1ZC00MGM4ODVhMzUzNmIiLCJqdGkiOiJhYzA4ZmFkMWE3MmYxMDQ5ODY2OWQ0NjkyMGY1NGRkNzQ2NGMxNWE1YWQxZDRiNzFiZDI2Y2YwNzgzYTFmNTU5NGExZGU3MzczNThjYTllNyIsImlhdCI6MTc4Nzc1MjE2MSwibmJmIjoxNzg3NzUyMTYxLCJleHAiOjE5MzUzNjAwMDAsInN1YiI6IjE0MTQyMzE0IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMwMDY5NTc0LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsImZpbGVzX2RlbGV0ZSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiZjE5MmJkMDYtZjcxYy00ZmVhLThjY2EtMGFmNzMzOWIxZWZiIiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.bDGRK43Qd1qbpzbfZ3rqJDwPXPM8jB5FynM1QZqpwtvx4_QE6hlMb2hfPAhrmEAYzGSRXrHJ1jp-EeNJlpnNl6Gj4XUqOZOHg1wWAVILF-wdMGVQm9sIj8FpZTEr5NoekVh27cxG0fRTmzTdpyWmKO6EJ_x1bhjMLlunTIHb5mkx32X1DbvdSa-h72GEQi9IflOoPUgtnl_5ytnIxpXuq89a-7GALl8yKMoMIaixM7RAZpjonlJt0DAWhzwHEljFgJK3g6V04SMn6-DRSPdH12cOY7KFazi8O5ePJqSLm7Fvde9bWVaEUHDE688psrsoU2vnk2aJAlwO4HlVX4YxLw',
  SHEET_ID: '1-pp7DhXzNK9xqat52lOu3tODRGtxuA3E65PKS78jeEY',
};

// pipelineName пишется в лист как префикс статуса ("Продления / X").
// Дашборд ищет по этим строкам ('продления', 'детская прямая') — держим
// исторические значения, даже если реальная воронка в Amo названа иначе.
const PIPELINES = {
  detskaya: {
    kind: 'detskaya',
    pipelineId: 5326345, // "Детская прямая" в superkid.amocrm.ru
    pipelineName: 'Детская прямая',
    sheetName: null, // null = first sheet
    trackOUHistory: true,
  },
  renewal: {
    kind: 'renewal',
    pipelineId: 11203938, // "Воронка продления" в superkid.amocrm.ru
    pipelineName: 'Продления', // ← сохраняем старый префикс для дашборда
    sheetName: 'Продления',
    trackOUHistory: false,
  },
};

// ============================================================
// TOKEN MANAGEMENT
// ============================================================

function getProps() {
  return PropertiesService.getScriptProperties();
}

// С долгосрочным токеном обмен не нужен — он уже сам access-token, живущий годами.
/**
 * Токен берётся из Script Properties, а не из кода: этот файл лежит в
 * публичном репозитории, и вписанный сюда LONG_TOKEN был доступен любому.
 * Записать новый токен один раз: запустить setAmoToken('...') из редактора,
 * затем удалить вызов, чтобы значение не осело в истории.
 */
function getAccessToken() {
  const fromProps = getProps().getProperty('AMO_LONG_TOKEN');
  if (fromProps) return fromProps;
  if (CONFIG.LONG_TOKEN) {
    Logger.log('⚠️ Токен берётся из кода. Перенеси его в Script Properties: setAmoToken("...")');
    return CONFIG.LONG_TOKEN;
  }
  throw new Error('Нет токена AmoCRM. Запусти setAmoToken("новый_токен") из редактора.');
}

/** Разовая запись токена в Script Properties. Вызвать вручную, потом стереть аргумент. */
function setAmoToken(token) {
  if (!token) { Logger.log('Передай токен аргументом: setAmoToken("eyJ...")'); return; }
  getProps().setProperty('AMO_LONG_TOKEN', token);
  Logger.log('✅ Токен сохранён в Script Properties. Теперь удали его из CONFIG.LONG_TOKEN.');
}

// Сброс маркеров инкрементального синка — использовать разово при смене
// AmoCRM-аккаунта, чтобы следующий прогон был полным.
function resetTokens() {
  const props = getProps();
  // Старые OAuth-ключи (на случай, если остались от прошлой схемы).
  props.deleteProperty('amo_access_token');
  props.deleteProperty('amo_refresh_token');
  props.deleteProperty('amo_token_expires');
  // Sync-маркеры.
  props.deleteProperty('last_sync_ts_detskaya');
  props.deleteProperty('last_full_sync_ts_detskaya');
  props.deleteProperty('last_sync_ts_renewal');
  props.deleteProperty('last_full_sync_ts_renewal');
  Logger.log('Sync markers cleared. Next run will be a FULL sync.');
}

// Диагностика: логирует список пользователей аккаунта — их id, имя, email
// и активность. Помогает выяснить, почему в дашборде «нет менеджеров»
// (id ответственного не мапится на юзера из /api/v4/users).
function listUsers() {
  const users = {};
  let page = 1;
  let total = 0;
  while (true) {
    const data = amoFetch(`/api/v4/users?page=${page}&limit=250`);
    if (!data || !data._embedded || !data._embedded.users) break;
    data._embedded.users.forEach(u => {
      users[u.id] = u;
      total++;
      Logger.log(`id=${u.id}  name="${u.name}"  email=${u.email}  active=${u.rights && u.rights.is_active}`);
    });
    if (data._embedded.users.length < 250) break;
    page++;
  }
  Logger.log(`Всего пользователей: ${total}`);
  return users;
}

// Разово вызвать, чтобы вытащить ID воронок и статусов из нового аккаунта.
// Логирует пары «имя → id». Далее их нужно вписать в PIPELINES.*.pipelineId.
function listPipelines() {
  const data = amoFetch('/api/v4/leads/pipelines');
  if (!data || !data._embedded || !data._embedded.pipelines) {
    Logger.log('No pipelines returned');
    return;
  }
  data._embedded.pipelines.forEach(p => {
    Logger.log(`pipelineId=${p.id}  name="${p.name}"`);
    if (p._embedded && p._embedded.statuses) {
      p._embedded.statuses.forEach(s => {
        Logger.log(`    statusId=${s.id}  "${s.name}"`);
      });
    }
  });
}

// ============================================================
// AMO CRM API
// ============================================================

function amoFetch(path, options) {
  const token = getAccessToken();
  const url = `https://${CONFIG.AMO_DOMAIN}${path}`;
  const res = UrlFetchApp.fetch(url, {
    method: options?.method || 'get',
    headers: { 'Authorization': 'Bearer ' + token },
    contentType: 'application/json',
    muteHttpExceptions: true,
    ...(options?.payload ? { payload: JSON.stringify(options.payload) } : {}),
  });

  if (res.getResponseCode() === 401) {
    // Долгосрочный токен либо отозван, либо неверный — refresh невозможен, только руками.
    throw new Error('AmoCRM 401 Unauthorized. Проверь LONG_TOKEN в CONFIG (возможно, отозван).');
  }

  if (res.getResponseCode() === 204) return null;
  return JSON.parse(res.getContentText());
}

// ============================================================
// FETCH PIPELINE STATUSES
// ============================================================

function getPipelineStatuses(pipelineId) {
  const data = amoFetch(`/api/v4/leads/pipelines/${pipelineId}`);
  const statuses = {};
  if (data && data._embedded && data._embedded.statuses) {
    data._embedded.statuses.forEach(s => {
      statuses[s.id] = s.name;
    });
  }
  return statuses;
}

// ============================================================
// FETCH USERS (managers)
// ============================================================

function getUsers() {
  const users = {};
  let page = 1;
  while (true) {
    const data = amoFetch(`/api/v4/users?page=${page}&limit=250`);
    if (!data || !data._embedded || !data._embedded.users) break;
    data._embedded.users.forEach(u => { users[u.id] = u.name; });
    if (data._embedded.users.length < 250) break;
    page++;
  }
  return users;
}

// ============================================================
// FETCH ALL DEALS FROM PIPELINE
// ============================================================

function fetchAllDeals(pipelineId, sinceTs) {
  // order[id]=asc is required for stable pagination. Without it AmoCRM sorts by
  // updated_at desc by default, and any deal updated during the sync migrates
  // between pages — causing deals to be skipped or duplicated across pages.
  //
  // sinceTs (Unix seconds) — если задано, тянем только сделки с updated_at
  // позже него. Используется для инкрементального синка.
  const deals = [];
  const seen = {};
  let page = 1;
  while (true) {
    let url = `/api/v4/leads?filter[pipeline_id]=${pipelineId}&with=contacts&order[id]=asc&limit=250&page=${page}`;
    if (sinceTs) url += `&filter[updated_at][from]=${sinceTs}`;
    const data = amoFetch(url);
    if (!data || !data._embedded || !data._embedded.leads) break;
    const batch = data._embedded.leads;
    batch.forEach(function(d) {
      if (!seen[d.id]) { seen[d.id] = 1; deals.push(d); }
    });
    if (batch.length < 250) break;
    page++;
    Utilities.sleep(300); // Rate limit: 7 req/sec
  }
  Logger.log(`Fetched ${deals.length} deals${sinceTs ? ' (incremental since ' + sinceTs + ')' : ' (full)'}`);
  return deals;
}

// ============================================================
// FETCH CONTACTS (batch)
// ============================================================

function fetchContacts(contactIds) {
  const contacts = {};
  // Batch in groups of 25 (URL length limit)
  for (let i = 0; i < contactIds.length; i += 25) {
    const batch = contactIds.slice(i, i + 25);
    const filter = batch.map(id => `filter[id][]=${id}`).join('&');
    const data = amoFetch(`/api/v4/contacts?${filter}&limit=250`);
    if (data && data._embedded && data._embedded.contacts) {
      data._embedded.contacts.forEach(c => {
        let phone = '', parentUser = '';
        if (c.custom_fields_values) {
          const phoneField = c.custom_fields_values.find(f => f.field_code === 'PHONE');
          if (phoneField && phoneField.values && phoneField.values[0]) {
            phone = phoneField.values[0].value;
          }
          const userField = c.custom_fields_values.find(f => f.field_name === 'Юзер родителя');
          if (userField && userField.values && userField.values[0]) {
            parentUser = userField.values[0].value;
          }
        }
        contacts[c.id] = { name: c.name, phone, parentUser };
      });
    }
    Utilities.sleep(200);
  }
  return contacts;
}

// ============================================================
// CUSTOM FIELD HELPERS
// ============================================================

// Названия полей в AMO правятся вручную, поэтому сверяем их нормализованно:
// лишний пробел, неразрывный пробел или другой регистр не должны ронять
// выгрузку в пустую колонку.
function normalizeFieldName(s) {
  return String(s || '')
    .replace(/ /g, ' ')   // неразрывный пробел
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

// Запасные названия для полей, которые в AMO могли переименовать.
// Первое совпадение выигрывает, порядок — от точного к более общему.
// ВНИМАНИЕ: «Дата проведения ОУ» сюда не добавлять — это отдельное поле
// (факт проведения, 631 заполнено), оно уже выгружается в свою колонку N.
// Вкладке «Открытые уроки» нужно назначенное время урока — «Дата и время ОУ».
var FIELD_ALIASES = {
  'Дата ОУ':        ['Дата ОУ', 'Дата и время ОУ', 'Дата открытого урока'],
  'Подтвердил ОУ':  ['Подтвердил ОУ', 'Подтверждение ОУ', 'Подтвердил'],
  'Был на ОУ':      ['Был на ОУ', 'Был на открытом уроке', 'Посетил ОУ'],
};

function getCustomFieldValue(deal, fieldName) {
  if (!deal.custom_fields_values) return '';

  var candidates = FIELD_ALIASES[fieldName] || [fieldName];
  for (var i = 0; i < candidates.length; i++) {
    var target = normalizeFieldName(candidates[i]);
    var field = deal.custom_fields_values.find(function(f) {
      return normalizeFieldName(f.field_name) === target;
    });
    if (field && field.values && field.values[0]) {
      // Multi-select: join all values with comma
      if (field.values.length > 1) {
        return field.values.map(function(v) { return v.value; }).join(', ');
      }
      return field.values[0].value;
    }
  }
  return '';
}

/**
 * ДИАГНОСТИКА полей ОУ. Запустить вручную из редактора Apps Script, когда
 * колонка выгружается пустой: показывает, находятся ли поля «Дата ОУ»,
 * «Подтвердил ОУ», «Был на ОУ» в AMO и сколько сделок их заполнили.
 * Результат — в «Просмотр» → «Журнал выполнения».
 */
function debugOUFields() {
  const deals = fetchAllDeals(PIPELINES.detskaya.pipelineId);
  if (!deals || !deals.length) {
    Logger.log('Сделок не получено — проверь токен и pipelineId.');
    return;
  }

  // Сколько раз каждое поле встречается заполненным
  const filled = {};
  const present = {};
  deals.forEach(d => {
    (d.custom_fields_values || []).forEach(f => {
      present[f.field_name] = (present[f.field_name] || 0) + 1;
      const v = f.values && f.values[0] ? f.values[0].value : '';
      if (v !== '' && v != null) filled[f.field_name] = (filled[f.field_name] || 0) + 1;
    });
  });

  Logger.log('Сделок в выборке: ' + deals.length);
  Logger.log('');
  Logger.log('=== Поля, нужные вкладке «Открытые уроки» ===');
  ['Дата ОУ', 'Подтвердил ОУ', 'Был на ОУ'].forEach(want => {
    const aliases = FIELD_ALIASES[want] || [want];
    let hit = null;
    for (const a of aliases) {
      const t = normalizeFieldName(a);
      hit = Object.keys(present).find(k => normalizeFieldName(k) === t) || hit;
      if (hit) break;
    }
    if (hit) {
      Logger.log(`  "${want}" → поле "${hit}", заполнено у ${filled[hit] || 0} из ${deals.length}`);
    } else {
      Logger.log(`  "${want}" → В AMO ТАКОГО ПОЛЯ НЕТ (проверь, не переименовали ли его)`);
    }
  });

  Logger.log('');
  Logger.log('=== Примеры значений полей ОУ (проверить формат) ===');
  ['Дата и время ОУ', 'Дата назначения ОУ', 'Дата проведения ОУ'].forEach(name => {
    const t = normalizeFieldName(name);
    const samples = [];
    for (const d of deals) {
      const f = (d.custom_fields_values || []).find(x => normalizeFieldName(x.field_name) === t);
      const v = f && f.values && f.values[0] ? f.values[0].value : '';
      if (v !== '' && v != null) samples.push(v);
      if (samples.length >= 3) break;
    }
    Logger.log(`  "${name}": ${JSON.stringify(samples)}`);
  });

  Logger.log('');
  Logger.log('=== ВСЕ поля сделок (ищем замену «Подтвердил ОУ» и «Был на ОУ») ===');
  Object.keys(present).sort()
    .forEach(k => Logger.log(`  "${k}" → заполнено ${filled[k] || 0} из ${deals.length}`));
}

function formatDate(timestamp) {
  if (!timestamp) return '';
  var ts = Number(timestamp);
  if (isNaN(ts)) return String(timestamp);
  var d = new Date(ts * 1000);
  return Utilities.formatDate(d, Session.getScriptTimeZone(), 'dd.MM.yyyy');
}

function formatDateTime(timestamp) {
  if (!timestamp) return '';
  var ts = Number(timestamp);
  if (isNaN(ts)) return String(timestamp);
  var d = new Date(ts * 1000);
  return Utilities.formatDate(d, Session.getScriptTimeZone(), 'dd.MM.yyyy HH:mm');
}

function formatDateOnly(timestamp) {
  if (!timestamp) return '';
  const d = new Date(timestamp * 1000);
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${dd}.${mm}.${yyyy}`;
}

// ============================================================
// DISCOVER CUSTOM FIELD NAMES
// ============================================================

function discoverFields(pipelineId) {
  const deals = fetchAllDeals(pipelineId || PIPELINES.detskaya.pipelineId);
  const fieldNames = new Set();
  deals.forEach(d => {
    if (d.custom_fields_values) {
      d.custom_fields_values.forEach(f => {
        fieldNames.add(`${f.field_id}: ${f.field_name} (${f.field_type})`);
      });
    }
  });
  const sorted = [...fieldNames].sort();
  sorted.forEach(f => Logger.log(f));
  return sorted;
}

// Прогон discoverFields для обеих воронок — удобно для миграции прокси
// (ему нужны field_id и в «Детской», и в «Продлениях»).
function discoverAllFields() {
  Logger.log('=== Детская прямая ===');
  discoverFields(PIPELINES.detskaya.pipelineId);
  Logger.log('=== Воронка продления ===');
  discoverFields(PIPELINES.renewal.pipelineId);
}

// ============================================================
// MAIN SYNC FUNCTION
// ============================================================

// Минимальный интервал между ПОЛНЫМИ синками (в секундах). Между ними бегаем
// инкрементально — только сделки с updated_at > предыдущего синка. Полный синк
// нужен периодически, чтобы выловить сделки, ушедшие в другую воронку (их
// инкремент-фильтр не вернёт, и они остались бы протухшими в таблице).
const FULL_SYNC_INTERVAL_SEC = 6 * 3600; // 6 часов

function syncPipeline(cfg) {
  const startTime = Date.now();
  const startTs = Math.floor(startTime / 1000); // unix seconds — saved on success

  const layout = LAYOUTS[cfg.kind];
  if (!layout) throw new Error(`Unknown pipeline kind: ${cfg.kind}`);

  const props = getProps();
  const lastSyncKey = 'last_sync_ts_' + cfg.kind;
  const lastFullKey = 'last_full_sync_ts_' + cfg.kind;
  const lastSyncTs = parseInt(props.getProperty(lastSyncKey) || '0', 10);
  const lastFullTs = parseInt(props.getProperty(lastFullKey) || '0', 10);

  // Решаем: полный или инкрементальный синк. Полный — если ни разу не было,
  // либо прошло больше FULL_SYNC_INTERVAL_SEC с предыдущего полного.
  const doFull = !lastSyncTs || (startTs - lastFullTs > FULL_SYNC_INTERVAL_SEC);
  // Накладываем 10-минутный overlap, чтобы не пропустить сделки, обновлённые
  // на границе предыдущего прогона (AmoCRM updated_at — секундный таймстамп,
  // а сетевые задержки могут сдвинуть его).
  const sinceTs = doFull ? null : Math.max(0, lastSyncTs - 600);

  Logger.log(`Starting ${doFull ? 'FULL' : 'INCREMENTAL'} sync: ${cfg.pipelineName}` +
             (sinceTs ? ` (since ${sinceTs})` : ''));

  // 1. Fetch needed data
  const statuses = getPipelineStatuses(cfg.pipelineId);
  const users = getUsers();
  const deals = fetchAllDeals(cfg.pipelineId, sinceTs);

  // Open sheet up front — нужно и для full, и для incremental.
  const ss = SpreadsheetApp.openById(CONFIG.SHEET_ID);
  const sheet = cfg.sheetName ? ss.getSheetByName(cfg.sheetName) : ss.getSheets()[0];
  if (!sheet) throw new Error(`Sheet not found: ${cfg.sheetName}`);
  ensureHeaders(sheet, layout.headers);
  const ncols = layout.headers.length;

  // Инкрементальный без обновлений — короткий путь.
  if (!doFull && deals.length === 0) {
    props.setProperty(lastSyncKey, String(startTs));
    Logger.log(`Sync complete (no updates): ${cfg.pipelineName} in ${((Date.now() - startTime) / 1000).toFixed(1)}s`);
    return;
  }

  // 2. Fetch contacts только для тех deals, что реально пришли.
  const contactIdSet = new Set();
  deals.forEach(d => {
    if (d._embedded && d._embedded.contacts) {
      d._embedded.contacts.forEach(c => contactIdSet.add(c.id));
    }
  });
  const contacts = fetchContacts([...contactIdSet]);

  // 3. Build rows for updated deals
  const ctx = {
    statuses,
    users,
    contacts,
    pipelineName: cfg.pipelineName,
    syncTime: Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'dd.MM.yyyy HH:mm'),
  };
  const updatedRows = deals.map(deal => layout.buildRow(deal, ctx));

  // 4. Merge & write
  let finalRows;
  if (doFull) {
    // Полный синк — содержимое листа полностью замещаем.
    finalRows = updatedRows;
  } else {
    // Инкремент — читаем существующий лист, патчим строки по dealId,
    // дописываем новые. Старые строки, которых нет в апдейте, остаются как есть.
    const lastRow = sheet.getLastRow();
    const existingRows = lastRow > 1
      ? sheet.getRange(2, 1, lastRow - 1, ncols).getValues()
      : [];
    const byId = {};
    const order = [];
    existingRows.forEach(row => {
      const id = row[0];
      if (id === '' || id == null) return;
      const key = String(id);
      byId[key] = row;
      order.push(key);
    });
    updatedRows.forEach(row => {
      const key = String(row[0]);
      if (!(key in byId)) order.push(key); // новая сделка → в конец
      byId[key] = row;
    });
    finalRows = order.map(k => byId[k]);
  }

  // Перезаписываем (тот же объём данных, но без API-задержки)
  const lastRowNow = sheet.getLastRow();
  if (lastRowNow > 1) {
    sheet.getRange(2, 1, lastRowNow - 1, ncols).clearContent();
  }
  if (finalRows.length > 0) {
    sheet.getRange(2, 1, finalRows.length, ncols).setValues(finalRows);
  }

  // 5. Update OU History (only for pipelines that track open lessons).
  // Берём updatedRows: история заполняется только из свежеполученных строк,
  // чтобы не пере-парсить весь лист каждый раз.
  if (cfg.trackOUHistory) {
    const historySheet = getOrCreateHistorySheet();
    const ouHistory = loadOUHistory(historySheet);
    let historyUpdated = false;
    updatedRows.forEach(function(row) {
      const dealId = String(row[0]);
      const dateOUStr = row[17];
      if (dateOUStr && !ouHistory[dealId]) {
        ouHistory[dealId] = dateOUStr;
        historyUpdated = true;
      }
    });
    if (historyUpdated) {
      saveOUHistory(historySheet, ouHistory);
      Logger.log('OU History updated');
    }
  }

  // Save sync timestamps. Полный синк обновляет ОБА маркера; инкремент — только
  // last_sync_ts (чтобы lastFull продолжал отсчитывать 6-часовое окно).
  props.setProperty(lastSyncKey, String(startTs));
  if (doFull) props.setProperty(lastFullKey, String(startTs));

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  Logger.log(`Sync complete: ${cfg.pipelineName} — ${doFull ? 'full' : 'incremental'} ${updatedRows.length} deals, total ${finalRows.length} on sheet, in ${elapsed}s`);
}

// ============================================================
// PER-PIPELINE ROW BUILDERS
// ============================================================

function getContactInfo(deal, contacts) {
  let name = '', phone = '', parentUser = '';
  if (deal._embedded && deal._embedded.contacts && deal._embedded.contacts[0]) {
    const cId = deal._embedded.contacts[0].id;
    if (contacts[cId]) {
      name = contacts[cId].name || '';
      phone = contacts[cId].phone || '';
      parentUser = contacts[cId].parentUser || '';
    }
  }
  return { name, phone, parentUser };
}

function buildDetskayaRow(deal, ctx) {
  const c = getContactInfo(deal, ctx.contacts);
  const manager = ctx.users[deal.responsible_user_id] || '';
  const statusName = ctx.statuses[deal.status_id] || '';
  const fullStatus = ctx.pipelineName + ' / ' + statusName;

  const childName = getCustomFieldValue(deal, 'Имя ребенка');
  const childAge = getCustomFieldValue(deal, 'Возраст ребенка');
  const pains = getCustomFieldValue(deal, 'Боли');
  const dateVR = getCustomFieldValue(deal, 'Дата ВР');
  const dateQual = getCustomFieldValue(deal, 'Дата Квала');
  const dateScheduledOU = getCustomFieldValue(deal, 'Дата назначения ОУ');
  const dateAttendedOU = getCustomFieldValue(deal, 'Дата проведения ОУ');
  const dateInvoice = getCustomFieldValue(deal, 'Дата Выставления счета');
  const datePrepay = getCustomFieldValue(deal, 'Дата предоплаты');
  const dateOU = getCustomFieldValue(deal, 'Дата ОУ');
  const confirmedOU = getCustomFieldValue(deal, 'Подтвердил ОУ');
  const wasOnOU = getCustomFieldValue(deal, 'Был на ОУ');
  const prepayAmount = getCustomFieldValue(deal, 'Сумма предоплаты');
  const daysAvail = getCustomFieldValue(deal, 'Дни когда может');
  const timeAvail = getCustomFieldValue(deal, 'Время когда может');
  const streamNum = getCustomFieldValue(deal, 'Номер потока');
  const language = getCustomFieldValue(deal, 'Язык обучения');
  const product = getCustomFieldValue(deal, 'Продукт');
  const utmSource = getCustomFieldValue(deal, 'utm_source');
  const utmCampaign = getCustomFieldValue(deal, 'utm_campaign');
  const utmMedium = getCustomFieldValue(deal, 'utm_medium');
  const utmTerm = getCustomFieldValue(deal, 'utm_term');
  const utmContent = getCustomFieldValue(deal, 'utm_content');

  const tags = deal._embedded?.tags?.map(t => t.name).join(', ') || '';
  const lossReason = deal.loss_reason?.[0]?.name || '';
  const link = `https://${CONFIG.AMO_DOMAIN}/leads/detail/${deal.id}`;
  // Дата создания выводится вместе со временем — нужно для слотов по часам.
  const createdAt = deal.created_at ? formatDateTime(deal.created_at) : '';
  const closedAt = deal.closed_at ? formatDate(deal.closed_at) : '';

  return [
    deal.id,                     // A
    ctx.syncTime,                // B
    link,                        // C
    manager,                     // D
    c.name,                      // E
    c.phone,                     // F
    childName,                   // G
    childAge,                    // H
    pains,                       // I
    createdAt,                   // J
    formatDate(dateVR),          // K
    formatDate(dateQual),        // L
    formatDate(dateScheduledOU), // M
    formatDate(dateAttendedOU),  // N
    formatDate(dateInvoice),     // O
    formatDate(datePrepay),      // P
    closedAt,                    // Q
    formatDateTime(dateOU),      // R: с временем для слотов
    confirmedOU,                 // S
    deal.price || 0,             // T (budget)
    prepayAmount,                // U
    daysAvail,                   // V
    timeAvail,                   // W
    streamNum,                   // X
    language,                    // Y
    product,                     // Z
    lossReason,                  // AA
    fullStatus,                  // AB
    utmSource,                   // AC
    utmCampaign,                 // AD
    utmMedium,                   // AE
    utmTerm,                     // AF
    utmContent,                  // AG
    tags,                        // AH
    c.parentUser,                // AI
    wasOnOU,                     // AJ
  ];
}

function buildRenewalRow(deal, ctx) {
  const c = getContactInfo(deal, ctx.contacts);
  const manager = ctx.users[deal.responsible_user_id] || '';
  const statusName = ctx.statuses[deal.status_id] || '';
  const fullStatus = ctx.pipelineName + ' / ' + statusName;

  const childName = getCustomFieldValue(deal, 'Имя ребенка');
  const childAge = getCustomFieldValue(deal, 'Возраст ребенка');
  const prepayAmount = getCustomFieldValue(deal, 'Сумма предоплаты');
  const datePrepay = getCustomFieldValue(deal, 'Дата предоплаты');
  const datePrepayRenewal = getCustomFieldValue(deal, 'Дата предоплаты продления');
  const streamNum = getCustomFieldValue(deal, 'Номер потока');
  const language = getCustomFieldValue(deal, 'Язык обучения');
  const product = getCustomFieldValue(deal, 'Продукт');
  const moduleNum = getCustomFieldValue(deal, 'Номер модуля');

  const tags = deal._embedded?.tags?.map(t => t.name).join(', ') || '';
  const lossReason = deal.loss_reason?.[0]?.name || '';
  const link = `https://${CONFIG.AMO_DOMAIN}/leads/detail/${deal.id}`;
  // Дата создания выводится вместе со временем.
  const createdAt = deal.created_at ? formatDateTime(deal.created_at) : '';
  const closedAt = deal.closed_at ? formatDate(deal.closed_at) : '';

  return [
    deal.id,                       // A  Сделка.id
    ctx.syncTime,                  // B  time
    link,                          // C  Сделка.Ссылка
    manager,                       // D  Сделка.Ответственный
    c.name,                        // E  Контакт.ФИО
    c.phone,                       // F  Контакт.Телефон
    childName,                     // G  Сделка.Имя ребенка
    childAge,                      // H  Сделка.Возраст ребенка
    createdAt,                     // I  Сделка.Дата создания
    closedAt,                      // J  Сделка.closed_at
    deal.price || 0,               // K  Сделка.Бюджет
    prepayAmount,                  // L  Сделка.Сумма предоплаты
    formatDate(datePrepay),        // M  Сделка.Дата предоплаты
    formatDate(datePrepayRenewal), // N  Сделка.Дата предоплаты продления
    streamNum,                     // O  Сделка.Номер потока
    language,                      // P  Сделка.Язык обучения
    product,                       // Q  Сделка.Продукт
    lossReason,                    // R  Сделка.loss_reason_name
    fullStatus,                    // S  Сделка.Статус
    tags,                          // T  Сделка.tags
    moduleNum,                     // U  Сделка.Номер модуля
  ];
}

// ============================================================
// PUBLIC ENTRY POINTS
// ============================================================

function syncDeals() {
  syncPipeline(PIPELINES.detskaya);
}

function syncRenewalDeals() {
  syncPipeline(PIPELINES.renewal);
}

// Run both pipelines sequentially. Sequential (not parallel) execution is
// intentional: AmoCRM rate-limits at ~7 req/sec per account, and our internal
// sleeps assume only one sync is in flight at a time.
function syncAll() {
  syncDeals();
  Utilities.sleep(1000); // small breather between pipelines
  syncRenewalDeals();
}

// Принудительно запросить ПОЛНЫЙ синк на следующем прогоне (например, после
// массовой ручной правки в Amo, чтобы подтянуть всё). Просто стираем оба
// маркера — следующий syncAll увидит, что lastSyncTs пуст, и сделает full.
function forceFullSyncNext() {
  const props = getProps();
  Object.keys(PIPELINES).forEach(function(key) {
    const kind = PIPELINES[key].kind;
    props.deleteProperty('last_sync_ts_' + kind);
    props.deleteProperty('last_full_sync_ts_' + kind);
  });
  Logger.log('Sync markers cleared. Next syncAll() will be FULL.');
}

// ============================================================
// HEADERS & LAYOUTS
// ============================================================

const DETSKAYA_HEADERS = [
  'Сделка.id',                 // A
  'Время синхронизации',       // B
  'Ссылка',                    // C
  'Ответственный',             // D
  'Контакт',                   // E
  'Телефон',                   // F
  'Имя ребенка',               // G
  'Возраст ребенка',           // H
  'Боли',                      // I
  'Дата создания',             // J
  'Дата ВР',                   // K
  'Дата Квала',                // L
  'Дата назначения ОУ',        // M
  'Дата проведения ОУ',        // N
  'Дата Выставления счета',    // O
  'Дата предоплаты',           // P
  'Дата закрытия (closed_at)', // Q
  'Дата ОУ',                   // R
  'Подтвердил ОУ',             // S
  'Бюджет',                    // T
  'Сумма предоплаты',          // U
  'Дни когда может',           // V
  'Время когда может',         // W
  'Номер потока',              // X
  'Язык обучения',             // Y
  'Продукт',                   // Z
  'Причина отказа',            // AA
  'Статус',                    // AB
  'utm_source',                // AC
  'utm_campaign',              // AD
  'utm_medium',                // AE
  'utm_term',                  // AF
  'utm_content',               // AG
  'Теги',                      // AH
  'Юзер родителя',             // AI
  'Сделка.Был на ОУ',          // AJ
];

const RENEWAL_HEADERS = [
  'Сделка.id',                        // A
  'time',                             // B
  'Сделка.Ссылка',                    // C
  'Сделка.Ответственный',             // D
  'Контакт.ФИО',                      // E
  'Контакт.Телефон',                  // F
  'Сделка.Имя ребенка',               // G
  'Сделка.Возраст ребенка',           // H
  'Сделка.Дата создания',             // I
  'Сделка.closed_at',                 // J
  'Сделка.Бюджет',                    // K
  'Сделка.Сумма предоплаты',          // L
  'Сделка.Дата предоплаты',           // M
  'Сделка.Дата предоплаты продления', // N
  'Сделка.Номер потока',              // O
  'Сделка.Язык обучения',             // P
  'Сделка.Продукт',                   // Q
  'Сделка.loss_reason_name',          // R
  'Сделка.Статус',                    // S
  'Сделка.tags',                      // T
  'Сделка.Номер модуля',              // U
];

// Function references are hoisted, so it's fine to declare LAYOUTS up here.
const LAYOUTS = {
  detskaya: { headers: DETSKAYA_HEADERS, buildRow: buildDetskayaRow },
  renewal:  { headers: RENEWAL_HEADERS,  buildRow: buildRenewalRow  },
};

function ensureHeaders(sheet, headers) {
  // Always overwrite header row from code — single source of truth.
  // setValues changes only cell values, formatting is preserved.
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  sheet.setFrozenRows(1);
}

// ============================================================
// OU HISTORY — track first-seen OU date for each deal
// (used by dashboard to show original registrations for past days)
// ============================================================

const OU_HISTORY_SHEET_NAME = 'OU History';

function getOrCreateHistorySheet() {
  const ss = SpreadsheetApp.openById(CONFIG.SHEET_ID);
  let sheet = ss.getSheetByName(OU_HISTORY_SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(OU_HISTORY_SHEET_NAME);
    sheet.getRange(1, 1, 1, 2).setValues([['deal_id', 'first_ou_date']]);
  }
  return sheet;
}

function loadOUHistory(sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow <= 1) return {};
  const data = sheet.getRange(2, 1, lastRow - 1, 2).getValues();
  const history = {};
  data.forEach(function(row) {
    const id = String(row[0]);
    if (id && row[1]) history[id] = String(row[1]);
  });
  return history;
}

function saveOUHistory(sheet, history) {
  const entries = Object.entries(history);
  if (entries.length === 0) return;
  const lastRow = sheet.getLastRow();
  if (lastRow > 1) sheet.getRange(2, 1, lastRow - 1, 2).clearContent();
  const rows = entries.map(function(e) { return [e[0], e[1]]; });
  sheet.getRange(2, 1, rows.length, 2).setValues(rows);
}

// ============================================================
// SETUP: С долгосрочным токеном setup не нужен — токен уже в CONFIG.LONG_TOKEN.
// Оставлено как smoke-test: дёргаем /api/v4/leads/pipelines и убеждаемся, что
// токен принимается.
// ============================================================

function setup() {
  try {
    listPipelines();
    Logger.log('✅ LONG_TOKEN works. Далее: listPipelines() → впиши pipelineId → syncAll().');
  } catch (e) {
    Logger.log('❌ Error: ' + e.message);
  }
}

// ============================================================
// TRIGGER: Run every 15 minutes
// ============================================================

const SYNC_HANDLERS = ['syncDeals', 'syncRenewalDeals', 'syncAll'];

function setupTrigger() {
  // Remove any existing sync triggers (including the legacy syncDeals one)
  ScriptApp.getProjectTriggers().forEach(t => {
    if (SYNC_HANDLERS.indexOf(t.getHandlerFunction()) !== -1) {
      ScriptApp.deleteTrigger(t);
    }
  });

  // Single trigger drives both pipelines sequentially
  ScriptApp.newTrigger('syncAll')
    .timeBased()
    .everyMinutes(15)
    .create();

  Logger.log('✅ Trigger set: syncAll every 15 minutes');
}

function removeTrigger() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (SYNC_HANDLERS.indexOf(t.getHandlerFunction()) !== -1) {
      ScriptApp.deleteTrigger(t);
    }
  });
  Logger.log('Trigger removed');
}

// ============================================================
// WEB APP — приём запросов от дашборда
// ============================================================
// Заменяет прокси на Railway (он удалён вместе с исходниками). Логика
// «отметить, что ребёнок был на ОУ» живёт здесь: у скрипта уже есть доступ
// к AmoCRM, отдельный хостинг не нужен.
//
// РАЗВЁРТЫВАНИЕ: «Развернуть» → «Новое развёртывание» → тип «Веб-приложение»,
// «Запуск от имени: Я», «Доступ: Все». Полученный URL вписать в дашборд.
// После правок кода — «Развернуть» → «Управление развёртываниями» →
// карандаш → «Версия: Новая». URL при этом не меняется.

// Куда переводить сделку после отметки посещения.
const OU_ATTENDED_TARGET = {
  pipelineId: 5326345,   // Детская прямая
  statusId: 87908298,    // «Прошел ОУ»
};

function jsonOut(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  try {
    if (!e || !e.postData || !e.postData.contents) {
      return jsonOut({ ok: false, error: 'Пустой запрос' });
    }
    const body = JSON.parse(e.postData.contents);

    // Необязательный общий секрет: если в свойствах задан WEBAPP_SECRET,
    // запросы без него отклоняются.
    const secret = getProps().getProperty('WEBAPP_SECRET');
    if (secret && body.secret !== secret) {
      return jsonOut({ ok: false, error: 'Неверный секрет' });
    }

    switch (body.action) {
      case 'ping':             return jsonOut({ ok: true, pong: true });
      case 'add-note':         return jsonOut(apiAddNote(body));
      case 'move-deal':        return jsonOut(apiMoveDeal(body));
      case 'process-attended': return jsonOut(apiProcessAttended(body));
      default:
        return jsonOut({ ok: false, error: 'Неизвестное действие: ' + body.action });
    }
  } catch (err) {
    return jsonOut({ ok: false, error: String(err && err.message || err) });
  }
}

// Проверка в браузере, что веб-приложение развёрнуто и отвечает.
function doGet() {
  return jsonOut({ ok: true, service: 'superkid-sync', time: new Date().toISOString() });
}

/** Добавляет текстовое примечание к сделке. */
function apiAddNote(body) {
  if (!body.dealId || !body.text) return { ok: false, error: 'Нужны dealId и text' };
  const res = amoFetch(`/api/v4/leads/${body.dealId}/notes`, {
    method: 'post',
    payload: [{ note_type: 'common', params: { text: body.text } }],
  });
  return { ok: true, result: res };
}

/** Переводит сделку в нужный статус воронки. */
function apiMoveDeal(body) {
  if (!body.dealId) return { ok: false, error: 'Нужен dealId' };
  const statusId = body.statusId || OU_ATTENDED_TARGET.statusId;
  const pipelineId = body.pipelineId || OU_ATTENDED_TARGET.pipelineId;
  const res = amoFetch(`/api/v4/leads/${body.dealId}`, {
    method: 'patch',
    payload: { status_id: Number(statusId), pipeline_id: Number(pipelineId) },
  });
  return { ok: true, result: res };
}

/**
 * Полный сценарий отметки посещения ОУ:
 * текст обратной связи (ChatGPT) → примечание в сделку → перевод по воронке.
 * Шаги выполняются по очереди и возвращаются в steps, чтобы при частичном
 * сбое было видно, что успело примениться.
 */
function apiProcessAttended(body) {
  if (!body.dealId) return { ok: false, error: 'Нужен dealId' };
  const steps = [];

  const grades = {
    'Коммуникация':       body.communication || '',
    'Работа в команде':   body.teamwork || '',
    'Компьютерные навыки': body.compSkills || '',
    'Самостоятельность':  body.independence || '',
    'Характер':           body.character || '',
  };

  // 1. Текст обратной связи
  let feedback = '';
  try {
    feedback = generateFeedbackText(body.name, body.age, grades);
    if (feedback) steps.push('chatgpt_ok');
  } catch (err) {
    steps.push('chatgpt_failed: ' + (err && err.message || err));
  }
  if (!feedback) {
    feedback = buildPlainFeedback(body.name, body.age, grades);
    steps.push('used_fallback');
  }

  // 2. Примечание
  try {
    apiAddNote({ dealId: body.dealId, text: feedback });
    steps.push('note_added');
  } catch (err) {
    return { ok: false, steps: steps, error: 'Не удалось добавить примечание: ' + err };
  }

  // 3. Перевод сделки
  try {
    apiMoveDeal({
      dealId: body.dealId,
      statusId: body.statusId,
      pipelineId: body.pipelineId,
    });
    steps.push('deal_moved');
  } catch (err) {
    return { ok: false, steps: steps, error: 'Примечание добавлено, но сделка не переведена: ' + err };
  }

  return { ok: true, steps: steps, feedback: feedback };
}

/** Заметка из оценок без ИИ — запасной вариант и основа промпта. */
function buildPlainFeedback(name, age, grades) {
  const lines = ['📋 Обратная связь по ребёнку: ' + (name || '—') +
                 (age ? ' (' + age + ' лет)' : ''), ''];
  Object.keys(grades).forEach(k => {
    if (grades[k]) lines.push('• ' + k + ': ' + grades[k]);
  });
  return lines.join('\n');
}

/**
 * Связный текст обратной связи через ChatGPT.
 * Ключ хранится в Script Properties: setOpenAiKey('sk-...').
 * Возвращает '' — вызывающий код подставит buildPlainFeedback.
 */
function generateFeedbackText(name, age, grades) {
  const apiKey = getProps().getProperty('OPENAI_API_KEY');
  if (!apiKey) return '';

  const facts = Object.keys(grades)
    .filter(k => grades[k])
    .map(k => k + ': ' + grades[k])
    .join('\n');
  if (!facts) return '';

  const prompt =
    'Ты преподаватель детской школы программирования. По итогам открытого урока ' +
    'напиши родителям короткую обратную связь о ребёнке — 3-4 предложения, ' +
    'дружелюбно и по делу, на русском языке.\n\n' +
    'Ребёнок: ' + (name || 'ученик') + (age ? ', ' + age + ' лет' : '') + '\n' +
    'Наблюдения преподавателя:\n' + facts + '\n\n' +
    'Опирайся только на эти наблюдения, ничего не выдумывай. Отметь сильные ' +
    'стороны и мягко назови, над чем стоит поработать. Без приветствия и подписи.';

  const res = UrlFetchApp.fetch('https://api.openai.com/v1/chat/completions', {
    method: 'post',
    contentType: 'application/json',
    headers: { 'Authorization': 'Bearer ' + apiKey },
    muteHttpExceptions: true,
    payload: JSON.stringify({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: prompt }],
      temperature: 0.7,
      max_tokens: 400,
    }),
  });

  const code = res.getResponseCode();
  if (code !== 200) {
    throw new Error('OpenAI ' + code + ': ' + res.getContentText().slice(0, 200));
  }
  const data = JSON.parse(res.getContentText());
  const text = data.choices && data.choices[0] && data.choices[0].message.content;
  if (!text) return '';

  return '📋 Обратная связь по ребёнку: ' + (name || '—') +
         (age ? ' (' + age + ' лет)' : '') + '\n\n' + text.trim() +
         '\n\n— Оценки преподавателя —\n' +
         Object.keys(grades).filter(k => grades[k])
           .map(k => '• ' + k + ': ' + grades[k]).join('\n');
}

/** Разовая запись ключа OpenAI. Вызвать вручную, потом стереть аргумент. */
function setOpenAiKey(key) {
  if (!key) { Logger.log('Передай ключ аргументом: setOpenAiKey("sk-...")'); return; }
  getProps().setProperty('OPENAI_API_KEY', key);
  Logger.log('✅ Ключ OpenAI сохранён в Script Properties.');
}

/** Проверка сценария на конкретной сделке — без перевода по воронке. */
function debugFeedbackText() {
  const grades = {
    'Коммуникация': 'Активно общается, отвечает на вопросы',
    'Работа в команде': 'Хорошо работает в группе',
    'Компьютерные навыки': 'Базовые',
    'Самостоятельность': 'Нужны подсказки',
    'Характер': 'Активный, эмоциональный',
  };
  Logger.log('--- Без ИИ ---');
  Logger.log(buildPlainFeedback('Тест', 10, grades));
  Logger.log('');
  Logger.log('--- Через ChatGPT ---');
  try {
    const t = generateFeedbackText('Тест', 10, grades);
    Logger.log(t || '(ключ OPENAI_API_KEY не задан — будет использован текст без ИИ)');
  } catch (err) {
    Logger.log('Ошибка: ' + err);
  }
}
