// ============================================================
// AmoCRM → Google Sheets Sync (Детская прямая pipeline)
// ============================================================

const CONFIG = {
  AMO_DOMAIN: 'seniorpartners.amocrm.ru',
  CLIENT_ID: '9c73ec33-fedc-4a24-b895-19d0c0b01c26',
  CLIENT_SECRET: 's0BurVMQB5LrQbj5Zc9sgEnJKbKugzPo7cLZT2K15E20PXqUGDVVrRIsSN136l9Y',
  REDIRECT_URI: 'https://docs.google.com/spreadsheets',
  SHEET_ID: '1j5BtlyOeY2CngENjvv9P3th8Z-VVflAi0iu_gWuAbrg',
};

const PIPELINES = {
  detskaya: {
    kind: 'detskaya',
    pipelineId: 10489658,
    pipelineName: 'Детская прямая',
    sheetName: null, // null = first sheet
    trackOUHistory: true,
  },
  renewal: {
    kind: 'renewal',
    pipelineId: 10761890,
    pipelineName: 'Продления',
    sheetName: 'Продления',
    trackOUHistory: false,
  },
};

// Authorization code — used ONCE to get tokens, then cleared
const AUTH_CODE = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjJlZjVmYWNlMWM4Zjg4YTdjMDdhOTNkNjQ0ZmY2ZmE1NzY0NTA1NmQxNTJiODFhNTc3OWIwZGI0ZDhmNzMwYjliY2JmYjI0YWVjZGUxOWMyIn0.eyJhdWQiOiI5YzczZWMzMy1mZWRjLTRhMjQtYjg5NS0xOWQwYzBiMDFjMjYiLCJqdGkiOiIyZWY1ZmFjZTFjOGY4OGE3YzA3YTkzZDY0NGZmNmZhNTc2NDUwNTZkMTUyYjgxYTU3NzliMGRiNGQ4ZjczMGI5YmNiZmIyNGFlY2RlMTljMiIsImlhdCI6MTc3NDIwNTQyOCwibmJmIjoxNzc0MjA1NDI4LCJleHAiOjE5MzAwODk2MDAsInN1YiI6IjU3Mzg0MzEiLCJncmFudF90eXBlIjoiIiwiYWNjb3VudF9pZCI6Mjg5OTE0NjQsImJhc2VfZG9tYWluIjoiYW1vY3JtLnJ1IiwidmVyc2lvbiI6Miwic2NvcGVzIjpbInB1c2hfbm90aWZpY2F0aW9ucyIsImZpbGVzIiwiY3JtIiwiZmlsZXNfZGVsZXRlIiwibm90aWZpY2F0aW9ucyJdLCJoYXNoX3V1aWQiOiIwNmRjYzdlZi1lNGQzLTQ0OWYtOWQ2MC0zNGFhNzhkNzA3MzUiLCJhcGlfZG9tYWluIjoiYXBpLWIuYW1vY3JtLnJ1In0.q9jjUlxTb6wU9hM9ebaWU6oFHdo74OBgT4Qqaqr2-UNb7wwg3s8SC4d90thKM28pVSW98jMUKV572Nlb4YGPVBeHLwFQ6_gqPuUzQbCuyyJmrbhzlNphLU7Cs76RnSOjWoCu3FkR9259H1LMn0jajW-K6lvVl4weCOV44f39VZTTLdpLDa2jhYs6aVsf1eIZo7-Ac-DJZj-tpGTjxrJFJjcpeUO4DZqj-Xtwb847FUwh1CRYm13xB-sDXGE6n8P4SO7UbyiPH4FkJv7XK_Qwy0QTsiPohQrdv6KgMwYsuLe-sU1N1Q7mbbfyA6RR-lJjsxzHSQXE7uahFIQtjFjC4Q';

// ============================================================
// TOKEN MANAGEMENT
// ============================================================

function getProps() {
  return PropertiesService.getScriptProperties();
}

function exchangeAuthCode() {
  const url = `https://${CONFIG.AMO_DOMAIN}/oauth2/access_token`;
  const res = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({
      client_id: CONFIG.CLIENT_ID,
      client_secret: CONFIG.CLIENT_SECRET,
      grant_type: 'authorization_code',
      code: AUTH_CODE,
      redirect_uri: CONFIG.REDIRECT_URI,
    }),
    muteHttpExceptions: true,
  });

  if (res.getResponseCode() !== 200) {
    throw new Error('Auth code exchange failed: ' + res.getContentText());
  }

  const data = JSON.parse(res.getContentText());
  const props = getProps();
  props.setProperty('amo_access_token', data.access_token);
  props.setProperty('amo_refresh_token', data.refresh_token);
  props.setProperty('amo_token_expires', String(Date.now() + data.expires_in * 1000));
  Logger.log('Tokens saved successfully!');
  return data.access_token;
}

function refreshTokens() {
  const props = getProps();
  const refreshToken = props.getProperty('amo_refresh_token');
  if (!refreshToken) throw new Error('No refresh token. Run exchangeAuthCode() first.');

  const url = `https://${CONFIG.AMO_DOMAIN}/oauth2/access_token`;
  const res = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({
      client_id: CONFIG.CLIENT_ID,
      client_secret: CONFIG.CLIENT_SECRET,
      grant_type: 'refresh_token',
      refresh_token: refreshToken,
      redirect_uri: CONFIG.REDIRECT_URI,
    }),
    muteHttpExceptions: true,
  });

  if (res.getResponseCode() !== 200) {
    throw new Error('Token refresh failed: ' + res.getContentText());
  }

  const data = JSON.parse(res.getContentText());
  props.setProperty('amo_access_token', data.access_token);
  props.setProperty('amo_refresh_token', data.refresh_token);
  props.setProperty('amo_token_expires', String(Date.now() + data.expires_in * 1000));
  return data.access_token;
}

function getAccessToken() {
  const props = getProps();
  const token = props.getProperty('amo_access_token');
  const expires = Number(props.getProperty('amo_token_expires') || 0);

  if (!token) return exchangeAuthCode();
  if (Date.now() > expires - 60000) return refreshTokens();
  return token;
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

  // Token expired mid-request — refresh and retry
  if (res.getResponseCode() === 401) {
    const newToken = refreshTokens();
    const res2 = UrlFetchApp.fetch(url, {
      method: options?.method || 'get',
      headers: { 'Authorization': 'Bearer ' + newToken },
      contentType: 'application/json',
      muteHttpExceptions: true,
      ...(options?.payload ? { payload: JSON.stringify(options.payload) } : {}),
    });
    return JSON.parse(res2.getContentText());
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

function fetchAllDeals(pipelineId) {
  // order[id]=asc is required for stable pagination. Without it AmoCRM sorts by
  // updated_at desc by default, and any deal updated during the sync migrates
  // between pages — causing deals to be skipped or duplicated across pages.
  const deals = [];
  const seen = {};
  let page = 1;
  while (true) {
    const url = `/api/v4/leads?filter[pipeline_id]=${pipelineId}&with=contacts&order[id]=asc&limit=250&page=${page}`;
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
  Logger.log(`Fetched ${deals.length} deals`);
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

function getCustomFieldValue(deal, fieldName) {
  if (!deal.custom_fields_values) return '';
  var field = deal.custom_fields_values.find(function(f) { return f.field_name === fieldName; });
  if (!field || !field.values || !field.values[0]) return '';
  // Multi-select: join all values with comma
  if (field.values.length > 1) {
    return field.values.map(function(v) { return v.value; }).join(', ');
  }
  return field.values[0].value;
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

// ============================================================
// MAIN SYNC FUNCTION
// ============================================================

function syncPipeline(cfg) {
  const startTime = Date.now();
  Logger.log(`Starting sync: ${cfg.pipelineName}...`);

  const layout = LAYOUTS[cfg.kind];
  if (!layout) throw new Error(`Unknown pipeline kind: ${cfg.kind}`);

  // 1. Fetch all needed data
  const statuses = getPipelineStatuses(cfg.pipelineId);
  const users = getUsers();
  const deals = fetchAllDeals(cfg.pipelineId);

  // 2. Collect contact IDs
  const contactIdSet = new Set();
  deals.forEach(d => {
    if (d._embedded && d._embedded.contacts) {
      d._embedded.contacts.forEach(c => contactIdSet.add(c.id));
    }
  });
  const contacts = fetchContacts([...contactIdSet]);

  // 3. Build rows
  const ctx = {
    statuses,
    users,
    contacts,
    pipelineName: cfg.pipelineName,
    syncTime: Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'dd.MM.yyyy HH:mm'),
  };
  const rows = deals.map(deal => layout.buildRow(deal, ctx));

  // 4. Write to sheet
  const ss = SpreadsheetApp.openById(CONFIG.SHEET_ID);
  const sheet = cfg.sheetName ? ss.getSheetByName(cfg.sheetName) : ss.getSheets()[0];
  if (!sheet) throw new Error(`Sheet not found: ${cfg.sheetName}`);

  ensureHeaders(sheet, layout.headers);

  // Keep header row, clear data
  const ncols = layout.headers.length;
  const lastRow = sheet.getLastRow();
  if (lastRow > 1) {
    sheet.getRange(2, 1, lastRow - 1, ncols).clearContent();
  }

  // Write all rows at once
  if (rows.length > 0) {
    sheet.getRange(2, 1, rows.length, ncols).setValues(rows);
  }

  // 5. Update OU History (only for pipelines that track open lessons).
  // Indices below match DETSKAYA_HEADERS (col A = id, col R = "Дата ОУ").
  if (cfg.trackOUHistory) {
    const historySheet = getOrCreateHistorySheet();
    const ouHistory = loadOUHistory(historySheet);
    let historyUpdated = false;
    rows.forEach(function(row) {
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

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  Logger.log(`Sync complete: ${cfg.pipelineName} — ${rows.length} deals in ${elapsed}s`);
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
// SETUP: Run this ONCE to exchange auth code for tokens
// ============================================================

function setup() {
  try {
    exchangeAuthCode();
    Logger.log('✅ Tokens obtained successfully!');
    Logger.log('Now run syncAll() to test, then setupTrigger() to automate.');
  } catch (e) {
    Logger.log('❌ Error: ' + e.message);
    Logger.log('If auth code expired, create a new integration in AmoCRM.');
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
