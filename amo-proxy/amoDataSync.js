// =====================================================================
// AmoCRM → Supabase sync (замена Apps Script sync.gs для дашборда).
// Запускается кроном (см. server.js: setInterval → runDataSync).
//
// Логика повторяет sync.gs 1-в-1, чтобы схема данных и имена колонок
// совпадали со старой Google-таблицей. Это важно для backward-compat:
// дашборд читает ровно те же поля, что и раньше.
//
// Env vars (используем те же имена, что и остальной amo-proxy):
//   AMO_DOMAIN     = superkid.amocrm.ru
//   AMO_TOKEN      = долгосрочный Bearer-токен (можно также AMO_DATA_TOKEN
//                    / AMO_LONG_TOKEN — ищем первый непустой)
//   SUPABASE_URL / SUPABASE_SERVICE_KEY (см. supabase.js)
//   AMO_DEALS_PIPELINE_ID    = 5326345  (Детская прямая)
//   AMO_RENEWALS_PIPELINE_ID = 11203938 (Воронка продления)
// =====================================================================

import { upsertRows, getSyncState, setSyncState, sbFetch } from './supabase.js';

const DEFAULTS = {
    domain: 'superkid.amocrm.ru',
    detskayaPipelineId: 5326345,
    renewalPipelineId: 11203938,
};

function getConfig() {
    return {
        domain: process.env.AMO_DOMAIN || DEFAULTS.domain,
        token: process.env.AMO_TOKEN || process.env.AMO_DATA_TOKEN || process.env.AMO_LONG_TOKEN || '',
        detskayaId: parseInt(process.env.AMO_DEALS_PIPELINE_ID || DEFAULTS.detskayaPipelineId, 10),
        renewalId: parseInt(process.env.AMO_RENEWALS_PIPELINE_ID || DEFAULTS.renewalPipelineId, 10),
    };
}

// Чистый fetch с ретраем 429/5xx (экспоненциальный бэкофф).
async function amoGet(path) {
    const { domain, token } = getConfig();
    if (!token) throw new Error('AMO_TOKEN не задан');
    const url = `https://${domain}${path}`;
    const maxAttempts = 4;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        const res = await fetch(url, {
            method: 'GET',
            headers: {
                Authorization: 'Bearer ' + token,
                'Content-Type': 'application/json',
            },
        });
        if (res.status === 401) throw new Error('AmoCRM 401: токен невалиден или отозван.');
        if (res.status === 204) return null;
        if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
            if (attempt < maxAttempts) {
                await sleep(1000 * Math.pow(2, attempt - 1));
                continue;
            }
            throw new Error(`AmoCRM ${res.status} после ${maxAttempts} попыток: ${(await res.text()).slice(0, 300)}`);
        }
        if (!res.ok) {
            throw new Error(`AmoCRM ${res.status}: ${(await res.text()).slice(0, 300)}`);
        }
        return res.json();
    }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// =====================================================================
// AmoCRM: получение сделок / контактов / воронки / пользователей
// =====================================================================

async function fetchAllDeals(pipelineId, sinceTs) {
    const out = [];
    const seen = new Set();
    let page = 1;
    while (true) {
        let path = `/api/v4/leads?filter[pipeline_id]=${pipelineId}` +
            `&with=contacts&order[id]=asc&limit=250&page=${page}`;
        if (sinceTs) path += `&filter[updated_at][from]=${sinceTs}`;
        const data = await amoGet(path);
        const leads = data && data._embedded && data._embedded.leads || [];
        if (!leads.length) break;
        for (const d of leads) {
            if (!seen.has(d.id)) { seen.add(d.id); out.push(d); }
        }
        if (leads.length < 250) break;
        page++;
        await sleep(300); // Amo rate limit ≤ 7/s
    }
    return out;
}

async function fetchContacts(ids) {
    if (!ids.length) return {};
    const map = {};
    const BATCH = 50;
    for (let i = 0; i < ids.length; i += BATCH) {
        const batch = ids.slice(i, i + BATCH);
        const filter = batch.map(id => `filter[id][]=${id}`).join('&');
        const data = await amoGet(`/api/v4/contacts?${filter}&limit=250`);
        const list = data && data._embedded && data._embedded.contacts || [];
        for (const c of list) {
            let phone = '', parentUser = '';
            if (c.custom_fields_values) {
                const phoneField = c.custom_fields_values.find(f => f.field_code === 'PHONE');
                if (phoneField && phoneField.values && phoneField.values[0]) {
                    phone = String(phoneField.values[0].value || '');
                }
                const userField = c.custom_fields_values.find(f => f.field_name === 'Юзер родителя');
                if (userField && userField.values && userField.values[0]) {
                    parentUser = String(userField.values[0].value || '');
                }
            }
            map[c.id] = { name: c.name, phone, parentUser };
        }
        await sleep(200);
    }
    return map;
}

async function fetchPipelineStatuses(pipelineId) {
    const data = await amoGet(`/api/v4/leads/pipelines/${pipelineId}`);
    const statuses = {};
    const list = data && data._embedded && data._embedded.statuses || [];
    for (const s of list) statuses[s.id] = s.name;
    return statuses;
}

async function fetchUsers() {
    const users = {};
    let page = 1;
    while (true) {
        const data = await amoGet(`/api/v4/users?page=${page}&limit=250`);
        const list = data && data._embedded && data._embedded.users || [];
        if (!list.length) break;
        for (const u of list) users[u.id] = u.name;
        if (list.length < 250) break;
        page++;
    }
    return users;
}

// =====================================================================
// Хелперы для парсинга полей
// =====================================================================

function cf(deal, name) {
    if (!deal.custom_fields_values) return '';
    const field = deal.custom_fields_values.find(f => f.field_name === name);
    if (!field || !field.values || !field.values[0]) return '';
    if (field.values.length > 1) {
        return field.values.map(v => v.value).join(', ');
    }
    return String(field.values[0].value == null ? '' : field.values[0].value);
}

function fmtDate(ts) {
    if (!ts) return '';
    const n = Number(ts);
    if (!Number.isFinite(n)) return String(ts);
    const d = new Date(n * 1000);
    return pad(d.getDate()) + '.' + pad(d.getMonth() + 1) + '.' + d.getFullYear();
}

function fmtDateTime(ts) {
    if (!ts) return '';
    const n = Number(ts);
    if (!Number.isFinite(n)) return String(ts);
    const d = new Date(n * 1000);
    return pad(d.getDate()) + '.' + pad(d.getMonth() + 1) + '.' + d.getFullYear() +
        ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes());
}

function pad(n) { return String(n).padStart(2, '0'); }

function contact(deal, contactsMap) {
    const cid = deal._embedded && deal._embedded.contacts && deal._embedded.contacts[0];
    if (!cid || !contactsMap[cid.id]) return { name: '', phone: '', parentUser: '' };
    return contactsMap[cid.id];
}

// =====================================================================
// Маппинг сделок → ряды таблиц Supabase.
// Поля ровно повторяют колонки Google Sheets, чтобы дашборд не трогать.
// =====================================================================

function buildDealsRow(deal, statusMap, userMap, contactsMap, pipelineName, domain) {
    const c = contact(deal, contactsMap);
    const status = (statusMap[deal.status_id] || '');
    return {
        id: deal.id,
        synced_at: new Date().toISOString(),
        link: `https://${domain}/leads/detail/${deal.id}`,
        manager: userMap[deal.responsible_user_id] || '',
        contact_name: c.name || '',
        phone: c.phone || '',
        child_name: cf(deal, 'Имя ребенка'),
        child_age: cf(deal, 'Возраст ребенка'),
        pains: cf(deal, 'Боли'),
        date_created: deal.created_at ? fmtDateTime(deal.created_at) : '',
        date_vr: fmtDate(cf(deal, 'Дата ВР')),
        date_qual: fmtDate(cf(deal, 'Дата Квала')),
        date_scheduled_ou: fmtDate(cf(deal, 'Дата назначения ОУ')),
        date_attended_ou: fmtDate(cf(deal, 'Дата проведения ОУ')),
        date_invoice: fmtDate(cf(deal, 'Дата Выставления счета')),
        date_prepay: fmtDate(cf(deal, 'Дата предоплаты')),
        closed_at: deal.closed_at ? fmtDate(deal.closed_at) : '',
        // В новом аккаунте superkid.amocrm.ru поле переименовано: было «Дата ОУ»,
        // стало «Дата и время ОУ». Пробуем новое имя, фолбек на старое — чтобы
        // код продолжал работать, если имя снова поменяют или мы вернём старое.
        date_ou: fmtDateTime(cf(deal, 'Дата и время ОУ') || cf(deal, 'Дата ОУ')),
        confirmed_ou: cf(deal, 'Подтвердил ОУ'),
        budget: Number(deal.price) || 0,
        prepay_amount: Number(cf(deal, 'Сумма предоплаты')) || 0,
        days_avail: cf(deal, 'Дни когда может'),
        time_avail: cf(deal, 'Время когда может'),
        stream_num: cf(deal, 'Номер потока'),
        language: cf(deal, 'Язык обучения'),
        product: cf(deal, 'Продукт'),
        loss_reason: deal.loss_reason && deal.loss_reason[0] && deal.loss_reason[0].name || '',
        status: pipelineName + ' / ' + status,
        utm_source: cf(deal, 'utm_source'),
        utm_campaign: cf(deal, 'utm_campaign'),
        utm_medium: cf(deal, 'utm_medium'),
        utm_term: cf(deal, 'utm_term'),
        utm_content: cf(deal, 'utm_content'),
        tags: (deal._embedded && deal._embedded.tags || []).map(t => t.name).join(', '),
        parent_user: c.parentUser || '',
        was_on_ou: cf(deal, 'Был на ОУ'),
    };
}

function buildRenewalRow(deal, statusMap, userMap, contactsMap, pipelineName, domain) {
    const c = contact(deal, contactsMap);
    const status = (statusMap[deal.status_id] || '');
    return {
        id: deal.id,
        synced_at: new Date().toISOString(),
        link: `https://${domain}/leads/detail/${deal.id}`,
        manager: userMap[deal.responsible_user_id] || '',
        contact_name: c.name || '',
        phone: c.phone || '',
        child_name: cf(deal, 'Имя ребенка'),
        child_age: cf(deal, 'Возраст ребенка'),
        date_created: deal.created_at ? fmtDateTime(deal.created_at) : '',
        closed_at: deal.closed_at ? fmtDate(deal.closed_at) : '',
        budget: Number(deal.price) || 0,
        prepay_amount: Number(cf(deal, 'Сумма предоплаты')) || 0,
        date_prepay: fmtDate(cf(deal, 'Дата предоплаты')),
        date_prepay_renewal: fmtDate(cf(deal, 'Дата предоплаты продления')),
        stream_num: cf(deal, 'Номер потока'),
        language: cf(deal, 'Язык обучения'),
        product: cf(deal, 'Продукт'),
        loss_reason: deal.loss_reason && deal.loss_reason[0] && deal.loss_reason[0].name || '',
        status: pipelineName + ' / ' + status,
        tags: (deal._embedded && deal._embedded.tags || []).map(t => t.name).join(', '),
        module_num: cf(deal, 'Номер модуля'),
    };
}

// =====================================================================
// Основная работа — syncOne для одной воронки, runDataSync для обеих.
// =====================================================================

async function syncOne(kind) {
    const cfg = getConfig();
    const t0 = Date.now();
    const spec = kind === 'deals'
        ? { pipelineId: cfg.detskayaId, pipelineName: 'Детская прямая', table: 'deals',
            builder: buildDealsRow, trackOuHistory: true }
        : { pipelineId: cfg.renewalId, pipelineName: 'Продления', table: 'renewals',
            builder: buildRenewalRow, trackOuHistory: false };

    const lastSyncKey = `last_sync_ts_${kind}`;
    const lastFullKey = `last_full_sync_ts_${kind}`;
    const now = Math.floor(Date.now() / 1000);
    const lastSync = parseInt((await getSyncState(lastSyncKey)) || '0', 10);
    const lastFull = parseInt((await getSyncState(lastFullKey)) || '0', 10);

    // Full раз в 12 часов, иначе инкрементальный с overlap 10 минут.
    const FULL_INTERVAL = 12 * 3600;
    const doFull = !lastSync || (now - lastFull > FULL_INTERVAL);
    const sinceTs = doFull ? null : Math.max(0, lastSync - 600);

    // Full-отметку ставим ДО работы: если упадём в середине, следующий
    // прогон пойдёт инкрементально вместо повторного дорогого full.
    if (doFull) await setSyncState(lastFullKey, String(now));

    console.log(`[sync ${kind}] ${doFull ? 'FULL' : 'INCR'} ${sinceTs ? '(since ' + sinceTs + ')' : ''}`);

    const deals = await fetchAllDeals(spec.pipelineId, sinceTs);
    console.log(`[sync ${kind}] fetched ${deals.length} deals`);

    if (!doFull && deals.length === 0) {
        await setSyncState(lastSyncKey, String(now));
        console.log(`[sync ${kind}] no updates in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
        return { kind, deals: 0, took: Date.now() - t0 };
    }

    const [statusMap, userMap] = await Promise.all([
        fetchPipelineStatuses(spec.pipelineId),
        fetchUsers(),
    ]);

    const contactIds = [...new Set(
        deals.flatMap(d => (d._embedded && d._embedded.contacts || []).map(c => c.id))
    )];
    const contactsMap = await fetchContacts(contactIds);

    const rows = deals.map(d => spec.builder(d, statusMap, userMap, contactsMap, spec.pipelineName, cfg.domain));
    await upsertRows(spec.table, rows);

    if (spec.trackOuHistory) {
        const historyRows = rows
            .filter(r => r.date_ou && r.date_ou.length)
            .map(r => ({
                deal_id: r.id,
                first_ou_date: r.date_ou,
                updated_at: new Date().toISOString(),
            }));
        if (historyRows.length) await ensureHistoryInsert(historyRows);
    }

    await setSyncState(lastSyncKey, String(now));
    const took = Date.now() - t0;
    console.log(`[sync ${kind}] done ${rows.length} rows in ${(took / 1000).toFixed(1)}s`);
    return { kind, deals: rows.length, took };
}

// OU History: добавляем только НОВЫЕ (нет такого deal_id в таблице).
// Избегаем перезаписи «первой» даты — она должна оставаться первой.
//
// deal_id=in.(...) мы шлём через query string; при FULL sync сделок с
// date_ou может быть 1500+, URL легко переваливает за 8-16 KB лимит и
// PostgREST молча отвечает 414. Поэтому и запрос существующих, и upsert
// режем на чанки.
async function ensureHistoryInsert(rows) {
    const CHUNK = 200;
    const existingIds = new Set();
    for (let i = 0; i < rows.length; i += CHUNK) {
        const idsChunk = rows.slice(i, i + CHUNK).map(r => r.deal_id);
        try {
            const existing = await sbFetch(
                `/ou_history?deal_id=in.(${idsChunk.join(',')})&select=deal_id`
            );
            (existing || []).forEach(r => existingIds.add(r.deal_id));
        } catch (e) {
            console.warn('[ou_history] существующие id не получены (чанк):', e.message);
        }
    }
    const fresh = rows.filter(r => !existingIds.has(r.deal_id));
    if (!fresh.length) {
        console.log(`[ou_history] все ${rows.length} дат уже есть, добавлять нечего`);
        return;
    }
    const inserted = await upsertRows('ou_history', fresh, { onConflict: 'deal_id' });
    console.log(`[ou_history] добавлено ${inserted} новых записей (из ${rows.length} с date_ou)`);
}

export async function runDataSync() {
    const results = { deals: null, renewals: null, startedAt: new Date().toISOString() };
    try {
        results.deals = await syncOne('deals');
    } catch (e) {
        results.deals = { error: e.message };
        console.error('[sync deals] FAILED:', e.message);
    }
    try {
        await sleep(1000);
        results.renewals = await syncOne('renewals');
    } catch (e) {
        results.renewals = { error: e.message };
        console.error('[sync renewals] FAILED:', e.message);
    }
    return results;
}

/** Разовая принудительная перезагрузка: сбрасывает маркеры → следующий прогон FULL. */
export async function forceFullNext() {
    await setSyncState('last_sync_ts_deals', '0');
    await setSyncState('last_full_sync_ts_deals', '0');
    await setSyncState('last_sync_ts_renewals', '0');
    await setSyncState('last_full_sync_ts_renewals', '0');
}
