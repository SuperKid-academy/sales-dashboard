// Автопродления: ученик, у которого в Activation осталось ≤ RENEWAL_LESSONS_THRESHOLD
// уроков, попадает в «Воронку продления» на этап «ОСТАЛОСЬ МЕНЬШЕ 3 уроков».
// Сделка из «АКТИВНАЯ БАЗА» передвигается, иначе создаётся новая.
// ID этапов, полей и менеджера ищутся по названиям — в новом аккаунте они свои.

import crypto from 'node:crypto';

const env = (name, def = '') => String(process.env[name] ?? '').trim() || def;

const MODES = new Set(['off', 'manual', 'auto']);
const CFG = {
  mode: MODES.has(env('RENEWAL_MODE').toLowerCase()) ? env('RENEWAL_MODE').toLowerCase() : 'off',
  apiKey: env('RENEWAL_API_KEY'),
  pipelineId: Number(env('RENEWAL_PIPELINE_ID', '11203938')),
  targetStage: env('RENEWAL_STAGE_TARGET', 'ОСТАЛОСЬ МЕНЬШЕ 3 уроков'),
  parkingStage: env('RENEWAL_STAGE_PARKING', 'АКТИВНАЯ БАЗА'),
  responsibleId: Number(env('RENEWAL_RESPONSIBLE_USER_ID', '0')) || null,
  responsibleName: env('RENEWAL_RESPONSIBLE_NAME', 'Бекзод'),
  threshold: Number(env('RENEWAL_LESSONS_THRESHOLD', '2')),
  intervalMin: Math.max(10, Number(env('RENEWAL_CRON_INTERVAL_MIN', '60')) || 60),
  recentPaymentDays: Number(env('RENEWAL_RECENT_PAYMENT_DAYS', '7')),
  actApi: env('ACT_API_URL', 'https://activation.superkid.uz/api').replace(/\/$/, ''),
  actUser: env('ACT_USERNAME') || env('CRM_USERNAME'),
  actPass: env('ACT_PASSWORD') || env('CRM_PASSWORD'),
};

const FIELD = {
  child: 'Имя ребенка',
  module: 'Номер модуля',
  group: 'Номер группы',
  origin: 'id исходной сделки',
};
const COPY_FIELDS = [
  'Имя ребенка', 'Возраст ребенка', 'Класс', 'Боли', 'Язык обучения', 'Продукт',
  'Дни обучения', 'Дни когда может', 'Время когда может', 'Номер потока',
];
// Совпадает с rIsExcludedStatus в дашборде.
const EXCLUDED_STATUSES = new Set(['terminated', 'отчислен', 'refused', 'frozen', 'transferred']);
const WON = 142;
const LOST = 143;
const MODULE_SIZE = 8;
const LESSONS_PER_PREPAY = 2;

const sleep = ms => new Promise(r => setTimeout(r, ms));
const norm = s => String(s ?? '').trim().toLowerCase().replace(/ё/g, 'е').replace(/\s+/g, ' ');
const normPhone = p => {
  const d = String(p ?? '').replace(/\D/g, '');
  return d.length >= 9 ? d.slice(-9) : '';
};

function levenshtein(a, b) {
  if (a === b) return 0;
  let prev = Array.from({ length: b.length + 1 }, (_, j) => j);
  for (let i = 0; i < a.length; i++) {
    const cur = [i + 1];
    for (let j = 0; j < b.length; j++) {
      cur[j + 1] = Math.min(cur[j] + 1, prev[j + 1] + 1, prev[j] + (a[i] === b[j] ? 0 : 1));
    }
    prev = cur;
  }
  return prev[b.length];
}

// Совпадает с rNamesSimilar в дашборде: терпит опечатки и уменьшительные формы.
function namesSimilar(a, b) {
  if (a === b) return true;
  if (!a || !b) return false;
  const [shorter, longer] = a.length <= b.length ? [a, b] : [b, a];
  if (shorter.length >= 3 && longer.includes(shorter)) return true;
  if (longer.length < 4) return false;
  return levenshtein(a, b) / longer.length <= 0.3;
}

// ============================================================
// Activation CRM
// ============================================================

let actToken = '';

async function actLogin() {
  if (!CFG.actUser || !CFG.actPass) throw new Error('Не заданы ACT_USERNAME / ACT_PASSWORD');
  const resp = await fetch(`${CFG.actApi}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: CFG.actUser, password: CFG.actPass }),
  });
  if (!resp.ok) throw new Error(`Activation: вход не удался (${resp.status})`);
  const data = await resp.json();
  actToken = data.access_token || data.token || '';
  if (!actToken) throw new Error('Activation: в ответе на вход нет токена');
}

async function actGet(path) {
  if (!actToken) await actLogin();
  let resp = await fetch(`${CFG.actApi}${path}`, { headers: { Authorization: `Bearer ${actToken}` } });
  if (resp.status === 401 || resp.status === 403) {
    await actLogin();
    resp = await fetch(`${CFG.actApi}${path}`, { headers: { Authorization: `Bearer ${actToken}` } });
  }
  if (!resp.ok) throw new Error(`Activation ${path} → ${resp.status}`);
  return resp.json();
}

// Повторяет rebuildCRMModuleMapping из дашборда. pending > 0 — внесена
// предоплата, но не доплата: ученик ещё не закрыл текущий модуль.
function moduleStateByDeal(payments) {
  const byDeal = new Map();
  for (const p of payments) {
    const id = String(p.deal_id);
    if (!byDeal.has(id)) byDeal.set(id, []);
    byDeal.get(id).push(p);
  }
  const out = new Map();
  for (const [dealId, list] of byDeal) {
    list.sort((a, b) => String(a.event_date || '').slice(0, 10).localeCompare(String(b.event_date || '').slice(0, 10))
      || (a.id || 0) - (b.id || 0));
    let cursor = 0;
    let maxPaid = 0;
    const pending = [];
    for (const p of list) {
      let to;
      if (p.module_from != null && p.module_to != null) {
        to = p.module_to;
        cursor = Math.max(cursor, to);
      } else if (p.type === 'prepayment') {
        to = cursor + 1 + pending.length;
        pending.push(to);
      } else if (p.type === 'payment' || p.type === 'full_payment' || p.type === 'top_up') {
        const lessons = parseInt(p.lessons_delta || 0, 10) || 0;
        let from;
        let modules;
        if (pending.length) {
          from = pending.shift();
          modules = Math.max(1, Math.ceil((LESSONS_PER_PREPAY + Math.max(0, lessons)) / MODULE_SIZE));
        } else {
          from = cursor + 1;
          modules = Math.max(1, Math.ceil(Math.max(MODULE_SIZE, lessons) / MODULE_SIZE));
        }
        to = from + modules - 1;
        cursor = Math.max(cursor, to);
      } else {
        continue;
      }
      maxPaid = Math.max(maxPaid, to);
    }
    out.set(dealId, { maxPaid, pending: pending.length });
  }
  return out;
}

async function loadCandidates() {
  const [balances, groups, paymentsResp] = await Promise.all([
    actGet('/balance/students?mode=active'),
    actGet('/managed-groups'),
    actGet('/balance/payments?types=prepayment,payment,full_payment,top_up&limit=5000'),
  ]);
  const payments = Array.isArray(paymentsResp) ? paymentsResp : (paymentsResp?.rows || []);
  const moduleState = moduleStateByDeal(payments);

  const enrollByDeal = new Map();
  for (let i = 0; i < groups.length; i += 5) {
    await Promise.all(groups.slice(i, i + 5).map(async g => {
      const students = await actGet(`/managed-groups/${g.id}/students`);
      for (const s of students || []) {
        const id = String(s.deal_id || '');
        if (!id) continue;
        if (!enrollByDeal.has(id)) enrollByDeal.set(id, []);
        enrollByDeal.get(id).push({ ...s, group: g });
      }
    }));
  }

  const today = Date.now();
  const seen = new Set();
  const candidates = [];
  for (const b of balances || []) {
    const dealId = String(b.deal_id || '');
    if (!dealId || seen.has(dealId)) continue;
    seen.add(dealId);
    const lessonsRemaining = parseInt(b.lessons_remaining ?? 0, 10) || 0;
    if (lessonsRemaining > CFG.threshold) continue;

    const enrolls = enrollByDeal.get(dealId) || [];
    const enroll = enrolls.find(e => !EXCLUDED_STATUSES.has(norm(e.student_status)));
    const state = moduleState.get(dealId) || { maxPaid: 0, pending: 0 };
    const c = {
      deal_id: dealId,
      child_name: b.student_name || enroll?.child_name || enroll?.student_name || '',
      parent_name: b.contact_name || enroll?.contact_name || '',
      phone: b.phone || enroll?.phone || '',
      lessons_remaining: lessonsRemaining,
      group_number: b.group_number || enroll?.group?.group_number || enroll?.group?.id || b.group_id || '',
      max_paid_module: state.maxPaid,
      next_module: Math.max(2, state.maxPaid + 1),
      skip: null,
    };
    const lastPay = Date.parse(b.last_payment_date || '');
    if (!enrolls.length) c.skip = 'not_in_group';
    else if (!enroll) c.skip = `excluded_status:${norm(enrolls[0].student_status)}`;
    else if (state.pending > 0) c.skip = 'awaiting_surcharge';
    else if (lastPay && today - lastPay < CFG.recentPaymentDays * 864e5) c.skip = 'recently_paid';
    candidates.push(c);
  }
  return candidates;
}

// ============================================================
// AmoCRM
// ============================================================

async function amoPaged(amo, path) {
  const out = [];
  for (let page = 1; page <= 200; page++) {
    const sep = path.includes('?') ? '&' : '?';
    const data = await amo(`${path}${sep}limit=250&page=${page}`);
    const key = data?._embedded && Object.keys(data._embedded)[0];
    const items = key ? data._embedded[key] : [];
    out.push(...items);
    if (items.length < 250) break;
    await sleep(150);
  }
  return out;
}

async function loadAmoMeta(amo) {
  const pipeline = await amo(`/api/v4/leads/pipelines/${CFG.pipelineId}`);
  const statuses = pipeline?._embedded?.statuses || [];
  const findStage = name => statuses.find(s => norm(s.name) === norm(name));
  const target = findStage(CFG.targetStage);
  const parking = findStage(CFG.parkingStage);
  if (!target) throw new Error(`В воронке ${CFG.pipelineId} нет этапа «${CFG.targetStage}»`);

  const fieldList = await amoPaged(amo, '/api/v4/leads/custom_fields');
  const fields = new Map(fieldList.map(f => [norm(f.name), { id: f.id, type: f.type, name: f.name }]));

  let responsible = CFG.responsibleId ? { id: CFG.responsibleId, name: `id ${CFG.responsibleId}` } : null;
  const warnings = [];
  if (!responsible) {
    const users = await amoPaged(amo, '/api/v4/users');
    const want = norm(CFG.responsibleName);
    const matches = users.filter(u => norm(u.name) === want);
    const loose = matches.length ? matches : users.filter(u => norm(u.name).startsWith(want));
    if (loose.length === 1) responsible = { id: loose[0].id, name: loose[0].name };
    else warnings.push(`Менеджер «${CFG.responsibleName}»: найдено ${loose.length} — задайте RENEWAL_RESPONSIBLE_USER_ID`);
  }
  for (const name of Object.values(FIELD)) {
    if (!fields.has(norm(name))) warnings.push(`Нет поля сделки «${name}» — оно не будет заполняться`);
  }
  if (!parking) warnings.push(`Нет этапа «${CFG.parkingStage}» — сделки будут только создаваться`);

  return {
    target: { id: target.id, name: target.name },
    parking: parking ? { id: parking.id, name: parking.name } : null,
    unsortedIds: new Set(statuses.filter(s => s.type === 1).map(s => s.id)),
    stageNames: new Map(statuses.map(s => [s.id, s.name])),
    fields,
    responsible,
    warnings,
  };
}

function cfValue(lead, fieldId) {
  const f = (lead.custom_fields_values || []).find(x => x.field_id === fieldId);
  return f?.values?.[0]?.value ?? null;
}

async function loadRenewalIndex(amo, meta) {
  const leads = await amoPaged(amo, `/api/v4/leads?filter[pipeline_id]=${CFG.pipelineId}&with=contacts`);
  const contactIds = [...new Set(leads.flatMap(l => (l._embedded?.contacts || []).map(c => c.id)))];
  const phonesByContact = new Map();
  for (let i = 0; i < contactIds.length; i += 50) {
    const filter = contactIds.slice(i, i + 50).map(id => `filter[id][]=${id}`).join('&');
    const data = await amo(`/api/v4/contacts?${filter}&limit=250`);
    for (const c of data?._embedded?.contacts || []) {
      const phoneField = (c.custom_fields_values || []).find(f => f.field_code === 'PHONE');
      phonesByContact.set(c.id, (phoneField?.values || []).map(v => normPhone(v.value)).filter(Boolean));
    }
    await sleep(150);
  }

  const childField = meta.fields.get(norm(FIELD.child));
  const moduleField = meta.fields.get(norm(FIELD.module));
  const byPhone = new Map();
  for (const l of leads) {
    const item = {
      id: l.id,
      status_id: l.status_id,
      created_at: l.created_at,
      childKey: childField ? norm(cfValue(l, childField.id)) : '',
      moduleNum: moduleField ? Number(cfValue(l, moduleField.id)) || null : null,
    };
    const phones = new Set((l._embedded?.contacts || []).flatMap(c => phonesByContact.get(c.id) || []));
    for (const p of phones) {
      if (!byPhone.has(p)) byPhone.set(p, []);
      byPhone.get(p).push(item);
    }
  }
  return { byPhone, total: leads.length };
}

// Сделки этого ребёнка: тот же телефон родителя, а имя ребёнка не противоречит
// (у братьев и сестёр телефон родителя общий).
function leadsForCandidate(c, index) {
  const pool = index.byPhone.get(normPhone(c.phone)) || [];
  const ck = norm(c.child_name);
  return pool.filter(l => !l.childKey || !ck || namesSimilar(l.childKey, ck));
}

function decide(c, index, meta, usedLeadIds) {
  if (c.skip) return { action: 'skip', reason: c.skip };
  if (!normPhone(c.phone)) return { action: 'skip', reason: 'no_phone' };
  const leads = leadsForCandidate(c, index);
  const forModule = leads.find(l => l.moduleNum === c.next_module);
  if (forModule) {
    return { action: 'skip', reason: 'already_has_lead_for_module', lead_id: forModule.id,
      stage: meta.stageNames.get(forModule.status_id) };
  }
  const open = leads.filter(l => l.status_id !== WON && l.status_id !== LOST && !usedLeadIds.has(l.id));
  const inWork = open.find(l => l.status_id !== meta.parking?.id);
  if (inWork) {
    return { action: 'skip', reason: meta.unsortedIds.has(inWork.status_id) ? 'unsorted_lead' : 'in_work',
      lead_id: inWork.id, stage: meta.stageNames.get(inWork.status_id) };
  }
  const parked = open.filter(l => l.status_id === meta.parking?.id).sort((a, b) => b.created_at - a.created_at);
  if (parked.length) return { action: 'move', lead_id: parked[0].id, parked_count: parked.length };
  return { action: 'create' };
}

function fieldValue(field, raw) {
  if (raw == null || raw === '') return null;
  if (field.type === 'numeric') {
    const n = Number(String(raw).replace(/\s/g, ''));
    return Number.isFinite(n) ? n : null;
  }
  return String(raw);
}

function renewalFields(c, meta) {
  const out = [];
  const add = (name, raw) => {
    const field = meta.fields.get(norm(name));
    const value = field && fieldValue(field, raw);
    if (value != null) out.push({ field_id: field.id, values: [{ value }] });
  };
  add(FIELD.module, c.next_module);
  add(FIELD.group, c.group_number);
  add(FIELD.origin, c.deal_id);
  return out;
}

function leadName(c) {
  const parts = [String(c.child_name || '').trim() || 'без имени', `модуль ${c.next_module}`];
  if (c.group_number) parts.push(`группа ${c.group_number}`);
  return parts.join(', ');
}

async function moveLead(amo, c, decision, meta) {
  const body = {
    pipeline_id: CFG.pipelineId,
    status_id: meta.target.id,
    name: leadName(c),
    custom_fields_values: renewalFields(c, meta),
  };
  if (meta.responsible) body.responsible_user_id = meta.responsible.id;
  await amo(`/api/v4/leads/${decision.lead_id}`, { method: 'PATCH', body });
  return decision.lead_id;
}

async function createLead(amo, c, meta) {
  const original = await amo(`/api/v4/leads/${c.deal_id}?with=contacts`);
  const contactId = original?._embedded?.contacts?.[0]?.id;
  if (!contactId) throw new Error('у исходной сделки нет контакта');

  const copyKeys = new Set(COPY_FIELDS.map(norm));
  const fields = (original.custom_fields_values || [])
    .filter(f => copyKeys.has(norm(f.field_name)) && f.values?.length)
    .map(f => ({
      field_id: f.field_id,
      values: f.values.map(v => (v.enum_id != null ? { enum_id: v.enum_id } : { value: v.value })),
    }));
  const childField = meta.fields.get(norm(FIELD.child));
  if (childField && c.child_name && !fields.some(f => f.field_id === childField.id)) {
    fields.push({ field_id: childField.id, values: [{ value: c.child_name }] });
  }
  fields.push(...renewalFields(c, meta));

  const lead = {
    name: leadName(c),
    price: original.price || 0,
    pipeline_id: CFG.pipelineId,
    status_id: meta.target.id,
    custom_fields_values: fields,
    _embedded: { contacts: [{ id: contactId }] },
  };
  if (meta.responsible) lead.responsible_user_id = meta.responsible.id;
  const data = await amo('/api/v4/leads', { method: 'POST', body: [lead] });
  return data?._embedded?.leads?.[0]?.id || null;
}

// ============================================================
// Прогон
// ============================================================

let running = false;
let lastRun = null;

async function runRenewals(amo, { write }) {
  if (running) throw Object.assign(new Error('Прогон уже идёт'), { status: 409 });
  running = true;
  const started = new Date();
  try {
    const [candidates, meta] = await Promise.all([loadCandidates(), loadAmoMeta(amo)]);
    const index = await loadRenewalIndex(amo, meta);
    const usedLeadIds = new Set();
    const items = [];
    for (const c of candidates) {
      const d = decide(c, index, meta, usedLeadIds);
      if (d.action === 'move') usedLeadIds.add(d.lead_id);
      const item = {
        deal_id: c.deal_id, child_name: c.child_name, parent_name: c.parent_name, phone: c.phone,
        lessons_remaining: c.lessons_remaining, next_module: c.next_module, group_number: c.group_number,
        ...d,
      };
      if (write && (d.action === 'move' || d.action === 'create')) {
        try {
          item.lead_id = d.action === 'move' ? await moveLead(amo, c, d, meta) : await createLead(amo, c, meta);
          item.action = d.action === 'move' ? 'moved' : 'created';
          console.log(`[renewals] ${item.action} lead ${item.lead_id} ← deal ${c.deal_id} (модуль ${c.next_module})`);
        } catch (e) {
          item.action = 'failed';
          item.reason = e.message;
          console.error(`[renewals] failed deal ${c.deal_id}: ${e.message}`);
        }
        await sleep(200);
      }
      items.push(item);
    }

    const counts = {};
    for (const it of items) {
      const key = it.action === 'skip' ? `skip:${it.reason.split(':')[0]}` : it.action;
      counts[key] = (counts[key] || 0) + 1;
    }
    const result = {
      ok: true,
      write,
      mode: CFG.mode,
      started_at: started.toISOString(),
      took_sec: Math.round((Date.now() - started.getTime()) / 1000),
      config: {
        pipeline_id: CFG.pipelineId,
        target_stage: meta.target,
        parking_stage: meta.parking,
        responsible: meta.responsible,
        threshold: CFG.threshold,
        fields: Object.fromEntries(Object.values(FIELD).map(n => [n, meta.fields.get(norm(n))?.id || null])),
      },
      warnings: meta.warnings,
      renewal_pipeline_leads: index.total,
      candidates: items.length,
      counts,
      items,
    };
    lastRun = { at: result.started_at, write, took_sec: result.took_sec, counts, warnings: meta.warnings };
    console.log(`[renewals] ${write ? 'write' : 'dry-run'}: ${JSON.stringify(counts)}`);
    return result;
  } finally {
    running = false;
  }
}

function keyOk(req) {
  const given = String(req.get('X-Renewal-Key') || req.query.key || '');
  if (!CFG.apiKey || !given) return false;
  const a = Buffer.from(given);
  const b = Buffer.from(CFG.apiKey);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

export function renewalMode() {
  return CFG.mode;
}

export function registerRenewals(app, amo) {
  const guard = (req, res, next) => {
    if (!CFG.apiKey) return res.status(503).json({ ok: false, error: 'Не задан RENEWAL_API_KEY' });
    if (!keyOk(req)) return res.status(401).json({ ok: false, error: 'Неверный ключ' });
    next();
  };
  const handle = write => async (req, res) => {
    try {
      res.json(await runRenewals(amo, { write }));
    } catch (e) {
      res.status(e.status || 500).json({ ok: false, error: e.message });
    }
  };

  app.get('/api/renewal/status', guard, (req, res) => {
    res.json({ ok: true, mode: CFG.mode, interval_min: CFG.intervalMin, running, last_run: lastRun });
  });
  app.all('/api/renewal/scan', guard, handle(false));
  app.post('/api/renewal/push', guard, (req, res, next) => {
    if (CFG.mode === 'off') {
      return res.status(403).json({ ok: false, error: 'RENEWAL_MODE=off — запись выключена. Поставьте manual или auto.' });
    }
    next();
  }, handle(true));

  if (CFG.mode === 'auto') {
    const tick = () => runRenewals(amo, { write: true })
      .catch(e => console.error('[renewals] cron run failed:', e.message));
    setTimeout(tick, 60 * 1000);
    setInterval(tick, CFG.intervalMin * 60 * 1000);
    console.log(`[renewals] авто-режим: раз в ${CFG.intervalMin} мин`);
  } else {
    console.log(`[renewals] режим ${CFG.mode} — по расписанию не запускается`);
  }
}
