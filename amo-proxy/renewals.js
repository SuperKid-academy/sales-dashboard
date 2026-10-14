// Автопродления: держит сделку каждого ученика Activation в «Воронке продления»
// на этапе по регламенту — «АКТИВНАЯ БАЗА», «ОСТАЛОСЬ МЕНЬШЕ 3 уроков» (≤ порога
// уроков) или «Замарозка». «ПРОДЛИЛИ» и «ОТКАЗАЛИСЬ» ставит менеджер.
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
  frozenStage: env('RENEWAL_STAGE_FROZEN', 'Замарозка,Заморозка'),
  responsibleId: Number(env('RENEWAL_RESPONSIBLE_USER_ID', '0')) || null,
  responsibleName: env('RENEWAL_RESPONSIBLE_NAME', 'Бекзод,Bekzod'),
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

// Node пишет на сетевой сбой просто «fetch failed», настоящая причина — в cause.
function describe(e) {
  const c = e?.cause;
  return c ? `${e.message} (${[c.code, c.message].filter(Boolean).join(': ')})` : e.message;
}

const TRANSIENT_HTTP = /(AmoCRM|→) (429|5\d\d)\b/;

// Сетевые сбои повторяем только для идемпотентных запросов: POST мог дойти,
// а ответ потеряться — повтор создал бы дубль. 429 повторяем всегда.
async function withRetry(label, fn, { idempotent = true } = {}) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (e) {
      const network = e instanceof TypeError;
      const retry = (network && idempotent) || (TRANSIENT_HTTP.test(e.message) && (idempotent || /429/.test(e.message)));
      if (!retry || attempt >= 4) throw new Error(`${label}: ${describe(e)}`);
      await sleep(2000 * 2 ** (attempt - 1));
    }
  }
}

async function actGet(path) {
  return withRetry(`Activation ${path}`, async () => {
    if (!actToken) await actLogin();
    let resp = await fetch(`${CFG.actApi}${path}`, { headers: { Authorization: `Bearer ${actToken}` } });
    if (resp.status === 401 || resp.status === 403) {
      await actLogin();
      resp = await fetch(`${CFG.actApi}${path}`, { headers: { Authorization: `Bearer ${actToken}` } });
    }
    if (!resp.ok) throw new Error(`Activation → ${resp.status}`);
    return resp.json();
  });
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
  const [balances, frozen, groups, paymentsResp] = await Promise.all([
    actGet('/balance/students?mode=active'),
    actGet('/balance/students?mode=frozen'),
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

    const enrolls = enrollByDeal.get(dealId) || [];
    const enroll = enrolls.find(e => !EXCLUDED_STATUSES.has(norm(e.student_status)));
    const state = moduleState.get(dealId) || { maxPaid: 0, pending: 0 };
    const c = {
      deal_id: dealId,
      child_name: b.student_name || enroll?.child_name || enroll?.student_name || '',
      parent_name: b.contact_name || enroll?.contact_name || '',
      phone: b.phone || enroll?.phone || '',
      lessons_remaining: lessonsRemaining,
      burning: lessonsRemaining <= CFG.threshold,
      frozen: false,
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
  for (const b of frozen || []) {
    const dealId = String(b.deal_id || '');
    if (!dealId || seen.has(dealId)) continue;
    seen.add(dealId);
    const state = moduleState.get(dealId) || { maxPaid: 0, pending: 0 };
    candidates.push({
      deal_id: dealId,
      child_name: b.student_name || '',
      parent_name: b.contact_name || '',
      phone: b.phone || '',
      lessons_remaining: parseInt(b.lessons_remaining ?? 0, 10) || 0,
      burning: false,
      frozen: true,
      group_number: b.group_number || b.group_id || '',
      max_paid_module: state.maxPaid,
      next_module: Math.max(2, state.maxPaid + 1),
      skip: null,
    });
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
  const findStage = names => {
    for (const name of names.split(',').map(norm).filter(Boolean)) {
      const st = statuses.find(s => norm(s.name) === name);
      if (st) return st;
    }
    return null;
  };
  const target = findStage(CFG.targetStage);
  const parking = findStage(CFG.parkingStage);
  const frozen = findStage(CFG.frozenStage);
  if (!target) throw new Error(`В воронке ${CFG.pipelineId} нет этапа «${CFG.targetStage}»`);

  const fieldList = await amoPaged(amo, '/api/v4/leads/custom_fields');
  const fields = new Map(fieldList.map(f => [norm(f.name), { id: f.id, type: f.type, name: f.name }]));

  let responsible = CFG.responsibleId ? { id: CFG.responsibleId, name: `id ${CFG.responsibleId}` } : null;
  const warnings = [];
  if (!responsible) {
    const users = await amoPaged(amo, '/api/v4/users');
    const aliases = CFG.responsibleName.split(',').map(norm).filter(Boolean);
    const matches = users.filter(u => aliases.some(a => norm(u.name).includes(a)));
    if (matches.length === 1) responsible = { id: matches[0].id, name: matches[0].name };
    else {
      warnings.push(`Менеджер «${CFG.responsibleName}»: найдено ${matches.length}. ` +
        `В amo есть: ${users.map(u => `${u.name} (${u.id})`).join(', ')}. Задайте RENEWAL_RESPONSIBLE_USER_ID`);
    }
  }
  for (const name of Object.values(FIELD)) {
    const f = fields.get(norm(name));
    if (!f) warnings.push(`Нет поля сделки «${name}» — оно не будет заполняться`);
    else if (!WRITABLE_TYPES.has(f.type)) {
      warnings.push(`Поле «${name}» имеет тип «${f.type}», нужен «Число» или «Текст» — оно не будет заполняться`);
    }
  }
  if (!parking) warnings.push(`Нет этапа «${CFG.parkingStage}»`);
  if (!frozen) warnings.push(`Нет этапа «${CFG.frozenStage}» — замороженные не переносятся`);

  return {
    target: { id: target.id, name: target.name },
    parking: parking ? { id: parking.id, name: parking.name } : null,
    frozen: frozen ? { id: frozen.id, name: frozen.name } : null,
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
  const items = [];
  for (const l of leads) {
    const phones = [...new Set((l._embedded?.contacts || []).flatMap(c => phonesByContact.get(c.id) || []))];
    const item = {
      id: l.id,
      phones,
      status_id: l.status_id,
      created_at: l.created_at,
      closed_at: l.closed_at,
      tags: (l._embedded?.tags || []).map(t => ({ id: t.id, name: t.name })),
      childKey: childField ? norm(cfValue(l, childField.id)) : '',
      moduleNum: moduleField ? Number(cfValue(l, moduleField.id)) || null : null,
    };
    items.push(item);
    for (const p of phones) {
      if (!byPhone.has(p)) byPhone.set(p, []);
      byPhone.get(p).push(item);
    }
  }
  return { byPhone, items, total: leads.length };
}

// Сделки этого ребёнка: тот же телефон родителя, а имя ребёнка не противоречит
// (у братьев и сестёр телефон родителя общий).
function leadsForCandidate(c, index) {
  const pool = index.byPhone.get(normPhone(c.phone)) || [];
  const ck = norm(c.child_name);
  return pool.filter(l => !l.childKey || !ck || namesSimilar(l.childKey, ck));
}

// Автоматика двигает сделку только между своими этапами. Из «ОСТАЛОСЬ МЕНЬШЕ
// 3 уроков» назад в базу не возвращает — это происходит через оплату и
// «ПРОДЛИЛИ», которые ставит менеджер.
function canMove(fromId, toId, meta) {
  if (fromId === meta.parking?.id || fromId === meta.frozen?.id) return true;
  return fromId === meta.target.id && toId === meta.frozen?.id;
}

function decide(c, index, meta, usedLeadIds) {
  const skip = (reason, lead) => ({ action: 'skip', reason,
    ...(lead ? { lead_id: lead.id, stage: meta.stageNames.get(lead.status_id) } : {}) });
  if (c.skip) return skip(c.skip);
  if (!normPhone(c.phone)) return skip('no_phone');
  const desired = c.frozen ? meta.frozen : (c.burning ? meta.target : meta.parking);
  if (!desired) return skip('no_stage');

  const leads = leadsForCandidate(c, index);
  const autoStages = new Set([meta.parking?.id, meta.target.id, meta.frozen?.id].filter(Boolean));
  const open = leads.filter(l => l.status_id !== WON && l.status_id !== LOST && !usedLeadIds.has(l.id));
  const manual = open.find(l => !autoStages.has(l.status_id));
  if (manual) return skip(meta.unsortedIds.has(manual.status_id) ? 'unsorted_lead' : 'in_work', manual);
  const inPlace = open.find(l => l.status_id === desired.id);
  if (inPlace) return skip('in_place', inPlace);

  const movable = open.filter(l => canMove(l.status_id, desired.id, meta)).sort((a, b) => b.created_at - a.created_at);
  if (movable.length) {
    return { action: 'move', lead_id: movable[0].id, from: meta.stageNames.get(movable[0].status_id),
      to: desired.name, to_id: desired.id };
  }
  if (open.length) return skip('no_backward_move', open[0]);
  if (c.frozen) return skip('frozen_no_lead');

  const forModule = leads.find(l => l.moduleNum === c.next_module);
  if (forModule) return skip('already_has_lead_for_module', forModule);
  const recentWin = leads.find(l => l.status_id === WON && l.closed_at
    && Date.now() / 1000 - l.closed_at < CFG.recentPaymentDays * 86400);
  if (recentWin) return skip('recently_renewed', recentWin);
  return { action: 'create', to: desired.name, to_id: desired.id };
}

// Пишем только в числовые и текстовые поля: «id исходной сделки» в новом
// аккаунте заведено как дата, и amo отклонял из-за него всю правку сделки.
const WRITABLE_TYPES = new Set(['numeric', 'text', 'textarea']);

function fieldValue(field, raw) {
  if (raw == null || raw === '' || !WRITABLE_TYPES.has(field.type)) return null;
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
  const body = { pipeline_id: CFG.pipelineId, status_id: decision.to_id };
  if (!c.frozen) {
    body.name = leadName(c);
    body.custom_fields_values = renewalFields(c, meta);
    if (meta.responsible) body.responsible_user_id = meta.responsible.id;
  }
  await amo(`/api/v4/leads/${decision.lead_id}`, { method: 'PATCH', body });
  return decision.lead_id;
}

async function createLead(amo, c, decision, meta) {
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
    status_id: decision.to_id,
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
          item.lead_id = d.action === 'move' ? await moveLead(amo, c, d, meta) : await createLead(amo, c, d, meta);
          item.action = d.action === 'move' ? 'moved' : 'created';
          console.log(`[renewals] ${item.action} lead ${item.lead_id} → «${d.to}» ← deal ${c.deal_id} (модуль ${c.next_module})`);
        } catch (e) {
          item.action = 'failed';
          item.reason = e.message;
          console.error(`[renewals] failed deal ${c.deal_id}: ${e.message}`);
        }
        await sleep(350);
      }
      items.push(item);
    }

    const counts = {};
    for (const it of items) {
      const key = it.action === 'skip' ? `skip:${it.reason.split(':')[0]}`
        : it.to ? `${it.action} → ${it.to}` : it.action;
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
        frozen_stage: meta.frozen,
        responsible: meta.responsible,
        threshold: CFG.threshold,
        fields: Object.fromEntries(Object.values(FIELD).map(n => {
          const f = meta.fields.get(norm(n));
          return [n, f ? { id: f.id, type: f.type } : null];
        })),
      },
      warnings: meta.warnings,
      renewal_pipeline_leads: index.total,
      candidates: items.length,
      counts,
      items,
    };
    const failures = items.filter(i => i.action === 'failed').slice(0, 10).map(i => ({ deal_id: i.deal_id, reason: i.reason }));
    lastRun = { at: result.started_at, write, took_sec: result.took_sec, counts, warnings: meta.warnings, failures };
    console.log(`[renewals] ${write ? 'write' : 'dry-run'}: ${JSON.stringify(counts)}`);
    return result;
  } finally {
    running = false;
  }
}

// «Ушедшие»: сделки «АКТИВНАЯ БАЗА», которым не соответствует ни один ученик
// Activation ни в одном режиме. Сделки без телефона не трогаем — их не сверить.
const ORPHAN_TAG = 'на удаление';

async function runOrphans(amo, { write }) {
  if (running) throw Object.assign(new Error('Прогон уже идёт'), { status: 409 });
  running = true;
  try {
    const meta = await loadAmoMeta(amo);
    if (!meta.parking) throw new Error(`Нет этапа «${CFG.parkingStage}»`);
    const students = [];
    for (const mode of ['active', 'prepaid', 'frozen', 'refused']) {
      students.push(...(await actGet(`/balance/students?mode=${mode}`) || []));
    }
    const index = await loadRenewalIndex(amo, meta);
    const parked = index.items.filter(l => l.status_id === meta.parking.id);
    const matches = l => students.some(s => {
      const p = normPhone(s.phone);
      const ck = norm(s.student_name);
      return p && l.phones.includes(p) && (!l.childKey || !ck || namesSimilar(l.childKey, ck));
    });
    const orphans = parked.filter(l => l.phones.length && !matches(l));
    const noPhone = parked.filter(l => !l.phones.length).map(l => l.id);
    const hasTag = l => l.tags.some(t => norm(t.name) === norm(ORPHAN_TAG));
    const untagged = orphans.filter(l => !hasTag(l));

    // tags_to_add amo молча игнорировал, поэтому передаём полный список:
    // текущие теги сделки (по id) плюс новый — так ни один не потеряется.
    let tagged = 0;
    if (write) {
      for (let i = 0; i < untagged.length; i += 50) {
        const chunk = untagged.slice(i, i + 50).map(l => ({
          id: l.id,
          _embedded: { tags: [...l.tags.map(t => ({ id: t.id })), { name: ORPHAN_TAG }] },
        }));
        await amo('/api/v4/leads', { method: 'PATCH', body: chunk });
        tagged += chunk.length;
        await sleep(500);
      }
      console.log(`[renewals] тег «${ORPHAN_TAG}» поставлен на ${tagged} сделок`);
    }
    return {
      ok: true,
      write,
      tag: ORPHAN_TAG,
      parked: parked.length,
      orphans: orphans.length,
      already_tagged: orphans.length - untagged.length,
      tagged,
      no_phone_lead_ids: noPhone,
      items: orphans.map(l => ({ lead_id: l.id, child_key: l.childKey })),
    };
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

export function registerRenewals(app, rawAmo) {
  const amo = (path, opts = {}) => {
    const method = opts.method || 'GET';
    return withRetry(`AmoCRM ${method} ${path.split('?')[0]}`, () => rawAmo(path, opts), { idempotent: method !== 'POST' });
  };
  const guard = (req, res, next) => {
    if (!CFG.apiKey) return res.status(503).json({ ok: false, error: 'Не задан RENEWAL_API_KEY' });
    if (!keyOk(req)) return res.status(401).json({ ok: false, error: 'Неверный ключ' });
    next();
  };
  const writeGuard = (req, res, next) => {
    if (CFG.mode === 'off') {
      return res.status(403).json({ ok: false, error: 'RENEWAL_MODE=off — запись выключена. Поставьте manual или auto.' });
    }
    next();
  };
  const handle = run => async (req, res) => {
    try {
      res.json(await run());
    } catch (e) {
      if (e.status !== 409) lastRun = { at: new Date().toISOString(), error: e.message };
      res.status(e.status || 500).json({ ok: false, error: e.message });
    }
  };

  app.get('/api/renewal/status', guard, (req, res) => {
    res.json({ ok: true, mode: CFG.mode, interval_min: CFG.intervalMin, running, last_run: lastRun });
  });
  app.all('/api/renewal/scan', guard, handle(() => runRenewals(amo, { write: false })));
  app.post('/api/renewal/push', guard, writeGuard, handle(() => runRenewals(amo, { write: true })));
  app.get('/api/renewal/orphans', guard, handle(() => runOrphans(amo, { write: false })));
  app.post('/api/renewal/orphans', guard, writeGuard, handle(() => runOrphans(amo, { write: true })));

  if (CFG.mode === 'auto') {
    const tick = () => runRenewals(amo, { write: true }).catch(e => {
      if (e.status !== 409) lastRun = { at: new Date().toISOString(), error: e.message };
      console.error('[renewals] cron run failed:', e.message);
    });
    setTimeout(tick, 60 * 1000);
    setInterval(tick, CFG.intervalMin * 60 * 1000);
    console.log(`[renewals] авто-режим: раз в ${CFG.intervalMin} мин`);
  } else {
    console.log(`[renewals] режим ${CFG.mode} — по расписанию не запускается`);
  }
}
