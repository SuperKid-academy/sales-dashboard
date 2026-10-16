// =====================================================================
// База к продлению из Activation → Supabase (sync_state «renewal_base»).
//
// Считает то же, что вкладка «Продления 2.0» в дашборде, но на сервере —
// с логином куратора из env (ACT_USERNAME / ACT_PASSWORD), чтобы дашборду
// не нужен был токен в браузере. Наружу уходят только счётчики по ступеням,
// без имён и телефонов.
//
// Ступени (модуль = 8 уроков ≈ месяц):
//   0 — ждём / оплачен 2-й модуль (из 1-го во 2-й),
//   1 — 3-й модуль (из 2-го в 3-й),
//   2 — 4-й и дальше.
// На месяц (прошлый и текущий):
//   renewed[i] — продлили в этом месяце (оплата модуля ≥ 2, по номеру модуля);
//   due[i]     — ещё не продлили, уроки кончаются до конца месяца (в текущем
//                месяце — и те, у кого уже кончились), по модулю, который ждём.
// На следующий месяц:
//   due[i]   — у кого уроки кончатся в следующем месяце;
//   carry[i] — должны продлиться в текущем и ещё не продлили: если продлят,
//              следующий модуль кончится уже в следующем месяце (дашборд
//              добавит их со ступенью выше и весом конверсии).
// Даты — по Ташкенту (UTC+5).
// =====================================================================
import { setSyncState } from './supabase.js';
import { actGet, actConfigured } from './renewals.js';

const MODULE_SIZE = 8;
const LESSONS_PER_PREPAY = 2;
const TZ_MS = 5 * 3600e3;
const DAY_MS = 864e5;
// Совпадает с rIsExcludedStatus в дашборде.
const EXCLUDED = new Set(['terminated', 'отчислен', 'refused', 'frozen', 'transferred']);
const DAY_MAP = {
  'пн': 1, 'вт': 2, 'ср': 3, 'чт': 4, 'пт': 5, 'сб': 6, 'вс': 0,
  'воскресенье': 0, 'понедельник': 1, 'вторник': 2, 'среда': 3, 'четверг': 4, 'пятница': 5, 'суббота': 6,
  'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6, 'sun': 0,
};

const excluded = s => EXCLUDED.has(String(s || '').trim().toLowerCase());
const parseDay = s => { const m = String(s || '').match(/^(\d{4})-(\d{2})-(\d{2})/); return m ? Date.UTC(+m[1], +m[2] - 1, +m[3]) : null; };
const ymOf = t => new Date(t).toISOString().slice(0, 7);
const monthStart = (ym, shift = 0) => { const [y, m] = ym.split('-').map(Number); return Date.UTC(y, m - 1 + shift, 1); };
const stage = mod => mod <= 2 ? 0 : mod === 3 ? 1 : 2;

function todayTashkent() {
  const n = new Date(Date.now() + TZ_MS);
  return Date.UTC(n.getUTCFullYear(), n.getUTCMonth(), n.getUTCDate());
}

function lessonDays(text) {
  const days = new Set();
  const lower = String(text || '').toLowerCase().replace(/ё/g, 'е').replace(/[-_,;/]+/g, ' ');
  for (const w of lower.split(/\s+/)) if (DAY_MAP[w] != null) days.add(DAY_MAP[w]);
  return [...days];
}

// Как rDateOfReachingZeroLessons в дашборде: день, когда у ученика кончатся
// оплаченные уроки, по дням занятий группы.
function dueDate(group, lessonsRemaining, today) {
  const left = parseInt(lessonsRemaining || 0, 10) || 0;
  if (left <= 0) return today;
  const days = lessonDays(group.lesson_days);
  if (!days.length) return today + Math.ceil(left * 3.5) * DAY_MS;
  let d = today, count = 0;
  for (let safety = 365; count < left && safety > 0; safety--) {
    d += DAY_MS;
    if (days.includes(new Date(d).getUTCDay())) count++;
  }
  return d;
}

// Как rebuildCRMModuleMapping в дашборде: какие модули покрывает каждая оплата.
function moduleMapping(payments) {
  const byDeal = new Map();
  for (const p of payments) {
    const id = String(p.deal_id);
    if (!byDeal.has(id)) byDeal.set(id, []);
    byDeal.get(id).push(p);
  }
  const byTx = new Map(), maxPaid = new Map();
  for (const [dealId, list] of byDeal) {
    list.sort((a, b) => (parseDay(a.event_date) || 0) - (parseDay(b.event_date) || 0) || (a.id || 0) - (b.id || 0));
    let cursor = 0;
    const pending = [];
    for (const p of list) {
      let from, to;
      if (p.module_from != null && p.module_to != null) {
        from = p.module_from; to = p.module_to;
      } else if (p.type === 'prepayment') {
        from = to = cursor + 1 + pending.length;
        pending.push(from);
      } else if (p.type === 'payment' || p.type === 'full_payment' || p.type === 'top_up') {
        const lessons = parseInt(p.lessons_delta || 0, 10) || 0;
        if (pending.length) {
          from = pending.shift();
          to = from + Math.max(1, Math.ceil((LESSONS_PER_PREPAY + Math.max(0, lessons)) / MODULE_SIZE)) - 1;
        } else {
          from = cursor + 1;
          to = from + Math.max(1, Math.ceil(Math.max(MODULE_SIZE, lessons) / MODULE_SIZE)) - 1;
        }
      } else continue;
      if (p.type !== 'prepayment' || (p.module_from != null && p.module_to != null)) cursor = Math.max(cursor, to);
      byTx.set(p.id, { from, to });
      maxPaid.set(dealId, Math.max(maxPaid.get(dealId) || 0, to));
    }
  }
  return { byTx, maxPaid };
}

export function activationConfigured() { return actConfigured(); }

export async function syncActivationBase() {
  if (!actConfigured()) return { skipped: true };
  const [groups, payResp] = await Promise.all([
    actGet('/managed-groups'),
    actGet('/balance/payments?types=prepayment,payment,full_payment,top_up&limit=5000'),
  ]);
  const payments = Array.isArray(payResp) ? payResp : (payResp?.rows || []);
  const balance = new Map();
  for (const mode of ['active', 'prepaid', 'frozen', 'refused']) {
    const rows = await actGet(`/balance/students?mode=${mode}`).catch(() => []);
    for (const r of rows || []) { const id = String(r.deal_id); if (!balance.has(id)) balance.set(id, r); }
  }
  const enrollByGroup = new Map();
  for (let i = 0; i < groups.length; i += 5) {
    await Promise.all(groups.slice(i, i + 5).map(async g => {
      enrollByGroup.set(g.id, await actGet(`/managed-groups/${g.id}/students`).catch(() => []));
    }));
  }

  const { byTx, maxPaid } = moduleMapping(payments);
  const today = todayTashkent();

  // Активные ученики — как buildRenewals2Rows в дашборде.
  const rows = [], seen = new Set();
  for (const g of groups) {
    if (String(g.status || '').toLowerCase() === 'completed') continue;
    for (const e of enrollByGroup.get(g.id) || []) {
      const id = String(e.deal_id);
      const bal = balance.get(id) || {};
      if (seen.has(id) || excluded(bal.status) || excluded(e.student_status)) continue;
      // «Чистый онбординг»: предоплата без единой полной оплаты.
      if (parseInt(bal.prepayment_amount || 0, 10) > 0 && !bal.last_payment_date) continue;
      seen.add(id);
      rows.push({ id, due: dueDate(g, bal.lessons_remaining, today), next: Math.max(2, (maxPaid.get(id) || 0) + 1) });
    }
  }

  // Продления месяца: первый оплаченный в месяце модуль ≥ 2 по каждой сделке.
  const renewedIn = (from, to) => {
    const out = new Map();
    for (const p of payments) {
      const m = byTx.get(p.id);
      const t = parseDay(p.event_date);
      if (!m || m.from < 2 || t == null || t < from || t >= to) continue;
      const id = String(p.deal_id);
      if (!out.has(id) || out.get(id) > m.from) out.set(id, m.from);
    }
    return out;
  };

  const cur = ymOf(today);
  const months = {};
  for (const shift of [-1, 0]) {
    const from = monthStart(cur, shift), to = monthStart(cur, shift + 1);
    const paid = renewedIn(from, to);
    const renewed = [0, 0, 0], due = [0, 0, 0];
    paid.forEach(mod => { renewed[stage(mod)]++; });
    for (const r of rows) {
      if (paid.has(r.id) || r.due >= to || (r.due < from && shift !== 0)) continue;
      due[stage(r.next)]++;
    }
    months[ymOf(from)] = { renewed, due };
  }
  const nFrom = monthStart(cur, 1), nTo = monthStart(cur, 2);
  const next = { ym: ymOf(nFrom), due: [0, 0, 0], carry: [0, 0, 0] };
  for (const r of rows) {
    if (r.due >= nTo) continue;
    if (r.due >= nFrom) next.due[stage(r.next)]++;
    else next.carry[stage(r.next)]++;
  }

  const value = { months, next, students: rows.length, read_at: new Date().toISOString() };
  await setSyncState('renewal_base', JSON.stringify(value));
  console.log(`[activation-base] учеников ${rows.length}, ${cur}: ${JSON.stringify(months[cur])}`);
  return value;
}
