// =====================================================================
// Superkid Dashboard v2 — страницы «Главная» и «Маркетинг → Каналы и CAC».
//
// Только для v2: в основном index.html этих страниц нет. Данные:
//  - сделки — напрямую из Supabase (те же таблицы, что у дашборда);
//  - план — SHARED_PLANS.salesPlans из plans.json (уже загружен основным кодом);
//  - воронка — computeStats() основного кода, чтобы цифры совпадали
//    со «Статистикой»;
//  - расход по неделям — РнП (Google Sheets). Таблица должна быть открыта
//    «всем, у кого есть ссылка — читатель», иначе CAC не считается и блок
//    показывает только лиды, оплаты и конверсии.
//
// Канал сделки: utm_source=telegram → Telegram; тег лид-формы fb… → Meta.
// =====================================================================
(function () {
  const RNP_ID = '1L24JvptR0cvEo7EM-WOnvQFqHVG332C6OpCarU4Us3c';
  // gid листов РнП по месяцам. Новый месяц без gid читается по имени листа.
  const RNP_GIDS = {
    '2026-06': 219157530, '2026-07': 1946522991, '2026-08': 987230643,
    '2026-09': 143848164, '2026-10': 1830885231,
  };
  const MONTHS_RU = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'];
  const WEEKS_BACK = 7;
  // С этой даты расход Meta берётся из рекламного кабинета, раньше — из РнП.
  const META_FROM = '2026-10-01';

  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const fmt = n => Math.round(n).toLocaleString('ru-RU');

  // Числовые поля: разряды через пробел прямо при вводе (1 000, 10 000 000),
  // дробная часть — через запятую. Поле — <input type="text" data-num>,
  // значение читается через parseNum.
  function fmtNumStr(v) {
    const s = String(v ?? '').replace(/[^\d.,]/g, '');
    const i = s.search(/[.,]/);
    const int = (i < 0 ? s : s.slice(0, i)).replace(/^0+(?=\d)/, '');
    const grouped = int.replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
    return i < 0 ? grouped : `${grouped || '0'},${s.slice(i + 1).replace(/[.,]/g, '')}`;
  }
  const parseNum = v => { const t = String(v ?? '').replace(/\s/g, '').replace(',', '.'); return t === '' ? NaN : Number(t); };
  const numVal = v => v === '' || v == null || !isFinite(+v) ? '' : fmtNumStr(String(+v).replace('.', ','));
  document.addEventListener('input', e => {
    const inp = e.target;
    if (!inp || !inp.matches || !inp.matches('input[data-num]')) return;
    const caret = inp.selectionStart ?? inp.value.length;
    const keep = inp.value.slice(0, caret).replace(/[^\d.,]/g, '').length;
    const next = fmtNumStr(inp.value);
    if (next === inp.value) return;
    inp.value = next;
    let pos = 0, n = 0;
    while (pos < next.length && n < keep) { if (/[\d,]/.test(next[pos])) n++; pos++; }
    try { inp.setSelectionRange(pos, pos); } catch (err) {}
  }, true);
  const usd = (n, d = 1) => '$' + n.toFixed(d).replace('.', ',');
  const pct = (n, d = 1) => n.toFixed(d).replace('.', ',') + '%';
  const pd = s => { const m = String(s || '').match(/(\d{2})\.(\d{2})\.(\d{4})/); return m ? new Date(+m[3], +m[2] - 1, +m[1]) : null; };
  const monday = d => { const x = new Date(d.getFullYear(), d.getMonth(), d.getDate()); x.setDate(x.getDate() - ((x.getDay() + 6) % 7)); return x; };
  const key = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  const dm = d => `${String(d.getDate()).padStart(2, '0')}.${String(d.getMonth() + 1).padStart(2, '0')}`;
  const addDays = (d, n) => { const x = new Date(d); x.setDate(x.getDate() + n); return x; };
  const ym = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  const tone = r => r >= 0.95 ? 'good' : r >= 0.8 ? 'warn' : 'bad';
  const cacTone = v => v <= 25 ? 'good' : v <= 45 ? 'warn' : 'bad';

  // ---------- Данные ----------
  let dataPromise = null;
  function loadData() {
    // renewal_base — база к продлению из Activation, её раз в час считает amo-proxy.
    if (!dataPromise) dataPromise = Promise.all([loadDeals(), loadSpend(), loadRenewals(), readSyncState('renewal_base').catch(() => null)])
      .then(([deals, spend, renewals, actBase]) => { linkRenewals(deals, renewals); return { deals, spend, renewals, actBase }; });
    return dataPromise;
  }
  function resetData() { dataPromise = null; }

  async function sbAll(table, cols) {
    const out = [];
    for (let off = 0; ; off += 1000) {
      const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}?select=${cols}&order=id.asc&limit=1000&offset=${off}`, {
        headers: { apikey: SUPABASE_ANON_KEY, Authorization: 'Bearer ' + SUPABASE_ANON_KEY },
      });
      if (!r.ok) throw new Error('Supabase ' + r.status);
      const b = await r.json();
      out.push(...b);
      if (b.length < 1000) break;
    }
    return out;
  }
  const pdt = s => { const m = String(s || '').match(/(\d{2})\.(\d{2})\.(\d{4})(?:\s+(\d{1,2}):(\d{2}))?/); return m ? new Date(+m[3], +m[2] - 1, +m[1], +(m[4] || 0), +(m[5] || 0)) : null; };
  const phoneKey = p => { const d = String(p || '').replace(/\D/g, ''); return d.length >= 9 ? d.slice(-9) : ''; };

  async function loadDeals() {
    const cols = 'id,date_created,date_vr,date_qual,date_scheduled_ou,date_attended_ou,date_prepay,closed_at,status,budget,prepay_amount,' +
      'utm_source,utm_medium,utm_campaign,utm_content,tags,manager,loss_reason,child_age,language,phone';
    const rows = await sbAll('deals', cols);
    return rows.map(d => {
      const st = String(d.status || '').toLowerCase();
      const src = String(d.utm_source || '').toLowerCase();
      const ch = src === 'telegram' ? 'tg' : /\bfb\d/.test(d.tags || '') ? 'meta' : 'other';
      const created = pd(d.date_created);
      const realized = st.includes('успешно реализовано');
      const paidAt = pd(d.date_prepay) || (realized ? pd(d.closed_at) : null);
      const qualAt = pd(d.date_qual) || pd(d.date_scheduled_ou);
      const prepay = Number(d.prepay_amount) || 0;
      return {
        id: d.id, ch, created, createdAt: pdt(d.date_created), paidAt, budget: Number(d.budget) || 0,
        src, medium: String(d.utm_medium || '').trim().toLowerCase(),
        campaign: String(d.utm_campaign || '').trim().toLowerCase(), content: String(d.utm_content || '').trim().toLowerCase(),
        workedAt: pd(d.date_vr) || qualAt, qualAt, scheduledAt: pd(d.date_scheduled_ou), attendedAt: pd(d.date_attended_ou),
        // Выручка первой продажи: реализовано — бюджет, иначе внесённая предоплата.
        revenue: paidAt ? (realized ? (Number(d.budget) || 0) : prepay) : 0,
        lost: st.includes('закрыто и не реализовано'), lossReason: String(d.loss_reason || '').trim(),
        manager: d.manager || '', age: parseInt(d.child_age, 10) || null, language: String(d.language || '').trim(),
        phone: phoneKey(d.phone), renewalRevenue: 0,
      };
    }).filter(d => d.created);
  }

  async function loadRenewals() {
    try {
      const rows = await sbAll('renewals', 'id,phone,status,budget,prepay_amount,date_created,closed_at,date_prepay,date_prepay_renewal');
      return rows.map(r => {
        const st = String(r.status || '').toLowerCase().replace(/ё/g, 'е');
        const renewed = st.includes('продлили'), prepaid = st.includes('внес предоплату');
        const rev = renewed ? (Number(r.budget) || 0) : prepaid ? (Number(r.prepay_amount) || 0) : 0;
        return { phone: phoneKey(r.phone), revenue: rev, st, created: pdt(r.date_created), startAt: pd(r.date_prepay),
          // Когда сделка ушла из базы: продлили / внёс предоплату — дата продления, отказались — дата закрытия.
          doneAt: renewed || prepaid ? (pd(r.date_prepay_renewal) || pd(r.closed_at)) : st.includes('отказ') ? pd(r.closed_at) : null };
      }).filter(r => r.phone);
    } catch (e) { return []; }
  }

  // Продления привязываем к первой оплаченной сделке клиента по телефону —
  // так когорта привлечения получает и выручку продлений (LTV).
  function linkRenewals(deals, renewals) {
    const byPhone = new Map();
    deals.filter(d => d.paidAt && d.phone).sort((a, b) => a.paidAt - b.paidAt)
      .forEach(d => { if (!byPhone.has(d.phone)) byPhone.set(d.phone, d); });
    renewals.forEach(r => { if (r.revenue > 0) { const d = byPhone.get(r.phone); if (d) d.renewalRevenue += r.revenue; } });
  }

  // База к продлению на месяц по ступеням обучения. Обучение помесячное:
  // кто заплатил в прошлом месяце, тот должен продлиться в этом.
  //   s1 — новые оплаты прошлого месяца (заканчивают 1-й месяц);
  //   s2 — продления прошлого месяца у тех, кто начал в позапрошлом (2-й);
  //   s3 — остальные продления прошлого месяца (3-й месяц и дальше).
  // Сделки «Активной базы» не берём: там много тех, кто уже не учится.
  // Начало обучения — первая оплата в основной воронке или «Дата предоплаты»
  // в сделке продления. Оплаты, загруженные импортом 18.08.2026, датой начала
  // не считаются (настоящая дата неизвестна) — такие ученики идут в 3-й+.
  // Для следующего месяца база ещё растёт: текущий месяц не закрыт.
  const IMPORT_DAY = '2026-08-18';
  function renewalBase(d, ymKey) {
    const [y, m] = ymKey.split('-').map(Number);
    const p1 = ym(new Date(y, m - 2, 1)), p2 = ym(new Date(y, m - 3, 1));
    const day = x => `${x.getFullYear()}-${String(x.getMonth() + 1).padStart(2, '0')}-${String(x.getDate()).padStart(2, '0')}`;
    const start = new Map();
    d.deals.forEach(x => {
      if (!x.paidAt || !x.phone || day(x.paidAt) === IMPORT_DAY) return;
      if (!start.has(x.phone) || start.get(x.phone) > x.paidAt) start.set(x.phone, x.paidAt);
    });
    (d.renewals || []).forEach(r => { if (r.startAt && (!start.has(r.phone) || start.get(r.phone) > r.startAt)) start.set(r.phone, r.startAt); });
    const out = { s1: 0, s2: 0, s3: 0, partial: ymKey > ym(new Date()) };
    out.s1 = d.deals.filter(x => x.paidAt && ym(x.paidAt) === p1).length;
    (d.renewals || []).forEach(r => {
      if (!(r.revenue > 0) || !r.doneAt || ym(r.doneAt) !== p1) return;
      const s = start.get(r.phone);
      if (s && ym(s) === p2) out.s2++; else out.s3++;
    });
    return out;
  }

  // Парсер CSV с кавычками.
  function parseCSV(text) {
    const rows = []; let row = [], cell = '', q = false;
    for (let i = 0; i < text.length; i++) {
      const c = text[i];
      if (q) {
        if (c === '"') { if (text[i + 1] === '"') { cell += '"'; i++; } else q = false; }
        else cell += c;
      } else if (c === '"') q = true;
      else if (c === ',') { row.push(cell); cell = ''; }
      else if (c === '\n') { row.push(cell); rows.push(row); row = []; cell = ''; }
      else if (c !== '\r') cell += c;
    }
    row.push(cell); rows.push(row);
    return rows;
  }
  const money = s => { const v = parseFloat(String(s || '').replace(/[$\s ]/g, '').replace(',', '.')); return isFinite(v) ? v : 0; };

  async function fetchRnpMonth(y, m) {
    const k = `${y}-${String(m + 1).padStart(2, '0')}`;
    const urls = [];
    if (RNP_GIDS[k]) urls.push(`https://docs.google.com/spreadsheets/d/${RNP_ID}/export?format=csv&gid=${RNP_GIDS[k]}`);
    const yy = String(y).slice(2);
    [`${MONTHS_RU[m]} ${yy}`, `${MONTHS_RU[m]} ${y}`].forEach(n =>
      urls.push(`https://docs.google.com/spreadsheets/d/${RNP_ID}/gviz/tq?tqx=out:csv&headers=0&sheet=${encodeURIComponent(n)}`));
    for (const u of urls) {
      try {
        const r = await fetch(u, { cache: 'no-store' });
        if (!r.ok) continue;
        const t = await r.text();
        if (t.trim().startsWith('<')) continue; // страница входа вместо CSV
        const res = parseRnp(parseCSV(t), y, m);
        if (res) return res;
      } catch (e) { /* закрытая таблица → CORS-ошибка, пробуем дальше */ }
    }
    return null;
  }

  // Ищем строки по содержимому, а не по номерам: листы разных месяцев
  // слегка отличаются. Факт недели — ячейка справа от метки «[ n ] Неделя».
  function parseRnp(rows, y, m) {
    let weekRow = -1, weekCols = [];
    rows.forEach((r, i) => {
      if (weekRow >= 0) return;
      const cols = [];
      r.forEach((c, j) => { const mm = String(c).match(/^\[\s*\d+\s*\]\s*Неделя\s+(\d+)\s*-\s*(\d+)/); if (mm) cols.push({ j, d1: +mm[1] }); });
      if (cols.length >= 3) { weekRow = i; weekCols = cols; }
    });
    if (weekRow < 0) return null;
    const head = rows[weekRow + 1] || [];
    const monthFactCol = head.findIndex(c => String(c).trim() === 'Факт');
    const weeks = weekCols.map((w, idx) => {
      const start = (idx === 0 && w.d1 > 7) ? new Date(y, m - 1, w.d1) : new Date(y, m, w.d1);
      return { mon: key(monday(start)), col: w.j + 1 };
    });
    const res = { month: { meta: 0, tg: 0 }, weeks: {} };
    let found = false;
    rows.forEach(r => {
      const i = r.findIndex(c => String(c).trim() === 'Расходы на маркетинг');
      if (i < 0) return;
      const name = String(r[i + 1] || '').trim().toLowerCase();
      const ch = name === 'meta' ? 'meta' : name === 'telegram' ? 'tg' : null;
      if (!ch) return;
      found = true;
      if (monthFactCol >= 0) res.month[ch] = money(r[monthFactCol]);
      weeks.forEach(w => { (res.weeks[w.mon] ??= { meta: 0, tg: 0 })[ch] = money(r[w.col]); });
    });
    return found ? res : null;
  }

  // Основной источник расходов — Supabase (sync_state → marketing_spend):
  // туда раз в час пишет apps-script/rnp-spend.gs, читая закрытый РнП.
  // Если записи нет — пробуем прочитать РнП напрямую (если открыт по ссылке).
  async function loadSpend() {
    const [rnp, meta] = await Promise.all([readSyncState('marketing_spend'), readSyncState('meta_spend')]);
    let base = rnp ? { weeks: rnp.weeks || {}, months: rnp.months || {} } : null;
    if (!base) {
      const sheet = await loadSpendFromSheet();
      if (sheet) base = { weeks: sheet.weeks, months: sheet.month ? { [ym(new Date())]: sheet.month } : {} };
    }
    if (!base && !meta) return null;
    base = base || { weeks: {}, months: {} };
    // Meta — из рекламного кабинета, если он подключён (точнее РнП и обновляется
    // сам). Telegram — из РнП.
    // Meta до октября 2026 — из РнП (в сентябре реклама шла и с кабинета,
    // который не подключён), с октября — только из рекламного кабинета.
    // Неделя попадает в «кабинет», если захватывает хотя бы 1 октября.
    if (meta) {
      const put = (obj, k, v) => { (obj[k] ??= { meta: 0, tg: 0 }).meta = v; };
      Object.entries(meta.weeks || {}).forEach(([w, v]) => {
        const end = addDays(new Date(w + 'T00:00:00'), 6);
        if (key(end) >= META_FROM) put(base.weeks, w, v);
      });
      Object.entries(meta.months || {}).forEach(([m, v]) => { if (m + '-01' >= META_FROM) put(base.months, m, v); });
    }
    const metaDays = {};
    if (meta && meta.days) Object.entries(meta.days).forEach(([k, v]) => { if (k >= META_FROM) metaDays[k] = v; });
    return { weeks: base.weeks, months: base.months, metaDays, month: base.months[ym(new Date())] || null, metaSource: meta ? 'кабинет' : 'РнП' };
  }

  async function readSyncState(k) {
    try {
      const r = await fetch(`${SUPABASE_URL}/rest/v1/sync_state?key=eq.${k}&select=value`, {
        headers: { apikey: SUPABASE_ANON_KEY, Authorization: 'Bearer ' + SUPABASE_ANON_KEY },
      });
      if (!r.ok) return null;
      const rows = await r.json();
      return rows[0] && rows[0].value ? JSON.parse(rows[0].value) : null;
    } catch (e) { return null; }
  }

  async function loadSpendFromSheet() {
    const now = new Date();
    const months = [-2, -1, 0].map(o => new Date(now.getFullYear(), now.getMonth() + o, 1));
    const results = await Promise.all(months.map(d => fetchRnpMonth(d.getFullYear(), d.getMonth())));
    if (results.every(r => !r)) return null;
    const weeks = {};
    // Позже — приоритетнее: неделя на стыке месяцев есть в обоих листах,
    // полные цифры за неделю — в листе нового месяца.
    results.forEach(r => { if (r) Object.assign(weeks, r.weeks); });
    const cur = results[results.length - 1];
    return { weeks, month: cur ? cur.month : null };
  }

  // ---------- Агрегаты ----------
  // Недели когорт: по умолчанию последние WEEKS_BACK, с периодом — недели,
  // которые он захватывает (не больше 16).
  function cohorts(deals, spend, range) {
    const today = new Date(); today.setHours(0, 0, 0, 0);
    const thisMon = monday(today);
    const lastMon = range ? monday(addDays(range.to, -1)) : thisMon;
    const n = range ? Math.min(16, Math.round((lastMon - monday(range.from)) / (7 * 864e5)) + 1) : WEEKS_BACK;
    const list = [];
    for (let i = n - 1; i >= 0; i--) {
      const mon = addDays(lastMon, -7 * i);
      if (mon > thisMon) continue;
      const end = addDays(mon, 7);
      const age = Math.round((today - mon) / 864e5);
      const note = +mon === +thisMon ? 'идёт неделя' : age < 14 ? 'дозревает' : '';
      const row = { mon, label: `${dm(mon)}–${dm(addDays(mon, 6))}`, short: dm(mon), note, ch: {} };
      ['meta', 'tg'].forEach(ch => {
        const ds = deals.filter(d => d.ch === ch && d.created >= mon && d.created < end);
        const paid = ds.filter(d => d.paidAt).length;
        const s = spend && spend.weeks[key(mon)] ? spend.weeks[key(mon)][ch] : null;
        row.ch[ch] = { leads: ds.length, paid, spend: s,
          cpl: s != null && ds.length ? s / ds.length : null,
          cac: s != null && paid ? s / paid : null,
          conv: ds.length ? paid / ds.length * 100 : 0 };
      });
      list.push(row);
    }
    return list;
  }

  function monthChannels(deals, spend) {
    const now = new Date();
    const from = new Date(now.getFullYear(), now.getMonth(), 1);
    const to = new Date(now.getFullYear(), now.getMonth() + 1, 1);
    const res = {};
    ['meta', 'tg'].forEach(ch => {
      const leads = deals.filter(d => d.ch === ch && d.created >= from && d.created < to).length;
      const paid = deals.filter(d => d.ch === ch && d.paidAt && d.paidAt >= from && d.paidAt < to).length;
      const s = spend && spend.month ? spend.month[ch] : null;
      res[ch] = { leads, paid, spend: s, cpl: s != null && leads ? s / leads : null, cac: s != null && paid ? s / paid : null };
    });
    return res;
  }

  // ---------- Период (общий для страниц маркетинга и аналитики продаж) ----------
  // Выбор хранится в браузере и переходит со страницы на страницу.
  const PERIODS = [['this_week', 'Эта неделя'], ['last_week', 'Прошлая неделя'], ['this_month', 'Этот месяц'], ['last_month', 'Прошлый месяц'],
    ['d30', '30 дней'], ['d90', '90 дней'], ['custom', 'Свой период']];
  const PERIOD_KEY = 'sk-v2-period';
  function periodGet() {
    try { const v = JSON.parse(localStorage.getItem(PERIOD_KEY) || 'null'); if (v && v.k) return v; } catch (e) {}
    return { k: 'this_month' };
  }
  function periodSet(v) { try { localStorage.setItem(PERIOD_KEY, JSON.stringify(v)); } catch (e) {} }
  const isoDay = s => { const m = String(s || '').match(/^(\d{4})-(\d{2})-(\d{2})$/); return m ? new Date(+m[1], +m[2] - 1, +m[3]) : null; };
  // {from, to (не включая), k, label, days, isThisMonth}
  function periodRange(v = periodGet()) {
    const t = new Date(); t.setHours(0, 0, 0, 0);
    const tomorrow = addDays(t, 1), mon = monday(t);
    let from, to;
    switch (v.k) {
      case 'this_week': from = mon; to = tomorrow; break;
      case 'last_week': from = addDays(mon, -7); to = mon; break;
      case 'last_month': from = new Date(t.getFullYear(), t.getMonth() - 1, 1); to = new Date(t.getFullYear(), t.getMonth(), 1); break;
      case 'd30': from = addDays(t, -29); to = tomorrow; break;
      case 'd90': from = addDays(t, -89); to = tomorrow; break;
      case 'all': from = new Date(2020, 0, 1); to = tomorrow; break;
      case 'custom': {
        const a = isoDay(v.from), b = isoDay(v.to);
        if (a && b && a <= b) { from = a; to = addDays(b, 1); break; }
      } // нет дат — как «этот месяц»
      // falls through
      default: from = new Date(t.getFullYear(), t.getMonth(), 1); to = tomorrow;
    }
    const last = addDays(to, -1);
    const label = v.k === 'all' ? 'всё время' : `${dm(from)}${from.getFullYear() !== last.getFullYear() ? '.' + from.getFullYear() : ''} – ${dm(last)}.${last.getFullYear()}`;
    return { from, to, k: v.k, label, days: Math.round((to - from) / 864e5), isThisMonth: v.k === 'this_month' };
  }
  function periodBar(extra) {
    const v = periodGet(), r = periodRange(v);
    const items = extra ? [...extra, ...PERIODS] : PERIODS;
    return `<div class="ki-period" data-period>
      <div class="ki-seg" role="group">${items.map(([k, l]) => `<button type="button" data-pk="${k}" class="${k === v.k ? 'on' : ''}">${l}</button>`).join('')}</div>
      <span class="ki-period-dates"${v.k === 'custom' ? '' : ' hidden'}><input type="date" data-pf value="${key(r.from)}" aria-label="С"> – <input type="date" data-pt value="${key(addDays(r.to, -1))}" aria-label="По"></span>
      <span class="ki-period-lbl">${r.label}</span></div>`;
  }
  function bindPeriod(root, onChange) {
    const box = root.querySelector('[data-period]');
    if (!box) return;
    box.addEventListener('click', e => {
      const b = e.target.closest('button[data-pk]'); if (!b) return;
      const cur = periodRange();
      const v = b.dataset.pk === 'custom' ? { k: 'custom', from: key(cur.from), to: key(addDays(cur.to, -1)) } : { k: b.dataset.pk };
      periodSet(v); onChange();
    });
    box.addEventListener('change', e => {
      if (!e.target.matches('input[type="date"]')) return;
      const f = box.querySelector('[data-pf]').value, t = box.querySelector('[data-pt]').value;
      if (f && t && f <= t) { periodSet({ k: 'custom', from: f, to: t }); onChange(); }
    });
  }

  // Расход канала за период, $. По дням: Meta с октября — из кабинета;
  // остальное — неделя РнП (или месяц) делится на прошедшие дни.
  function spendFor(spend, ch, from, to) {
    if (!spend) return null;
    const t = new Date(); t.setHours(0, 0, 0, 0);
    const end = to < addDays(t, 1) ? to : addDays(t, 1);
    let total = 0, any = false;
    for (let dd = new Date(from); dd < end; dd = addDays(dd, 1)) {
      const k = key(dd);
      if (ch === 'meta' && spend.metaDays && k >= META_FROM && Object.keys(spend.metaDays).length) { total += spend.metaDays[k] || 0; any = true; continue; }
      const mon = monday(dd), w = spend.weeks && spend.weeks[key(mon)];
      if (w && w[ch] != null) { total += w[ch] / Math.min(7, Math.round((t - mon) / 864e5) + 1); any = true; continue; }
      const mk = ym(dd), m = spend.months && spend.months[mk];
      if (m && m[ch] != null) {
        const first = new Date(dd.getFullYear(), dd.getMonth(), 1), dim = new Date(dd.getFullYear(), dd.getMonth() + 1, 0).getDate();
        total += m[ch] / Math.min(dim, Math.round((t - first) / 864e5) + 1); any = true;
      }
    }
    return any ? total : null;
  }

  // Лиды, оплаты, выручка и расход по каналам за период.
  function rangeChannels(d, from, to, rate) {
    const out = {};
    ['meta', 'tg', 'other'].forEach(ch => {
      const ds = d.deals.filter(x => x.ch === ch);
      const leads = ds.filter(x => x.created >= from && x.created < to).length;
      const paidDs = ds.filter(x => x.paidAt && x.paidAt >= from && x.paidAt < to);
      const revenue = paidDs.reduce((a, x) => a + x.revenue, 0);
      const spend = ch === 'other' ? 0 : (spendFor(d.spend, ch, from, to) || 0);
      out[ch] = { leads, paid: paidDs.length, revenue, spend,
        cpl: spend && leads ? spend / leads : null, cac: spend && paidDs.length ? spend / paidDs.length : null,
        romi: spend ? (revenue / (rate || 12000) - spend) / spend * 100 : null };
    });
    return out;
  }

  function monthPace() {
    const now = new Date();
    const dim = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
    return { day: now.getDate(), dim, pace: now.getDate() / dim, label: `${MONTHS_RU[now.getMonth()]} ${now.getFullYear()} · прошло ${now.getDate()} из ${dim} дней` };
  }

  // ---------- Отрисовка ----------
  const CH = { meta: { name: 'Meta', sub: 'лид-форма' }, tg: { name: 'Telegram', sub: 'баннер' } };

  // Живой план месяца. Цель по выручке, чеки и конверсии — из «Плана»
  // (bizPlan; если его ещё не сохраняли — из плана продаж). Продления не
  // берутся из сохранённого плана, а считаются сейчас: база по ступеням
  // (Activation, иначе оценка по Amo) × конверсии ступеней. Новые продажи =
  // цель − продления, воронка — назад по конверсиям.
  function livePlan(d, k) {
    const SP = (typeof SHARED_PLANS !== 'undefined' && SHARED_PLANS) || {};
    const sp = (SP.salesPlans || {})[k] || {}, bp = (SP.bizPlan || {})[k] || {};
    const total = +bp.total || ((+sp.revenue || 0) + (+sp.renewalRevenue || 0));
    if (!total || !d) return sp;
    const cr = [+bp.ren1Cr || 70, +bp.ren2Cr || 80, +bp.ren3Cr || 85];
    const rb = window.V2Mkt ? window.V2Mkt.renewalBaseFor(d, k, cr) : { ...renewalBase(d, k), source: 'amo' };
    const renewalClients = Math.round(rb.s1 * cr[0] / 100 + rb.s2 * cr[1] / 100 + rb.s3 * cr[2] / 100);
    const renCheck = +bp.renCheck || (sp.renewalClients ? sp.renewalRevenue / sp.renewalClients : 800000);
    const renewalRevenue = Math.round(renewalClients * renCheck);
    const revenue = Math.max(0, total - renewalRevenue);
    const newCheck = +bp.newCheck || (sp.payments ? sp.revenue / sp.payments : 800000);
    const c = { crLeadQual: +bp.crQual || +sp.crLeadQual || 0, crQualScheduled: +bp.crSched || +sp.crQualScheduled || 0,
      crScheduledAttended: +bp.crAtt || +sp.crScheduledAttended || 0, crAttendedPaid: +bp.crPaid || +sp.crAttendedPaid || 0 };
    const back = (x, cr) => cr ? Math.round(x / (cr / 100)) : 0;
    const payments = newCheck ? Math.round(revenue / newCheck) : 0;
    const attendedOU = back(payments, c.crAttendedPaid), scheduledOU = back(attendedOU, c.crScheduledAttended),
      qualLeads = back(scheduledOU, c.crQualScheduled), newLeads = back(qualLeads, c.crLeadQual);
    return { ...sp, ...c, total, revenue, renewalRevenue, renewalClients, renCheck, newCheck, payments, attendedOU, scheduledOU, qualLeads, newLeads, rb, cr, live: true };
  }

  function funnelHtml(planIn) {
    const p = monthPace();
    const now = new Date();
    const from = new Date(now.getFullYear(), now.getMonth(), 1);
    const to = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59);
    let st = null;
    try { st = computeStats('all', from, to); } catch (e) { return ''; }
    const plan = planIn || (typeof SHARED_PLANS !== 'undefined' && SHARED_PLANS.salesPlans && SHARED_PLANS.salesPlans[ym(now)]) || {};
    const stages = [
      { lbl: 'Лиды', f: st.newLeads, p: plan.newLeads },
      { lbl: 'Квал', f: st.qualLeads, p: plan.qualLeads, cr: [st.crLeadQual, plan.crLeadQual] },
      { lbl: 'Назначено ОУ', f: st.scheduledOU, p: plan.scheduledOU, cr: [st.crQualScheduled, plan.crQualScheduled] },
      { lbl: 'Пришли на ОУ', f: st.attendedOU, p: plan.attendedOU, cr: [st.crScheduledAttended, plan.crScheduledAttended] },
      { lbl: 'Оплаты', f: st.payments, p: plan.payments, cr: [st.crAttendedPaid, plan.crAttendedPaid] },
      { lbl: 'Выручка, сум', f: st.revenue, p: plan.revenue, money: true },
    ];
    const cells = stages.map(s => {
      const hasPlan = s.p > 0;
      const done = hasPlan ? s.f / s.p : 0;
      const vsPace = hasPlan ? done / p.pace : 1;
      const val = s.money ? (s.f / 1e6).toFixed(1).replace('.', ',') + ' млн' : fmt(s.f);
      const planTxt = !hasPlan ? 'план не задан' : 'из ' + (s.money ? Math.round(s.p / 1e6) + ' млн' : fmt(s.p));
      let chip = '';
      if (s.cr && s.cr[1]) chip = `<span class="ki-chip ki-${tone(s.cr[0] / s.cr[1])}">CR ${pct(s.cr[0])} · план ${s.cr[1]}%</span>`;
      else if (hasPlan) chip = `<span class="ki-chip ki-${tone(vsPace)}">${Math.round(vsPace * 100)}% от темпа</span>`;
      return `<div class="ki-stage">
        <div class="ki-lbl">${s.lbl}</div>
        <div class="ki-val num${s.money ? ' ki-sm' : ''}">${val}</div>
        <div class="ki-bar" aria-hidden="true"><b style="width:${Math.min(done, 1) * 100}%;background:var(--${hasPlan ? { good: 'green', warn: 'yellow', bad: 'red' }[tone(vsPace)] : 'border'})"></b>${hasPlan ? `<i style="left:${p.pace * 100}%"></i>` : ''}</div>
        <div class="ki-meta num"><span>${hasPlan ? Math.round(done * 100) + '% плана' : ''}</span><span>${planTxt}</span></div>
        ${chip}
      </div>`;
    }).join('');
    return `<section class="ki-panel">
      <div class="ki-ph"><h3>Воронка новых продаж</h3><span>Черта на полосе — где надо быть сегодня, чтобы выполнить план</span></div>
      <div class="ki-funnel">${cells}</div>
    </section>`;
  }

  function leadsChart(rows) {
    const W = 560, H = 230, L = 34, R = 8, T = 14, B = 26;
    const maxRaw = Math.max(10, ...rows.flatMap(r => [r.ch.meta.leads, r.ch.tg.leads]));
    const step = maxRaw > 300 ? 100 : maxRaw > 120 ? 50 : maxRaw > 40 ? 20 : 10;
    const max = Math.ceil(maxRaw / step) * step;
    const cw = (W - L - R) / rows.length, bw = Math.min(16, cw * 0.28);
    const y = v => T + (H - T - B) * (1 - v / max);
    let s = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Лиды по неделям и каналам">`;
    for (let t = 0; t <= max; t += step) {
      s += `<line x1="${L}" x2="${W - R}" y1="${y(t)}" y2="${y(t)}" stroke="var(--border)"/>`;
      s += `<text x="${L - 6}" y="${y(t) + 4}" text-anchor="end" font-size="10" fill="var(--text-dim)">${t}</text>`;
    }
    rows.forEach((r, i) => {
      const cx = L + cw * i + cw / 2, live = i === rows.length - 1;
      [['meta', cx - bw - 2], ['tg', cx + 2]].forEach(([ch, x]) => {
        const v = r.ch[ch].leads;
        s += `<rect x="${x}" y="${y(v)}" width="${bw}" height="${Math.max(0, y(0) - y(v))}" rx="3" fill="var(--${ch})" opacity="${live ? 0.5 : 1}"/>`;
        if (v > 0) s += `<text x="${x + bw / 2}" y="${y(v) - 4}" text-anchor="middle" font-size="9.5" fill="var(--text-dim)">${v}</text>`;
      });
      s += `<text x="${cx}" y="${H - 8}" text-anchor="middle" font-size="10.5" fill="var(--text-dim)">${r.short}${live ? ' •' : ''}</text>`;
    });
    return s + '</svg>';
  }

  function cplChart(rows) {
    const pts = { meta: rows.map(r => r.ch.meta.cpl), tg: rows.map(r => r.ch.tg.cpl) };
    const all = [...pts.meta, ...pts.tg].filter(v => v != null);
    if (!all.length) return '';
    const W = 560, H = 220, L = 34, R = 14, T = 14, B = 26;
    const max = Math.max(1, Math.ceil(Math.max(...all)));
    const xs = i => L + (W - L - R) * i / (rows.length - 1);
    const ys = v => T + (H - T - B) * (1 - v / max);
    let s = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Стоимость лида по неделям">`;
    for (let t = 0; t <= max; t++) {
      s += `<line x1="${L}" x2="${W - R}" y1="${ys(t)}" y2="${ys(t)}" stroke="var(--border)"/>`;
      s += `<text x="${L - 6}" y="${ys(t) + 4}" text-anchor="end" font-size="10" fill="var(--text-dim)">$${t}</text>`;
    }
    ['meta', 'tg'].forEach(ch => {
      const arr = pts[ch];
      const p = arr.map((v, i) => v == null ? null : [xs(i), ys(v)]).filter(Boolean);
      if (p.length > 1) s += `<polyline points="${p.map(q => q.join(',')).join(' ')}" fill="none" stroke="var(--${ch})" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>`;
      const lastI = arr.reduce((a, v, i) => v != null ? i : a, -1);
      arr.forEach((v, i) => {
        if (v == null) return;
        const last = i === lastI;
        s += `<circle cx="${xs(i)}" cy="${ys(v)}" r="${last ? 4.5 : 3}" fill="${last ? `var(--${ch})` : 'var(--card)'}" stroke="var(--${ch})" stroke-width="2"/>`;
        if (last) s += `<text x="${xs(i) - 8}" y="${ys(v) - 9}" text-anchor="end" font-size="11" font-weight="700" fill="var(--${ch})">${usd(v, 2)}</text>`;
      });
    });
    rows.forEach((r, i) => { s += `<text x="${xs(i)}" y="${H - 8}" text-anchor="middle" font-size="10.5" fill="var(--text-dim)">${r.short}</text>`; });
    return s + '</svg>';
  }

  function channelCards(mc, spendOk, compact) {
    const p = monthPace();
    return ['meta', 'tg'].map(ch => {
      const c = mc[ch];
      return `<div class="ki-ch">
        <div class="ki-ch-h"><i class="ki-sw" style="background:var(--${ch})"></i>${CH[ch].name}<small>${CH[ch].sub}</small></div>
        <div class="ki-big num">${c.cac != null ? '$' + Math.round(c.cac) : '—'}<small>CAC за период</small></div>
        <dl class="ki-kv num">
          <dt>Расход</dt><dd>${c.spend != null ? usd(c.spend) : '—'}</dd>
          <dt>Лиды · CPL</dt><dd>${fmt(c.leads)}${c.cpl != null ? ' · ' + usd(c.cpl, 2) : ''}</dd>
          <dt>Оплат за период</dt><dd>${c.paid}</dd>
        </dl>
      </div>`;
    }).join('') + (spendOk ? '' : '');
  }

  function cohortTable(rows) {
    let html = '';
    rows.forEach(r => {
      ['meta', 'tg'].forEach((ch, k) => {
        const c = r.ch[ch];
        if (!c.leads && !c.spend) return;
        html += `<tr>
          <td class="ki-wk">${k === 0 || !r.ch.meta.leads ? r.label + (r.note ? ` <span class="ki-pill">${r.note}</span>` : '') : ''}</td>
          <td><span class="ki-chan"><i class="ki-sw" style="background:var(--${ch})"></i>${CH[ch].name}</span></td>
          <td>${c.spend != null ? usd(c.spend) : '—'}</td>
          <td>${fmt(c.leads)}</td>
          <td>${c.cpl != null ? usd(c.cpl, 2) : '—'}</td>
          <td>${c.paid}</td>
          <td>${c.leads ? pct(c.conv) : '—'}</td>
          <td>${c.cac != null ? `<span class="ki-cac ki-${cacTone(c.cac)}">$${Math.round(c.cac)}</span>` : '—'}</td>
        </tr>`;
      });
    });
    return `<div class="ki-tbl"><table>
      <thead><tr><th>Неделя</th><th>Канал</th><th>Расход</th><th>Лиды</th><th>CPL</th><th>Оплаты</th><th>Лид → оплата</th><th>CAC</th></tr></thead>
      <tbody>${html}</tbody></table></div>`;
  }

  function cacBars(rows) {
    const items = [];
    rows.forEach(r => { if (r.note) return; ['meta', 'tg'].forEach(ch => { const c = r.ch[ch]; if (c.cac != null) items.push({ w: r.short, ch, v: c.cac }); }); });
    if (!items.length) return '<p class="ki-note">Нет закрытых когорт с расходом.</p>';
    const max = Math.max(60, ...items.map(i => i.v));
    return items.map(it => `<div class="ki-cac-row num">
      <span class="ki-wk">${it.w}</span>
      <span class="ki-chan"><i class="ki-sw" style="background:var(--${it.ch})"></i>${it.ch === 'meta' ? 'Meta' : 'TG'}</span>
      <span class="ki-track"><b style="width:${Math.min(it.v / max, 1) * 100}%;background:var(--${{ good: 'green', warn: 'yellow', bad: 'red' }[cacTone(it.v)]})"></b><i style="left:${25 / max * 100}%"></i><i style="left:${45 / max * 100}%"></i></span>
      <span class="ki-v">$${Math.round(it.v)}</span>
    </div>`).join('');
  }

  const legend = `<div class="ki-legend"><span><i class="ki-sw" style="background:var(--meta)"></i>Meta (лид-форма)</span><span><i class="ki-sw" style="background:var(--tg)"></i>Telegram (баннер)</span></div>`;
  const noSpend = `<div class="ki-notice">Расходы по каналам ещё не загружены, поэтому CAC и CPL не посчитаны. Их раз в час приносит из РнП скрипт apps-script/rnp-spend.gs — проверьте, что он добавлен в Apps Script и запущен.</div>`;

  // ---------- Главная (взгляд CEO) ----------
  // Деньги месяца → сигналы → воронка новых → продления → маркетинг → скорость.
  function renderHome(el, d) {
    // База продлений берётся из Activation, когда он загрузится, — перерисуем.
    const M = window.V2Mkt;
    if (M && M.hasAct() && !M.isActReady()) M.loadAct().then(() => { if (el.isConnected && M.isActReady()) renderHome(el, d); });
    const now = new Date(), p = monthPace();
    const from = new Date(now.getFullYear(), now.getMonth(), 1);
    const to = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59);
    const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 1);
    const k = ym(now);
    const SP = (typeof SHARED_PLANS !== 'undefined' && SHARED_PLANS) || {};
    const plan = livePlan(d, k), mp = (SP.mktPlan || {})[k] || {}, bp = (SP.bizPlan || {})[k] || {};
    const rate = +mp.rate || +bp.rate || 12000;
    let st = {};
    try { st = computeStats('all', from, to); } catch (e) {}
    const daysLeft = p.dim - p.day;
    const sig = [];   // [тон, текст, страница]
    const paceOf = (f, pl) => pl ? (f / pl) / p.pace : null;
    const pctOf = (f, pl) => pl ? Math.round(f / pl * 100) : null;
    const toneP = r => r == null ? '' : r >= 0.9 ? 'good' : r >= 0.8 ? 'warn' : 'bad';
    const chip = (r, label) => r == null ? '<span class="ki-chip ki-mute">план не задан</span>' : `<span class="ki-chip ki-${toneP(r)}">${Math.round(r * 100)}% ${label || 'от темпа'}</span>`;

    // 1. Деньги месяца
    const newF = st.revenue || 0, renF = st.renewalRevenue || 0, totF = newF + renF;
    const newP = +plan.revenue || 0, renP = +plan.renewalRevenue || 0, totP = newP + renP;
    const forecast = p.day ? totF / p.day * p.dim : 0;
    const needDay = daysLeft > 0 && totP > totF ? (totP - totF) / daysLeft : 0;
    const nowDay = p.day ? totF / p.day : 0;
    const money = `<div class="ki-money">
      <div class="ki-money-main">
        <div class="ki-lbl">Выручка месяца</div>
        <div class="ki-money-val num">${(totF / 1e6).toFixed(1).replace('.', ',')}<small> млн из ${totP ? Math.round(totP / 1e6) : '—'}</small></div>
        <div class="ki-bar ki-bar-lg" aria-hidden="true"><b style="width:${totP ? Math.min(totF / totP, 1) * 100 : 0}%;background:var(--${{ good: 'green', warn: 'yellow', bad: 'red' }[toneP(paceOf(totF, totP))] || 'accent'})"></b>${totP ? `<i style="left:${p.pace * 100}%"></i>` : ''}</div>
        <div class="ki-money-row">${chip(paceOf(totF, totP))}<span>${pctOf(totF, totP) ?? '—'}% плана · прошло ${p.day} из ${p.dim} дней</span></div>
      </div>
      <div class="ki-kpi"><div class="ki-lbl">Прогноз на конец месяца</div><div class="ki-val num">${(forecast / 1e6).toFixed(0)} млн</div><div class="ki-kpi-sub">${totP ? `${Math.round(forecast / totP * 100)}% плана при текущем темпе` : 'план не задан'}</div></div>
      <div class="ki-kpi"><div class="ki-lbl">Нужно в день до конца</div><div class="ki-val num">${needDay ? (needDay / 1e6).toFixed(1).replace('.', ',') + ' млн' : '—'}</div><div class="ki-kpi-sub">сейчас ${(nowDay / 1e6).toFixed(1).replace('.', ',')} млн в день${needDay && nowDay ? ` · ×${(needDay / nowDay).toFixed(1).replace('.', ',')}` : ''}</div></div>
      <div class="ki-kpi"><div class="ki-lbl">Новые продажи</div><div class="ki-val num">${(newF / 1e6).toFixed(1).replace('.', ',')} млн</div><div class="ki-kpi-sub">${chip(paceOf(newF, newP))} из ${newP ? Math.round(newP / 1e6) + ' млн' : '—'}</div></div>
      <div class="ki-kpi"><div class="ki-lbl">Продления</div><div class="ki-val num">${(renF / 1e6).toFixed(1).replace('.', ',')} млн</div><div class="ki-kpi-sub">${chip(paceOf(renF, renP))} из ${renP ? Math.round(renP / 1e6) + ' млн' : '—'}</div></div>
    </div>`;
    // Один сигнал про выручку: детали уже в блоке «Деньги месяца».
    { const r = paceOf(totF, totP);
      if (r != null && r < 0.9) {
        const parts = [[paceOf(newF, newP), 'новые'], [paceOf(renF, renP), 'продления']].filter(([x]) => x != null).map(([x, n]) => `${n} ${Math.round(x * 100)}%`).join(', ');
        sig.push([r < 0.8 ? 'bad' : 'warn', `Выручка идёт на ${Math.round(r * 100)}% от темпа плана${parts ? ` (${parts})` : ''}. Прогноз — ${Math.round(forecast / 1e6)} млн.`]);
      } }

    // 3. Новые: средний чек и доля предоплат (сделки, оплаченные в месяце)
    const paidM = d.deals.filter(x => x.paidAt && x.paidAt >= from && x.paidAt < monthEnd);
    const avgCheck = st.payments ? newF / st.payments : 0;
    const planCheck = plan.newCheck || (plan.payments ? newP / plan.payments : 0);
    const prepayShare = paidM.length ? paidM.filter(x => x.revenue && x.revenue < x.budget).length / paidM.length : 0;
    if (avgCheck && planCheck && avgCheck < planCheck * 0.95) sig.push([avgCheck < planCheck * 0.9 ? 'bad' : 'warn', `Средний чек новых ${fmt(avgCheck)} сум — ${Math.round(avgCheck / planCheck * 100)}% от плана.`]);
    if (plan.newLeads) { const r = paceOf(st.newLeads || 0, plan.newLeads); if (r < 0.9) sig.push([r < 0.8 ? 'bad' : 'warn', `Лиды: ${Math.round(r * 100)}% от темпа плана.`, 'mktSummary']); }
    if (plan.crLeadQual && st.crLeadQual != null) { const r = st.crLeadQual / plan.crLeadQual; if (r < 0.9) sig.push([r < 0.8 ? 'bad' : 'warn', `CR лид → квал ${pct(st.crLeadQual)} при плане ${plan.crLeadQual}%.`, 'quality']); }

    // 4. Продления
    const renC = st.renewalClients || 0, renCP = +plan.renewalClients || 0;
    const renew = `<section class="ki-panel"><div class="ki-ph"><h3>Продления</h3><span>Факт — оплаты в воронке «Продления»; план — база × ${plan.cr ? plan.cr.join(' / ') : '70 / 80 / 85'}%</span></div>
      <div class="ki-funnel ki-funnel-2">
        ${[['Клиентов продлили', renC, renCP, false], ['Выручка продлений', renF, renP, true]].map(([l, f, pl, m]) => `<div class="ki-stage">
          <div class="ki-lbl">${l}</div><div class="ki-val num${m ? ' ki-sm' : ''}">${m ? (f / 1e6).toFixed(1).replace('.', ',') + ' млн' : fmt(f)}</div>
          <div class="ki-bar" aria-hidden="true"><b style="width:${pl ? Math.min(f / pl, 1) * 100 : 0}%;background:var(--${{ good: 'green', warn: 'yellow', bad: 'red' }[toneP(paceOf(f, pl))] || 'border'})"></b>${pl ? `<i style="left:${p.pace * 100}%"></i>` : ''}</div>
          <div class="ki-meta num"><span>${pl ? Math.round(f / pl * 100) + '% плана' : ''}</span><span>${pl ? 'из ' + (m ? Math.round(pl / 1e6) + ' млн' : fmt(pl)) : 'план не задан'}</span></div>
          ${chip(paceOf(f, pl))}</div>`).join('')}
        ${(() => {
          const rb = plan.rb || (window.V2Mkt ? window.V2Mkt.renewalBaseFor(d, k, [70, 80, 85]) : { ...renewalBase(d, k), source: 'amo' });
          const base = rb.s1 + rb.s2 + rb.s3; const r = base ? renC / base : null;
          const M2 = window.V2Mkt;
          const src = rb.source === 'act' ? `Activation: модуль 2 — ${fmt(rb.s1)}, 3 — ${fmt(rb.s2)}, 4+ — ${fmt(rb.s3)}`
            : M2 && M2.hasAct() ? 'Activation загружается… пока оценка по оплатам Amo'
            : `Оценка по оплатам Amo. <button type="button" class="ki-go" data-actlogin>Подключить Activation</button>`;
          return `<div class="ki-stage"><div class="ki-lbl">База к продлению</div><div class="ki-val num">${fmt(base)}</div>
          <div class="ki-kpi-sub">${src}</div>
          <div class="ki-meta num"><span>Продлили ${r != null ? Math.round(r * 100) + '% базы' : '—'}</span><span>план ${base ? Math.round(renCP / base * 100) + '%' : '—'}</span></div></div>`; })()}
      </div></section>`;

    // 5. Маркетинг по каналам
    const spendM = d.spend && d.spend.months ? d.spend.months[k] : (d.spend && d.spend.month);
    const cacT = +mp.cacTarget || 0;
    const chs = ['meta', 'tg'].map(ch => {
      const spend = spendM ? +spendM[ch] || 0 : 0;
      const budget = ch === 'meta' ? +mp.metaBudget || 0 : +mp.tgBudget || 0;
      const leads = d.deals.filter(x => x.ch === ch && x.created >= from && x.created < monthEnd).length;
      const paid = paidM.filter(x => x.ch === ch);
      const rev = paid.reduce((a, x) => a + x.revenue, 0);
      return { ch, spend, budget, leads, paid: paid.length, rev, cac: paid.length && spend ? spend / paid.length : null, romi: spend ? (rev / rate - spend) / spend * 100 : null, bpace: budget ? (spend / budget) / p.pace : null };
    });
    const tot = chs.reduce((a, c) => ({ spend: a.spend + c.spend, budget: a.budget + c.budget, leads: a.leads + c.leads, paid: a.paid + c.paid, rev: a.rev + c.rev }), { spend: 0, budget: 0, leads: 0, paid: 0, rev: 0 });
    tot.cac = tot.paid && tot.spend ? tot.spend / tot.paid : null; tot.romi = tot.spend ? (tot.rev / rate - tot.spend) / tot.spend * 100 : null; tot.bpace = tot.budget ? (tot.spend / tot.budget) / p.pace : null;
    const bTone = r => r == null ? '' : (r < 0.75 || r > 1.2) ? 'bad' : (r < 0.85 || r > 1.1) ? 'warn' : 'good';
    const cTone = v => v == null || !cacT ? '' : v > cacT * 1.5 ? 'bad' : v > cacT * 1.25 ? 'warn' : 'good';
    const mRow = (name, c, cls) => `<tr class="${cls || ''}"><td>${name}</td><td>${c.spend ? usd(c.spend, 0) : '—'}${c.budget ? `<small>из ${usd(c.budget, 0)}</small>` : ''}</td>
      <td>${c.bpace != null ? `<span class="ki-cac ki-${bTone(c.bpace)}">${Math.round(c.bpace * 100)}%</span>` : '—'}</td><td>${fmt(c.leads)}</td><td>${c.paid}</td>
      <td>${c.cac != null ? `<span class="ki-cac ki-${cTone(c.cac)}">$${Math.round(c.cac)}</span>` : '—'}</td>
      <td>${c.romi != null && c.paid ? `<span class="ki-cac ki-${c.romi >= 100 ? 'good' : c.romi >= 0 ? 'warn' : 'bad'}">${Math.round(c.romi)}%</span>` : '—'}</td></tr>`;
    const mkt = `<section class="ki-panel"><div class="ki-ph"><h3>Маркетинг</h3><span>${cacT ? `Цель CAC $${cacT}` : 'Цель CAC не задана'} · курс ${fmt(rate)}</span></div>
      <div class="ki-tbl"><table><thead><tr><th>Канал</th><th>Расход</th><th>Темп бюджета</th><th>Лиды</th><th>Оплаты</th><th>CAC</th><th>ROMI</th></tr></thead>
      <tbody>${mRow('<span class="ki-chan"><i class="ki-sw" style="background:var(--meta)"></i>Meta</span>', chs[0])}${mRow('<span class="ki-chan"><i class="ki-sw" style="background:var(--tg)"></i>Telegram</span>', chs[1])}${mRow('<b>Итого</b>', tot, 'ki-tr-tot')}</tbody></table></div></section>`;
    if (tot.bpace != null && bTone(tot.bpace) !== 'good') sig.push([bTone(tot.bpace), `Бюджет маркетинга: ${Math.round(tot.bpace * 100)}% от темпа — ${tot.bpace < 1 ? 'недотрачиваем, лидов будет меньше' : 'перерасход'}.`, 'mktSummary']);
    chs.forEach(c => { if (c.cac != null && cTone(c.cac) !== 'good' && cTone(c.cac)) sig.push([cTone(c.cac), `CAC ${c.ch === 'meta' ? 'Meta' : 'Telegram'} $${Math.round(c.cac)} при цели $${cacT}.`, c.ch === 'meta' ? 'meta' : 'tg']); });

    // 6. Скорость обработки (лиды месяца)
    const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const monthLeads = d.deals.filter(x => x.created >= from && x.created < todayStart);
    const sameDay = monthLeads.filter(x => x.workedAt && x.workedAt - x.created < 864e5).length;
    const sameShare = monthLeads.length ? sameDay / monthLeads.length : 0;
    const untouched = d.deals.filter(x => x.created >= addDays(todayStart, -14) && x.created < addDays(todayStart, -1) && !x.workedAt && !x.lost).length;
    const crBy = ch => { const L = d.deals.filter(x => x.ch === ch && x.created >= from && x.created < monthEnd); return L.length ? L.filter(x => x.qualAt).length / L.length * 100 : null; };
    const sTone = sameShare >= 0.8 ? 'good' : sameShare >= 0.7 ? 'warn' : sameShare >= 0.55 ? 'warn' : 'bad';
    const speed = `<section class="ki-panel"><div class="ki-ph"><h3>Обработка лидов</h3><span>Лиды этого месяца</span></div>
      <div class="ki-kpis ki-kpis-in">
        <div class="ki-kpi"><div class="ki-lbl">Взяли в работу в день заявки</div><div class="ki-val num">${Math.round(sameShare * 100)}%</div><div class="ki-kpi-sub"><span class="ki-chip ki-${sTone}">цель 80%</span> ${sameDay} из ${monthLeads.length}</div></div>
        <div class="ki-kpi"><div class="ki-lbl">Без касания больше суток</div><div class="ki-val num">${untouched}</div><div class="ki-kpi-sub">лиды за 14 дней, не взяты в работу и не закрыты</div></div>
        <div class="ki-kpi"><div class="ki-lbl">CR лид → квал</div><div class="ki-val num">${st.crLeadQual != null ? pct(st.crLeadQual) : '—'}</div><div class="ki-kpi-sub">Meta ${crBy('meta') != null ? Math.round(crBy('meta')) + '%' : '—'} · Telegram ${crBy('tg') != null ? Math.round(crBy('tg')) + '%' : '—'}${plan.crLeadQual ? ` · план ${plan.crLeadQual}%` : ''}</div></div>
      </div></section>`;
    if (sameShare < 0.7 && monthLeads.length >= 20) sig.push([sameShare < 0.55 ? 'bad' : 'warn', `В день заявки берут в работу только ${Math.round(sameShare * 100)}% лидов (цель 80%).`, 'speed']);
    if (untouched > 10) sig.push([untouched > 25 ? 'bad' : 'warn', `${untouched} лидов ждут больше суток без касания.`, 'speed']);

    // 2. Сигналы: только сработавшие, сначала красные, не больше пяти
    sig.sort((a, b) => (a[0] === 'bad' ? 0 : 1) - (b[0] === 'bad' ? 0 : 1));
    const sigLi = ([t, m, go]) => `<li><span class="ki-dot ki-${t}"></span><span>${m}</span>${go ? `<button type="button" class="ki-go" data-go="${go}">Открыть →</button>` : ''}</li>`;
    const sigHtml = `<section class="ki-panel"><div class="ki-ph"><h3>Сигналы</h3><span>${sig.length ? `Требуют внимания: ${sig.length}` : 'Всё в пределах плана'}</span></div>
      <ul class="ki-signals">${sig.slice(0, 5).map(sigLi).join('') || '<li class="ki-muted">Сработавших сигналов нет.</li>'}</ul>
      ${sig.length > 5 ? `<details class="ki-more"><summary>Ещё ${sig.length - 5}</summary><ul class="ki-signals">${sig.slice(5).map(sigLi).join('')}</ul></details>` : ''}</section>`;

    const extra = `<div class="ki-kpis">
      <div class="ki-kpi"><div class="ki-lbl">Средний чек новых</div><div class="ki-val num">${avgCheck ? fmt(avgCheck) : '—'}</div><div class="ki-kpi-sub">${planCheck ? `план ${fmt(planCheck)} сум` : 'сум'}</div></div>
      <div class="ki-kpi"><div class="ki-lbl">Доля предоплат</div><div class="ki-val num">${paidM.length ? Math.round(prepayShare * 100) + '%' : '—'}</div><div class="ki-kpi-sub">оплатили не полностью — деньги ещё дособрать</div></div>
    </div>`;

    el.innerHTML = `<p class="ki-sub">${p.label}</p>${money}${sigHtml}${funnelHtml(plan)}${extra}${renew}${mkt}${speed}`;
    el.querySelectorAll('[data-go]').forEach(b => b.addEventListener('click', () => window.V2Go && window.V2Go(b.dataset.go)));
    // Вход куратора Activation (окно из основного дашборда); после входа — перерисовка.
    el.querySelectorAll('[data-actlogin]').forEach(b => b.addEventListener('click', () => {
      if (typeof showGroupsLogin !== 'function') return;
      showGroupsLogin();
      let n = 0;
      const t = setInterval(() => {
        const M3 = window.V2Mkt;
        if (M3 && M3.hasAct()) { clearInterval(t); M3.loadAct().then(() => { if (el.isConnected) renderHome(el, d); }); }
        else if (++n > 300) clearInterval(t);
      }, 1000);
    }));
  }

  function renderChannels(el, d) {
    const r = periodRange();
    const rows = cohorts(d.deals, d.spend, r);
    const mc = rangeChannels(d, r.from, r.to);
    el.innerHTML = `
      <div class="ki-bar-row">${periodBar()}</div>
      ${d.spend ? '' : noSpend}
      <div class="ki-chs ki-chs-wide">${channelCards(mc, !!d.spend)}</div>
      <div class="ki-row">
        <section class="ki-panel"><div class="ki-ph"><h3>Стоимость лида по неделям</h3><span>CPL, $</span></div>${legend}<div class="ki-chart">${cplChart(rows) || '<p class="ki-note">Нет данных о расходе.</p>'}</div></section>
        <section class="ki-panel"><div class="ki-ph"><h3>CAC по закрытым когортам</h3><span>Расход недели ÷ оплаты когорты</span></div><div class="ki-cacs">${cacBars(rows)}</div>
          <p class="ki-note">Пунктир — пороги $25 и $45. Выше $45 клиент не окупает первую оплату (чек 800 000 сум ≈ $67).</p></section>
      </div>
      <section class="ki-panel"><div class="ki-ph"><h3>Когорты по неделе создания лида</h3><span>Лиды, оплаты за всё время и CAC по каждой неделе</span></div>${cohortTable(rows)}</section>
      ${window.V2Mkt ? window.V2Mkt.romiHtml(d, r) : ''}`;
    bindPeriod(el, () => renderChannels(el, d));
  }

  // ---------- Размещения в Telegram ----------
  // Хранятся в plans.json (SHARED_PLANS.tgPlacements) тем же механизмом, что
  // планы: сохраняет тот, у кого настроен GitHub-токен (⚙️).
  // Формат меток повторяет уже работающие ссылки: source=telegram,
  // medium=banner, campaign=«16october» (день запуска). Канал — в utm_content,
  // чтобы различать размещения, запущенные в один день.
  const MONTHS_EN = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'];
  const TR = { а: 'a', б: 'b', в: 'v', г: 'g', д: 'd', е: 'e', ё: 'e', ж: 'zh', з: 'z', и: 'i', й: 'y', к: 'k', л: 'l', м: 'm', н: 'n', о: 'o', п: 'p', р: 'r', с: 's', т: 't', у: 'u', ф: 'f', х: 'h', ц: 'ts', ч: 'ch', ш: 'sh', щ: 'sch', ъ: '', ы: 'y', ь: '', э: 'e', ю: 'yu', я: 'ya', ў: 'o', қ: 'q', ғ: 'g', ҳ: 'h' };
  const slug = s => String(s || '').toLowerCase().replace(/^@/, '').split('').map(c => TR[c] ?? c).join('')
    .replace(/https?:\/\/(t\.me|telegram\.me)\//, '').replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '').slice(0, 40);
  const campaignFor = iso => { const d = iso ? new Date(iso + 'T00:00:00') : null; return d && !isNaN(d) ? `${d.getDate()}${MONTHS_EN[d.getMonth()]}` : ''; };
  const todayIso = () => { const d = new Date(); return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`; };
  const isoToDm = iso => iso ? iso.slice(8, 10) + '.' + iso.slice(5, 7) + '.' + iso.slice(0, 4) : '';
  const lsGet = (k, def) => { try { return localStorage.getItem(k) || def; } catch (e) { return def; } };
  const lsSet = (k, v) => { try { localStorage.setItem(k, v); } catch (e) {} };

  function buildUrl(base, utm) {
    let u;
    try { u = new URL(base.trim()); } catch (e) { return ''; }
    Object.entries(utm).forEach(([k, v]) => { if (v) u.searchParams.set(k, v); else u.searchParams.delete(k); });
    return u.toString();
  }
  const placements = () => (typeof SHARED_PLANS !== 'undefined' && Array.isArray(SHARED_PLANS.tgPlacements)) ? SHARED_PLANS.tgPlacements : [];

  function placementStats(p, deals) {
    const camp = String(p.utm.utm_campaign || '').toLowerCase();
    const cont = String(p.utm.utm_content || '').toLowerCase();
    const ds = deals.filter(d => d.src === String(p.utm.utm_source || 'telegram').toLowerCase()
      && d.campaign === camp && (!cont || d.content === cont));
    const paid = ds.filter(d => d.paidAt).length;
    const cost = Number(p.cost) || 0;
    const revenue = ds.reduce((a, d) => a + (d.revenue || 0) + (d.renewalRevenue || 0), 0);
    return {
      leads: ds.length, qual: ds.filter(d => d.qualAt).length, scheduled: ds.filter(d => d.scheduledAt).length,
      attended: ds.filter(d => d.attendedAt).length, paid, revenue,
      cpl: ds.length ? cost / ds.length : null, cac: paid ? cost / paid : null,
      romi: cost ? (revenue - cost) / cost * 100 : null,
    };
  }
  const sum = v => v == null ? '—' : fmt(v) + ' сум';
  // Пороги CAC в сумах при чеке 800 000: до 300 000 хорошо, до 560 000 терпимо.
  const cacToneUzs = v => v <= 300000 ? 'good' : v <= 560000 ? 'warn' : 'bad';

  async function persistPlacements(mutate, check) {
    mutate();
    if (typeof saveSharedPlans !== 'function') return false;
    let ok = await saveSharedPlans();
    // При конфликте версий saveSharedPlans перечитывает plans.json и может
    // затереть наше изменение — применяем ещё раз.
    if (ok && check && !check()) { mutate(); ok = await saveSharedPlans(); }
    return ok;
  }

  function renderTg(el, d) {
    const landing = lsGet('sk-tg-landing', '');
    // Период — по дате размещения (есть вариант «Всё время»).
    const r = periodRange();
    const all = placements();
    const list = all.filter(p => { const t = isoDay(p.start); return !t || (t >= r.from && t < r.to); })
      .sort((a, b) => (b.start || '').localeCompare(a.start || ''));
    const tot = { cost: 0, leads: 0, qual: 0, scheduled: 0, attended: 0, paid: 0, revenue: 0 };
    const rowsHtml = list.map(p => {
      const s = placementStats(p, d.deals);
      tot.cost += Number(p.cost) || 0; ['leads', 'qual', 'scheduled', 'attended', 'paid', 'revenue'].forEach(k => { tot[k] += s[k] || 0; });
      return `<tr data-id="${esc(p.id)}">
        <td>${esc(isoToDm(p.start))}</td>
        <td class="ki-wk">${esc(p.channel)}</td>
        <td>${sum(Number(p.cost) || 0)}</td>
        <td>${s.leads}</td><td>${s.qual}</td><td>${s.scheduled}</td><td>${s.attended}</td><td>${s.paid}</td>
        <td>${s.revenue ? (s.revenue / 1e6).toFixed(1).replace('.', ',') + ' млн' : '—'}</td>
        <td>${sum(s.cpl)}</td>
        <td>${s.cac != null ? `<span class="ki-cac ki-${cacToneUzs(s.cac)}">${sum(s.cac)}</span>` : '—'}</td>
        <td>${s.romi != null && s.paid ? `<span class="ki-cac ki-${s.romi >= 100 ? 'good' : s.romi >= 0 ? 'warn' : 'bad'}">${Math.round(s.romi)}%</span>` : '—'}</td>
        <td class="ki-act"><button type="button" class="ki-btn" data-copy="${esc(p.url)}">Ссылка</button><button type="button" class="ki-btn ki-btn-del" data-del="${esc(p.id)}">Удалить</button></td>
      </tr>`;
    }).join('');
    el.innerHTML = `
      <p class="ki-sub">Добавляйте каждое размещение в канале: ссылка с UTM соберётся сама, а лиды и оплаты по ней посчитаются из AmoCRM.</p>
      <div class="ki-row ki-row-form">
        <section class="ki-panel">
          <div class="ki-ph"><h3>Новое размещение</h3><span>Поля со звёздочкой обязательны</span></div>
          <form class="ki-form" id="tgForm" novalidate>
            <label class="ki-f ki-f2"><span>Канал *</span><input id="tgChannel" required placeholder="@mamy_tashkent или название канала" autocomplete="off"></label>
            <label class="ki-f"><span>Дата размещения *</span><input id="tgStart" type="date" required value="${todayIso()}"></label>
            <label class="ki-f"><span>Стоимость, сум *</span><input id="tgCost" type="text" data-num inputmode="numeric" required placeholder="500 000"></label>
            <label class="ki-f ki-f2"><span>Посадочная страница *</span><input id="tgLanding" type="url" required placeholder="https://superkid.uz/..." value="${esc(landing)}"></label>
            <fieldset class="ki-utm ki-f2">
              <legend>UTM-метки</legend>
              <label class="ki-f"><span>utm_source</span><input id="tgSource" value="telegram"></label>
              <label class="ki-f"><span>utm_medium</span><input id="tgMedium" value="banner" list="tgMediums"><datalist id="tgMediums"><option value="banner"><option value="post"><option value="stories"></datalist></label>
              <label class="ki-f"><span>utm_campaign</span><input id="tgCampaign"></label>
              <label class="ki-f"><span>utm_content</span><input id="tgContent"></label>
              <label class="ki-f"><span>utm_term</span><input id="tgTerm" placeholder="необязательно"></label>
            </fieldset>
            <div class="ki-f ki-f2">
              <span>Ссылка для размещения</span>
              <div class="ki-link"><code id="tgUrl">Заполните посадочную страницу</code><button type="button" class="ki-btn" id="tgCopy">Скопировать</button></div>
            </div>
            <div class="ki-f2 ki-actions"><button type="submit" class="ki-primary">Сохранить размещение</button><span id="tgMsg" class="ki-msg" role="status"></span></div>
          </form>
        </section>
        <section class="ki-panel ki-help">
          <div class="ki-ph"><h3>Как считается</h3></div>
          <ul>
            <li>Лид относится к размещению, если в AmoCRM у него совпали <b>utm_campaign</b> и <b>utm_content</b>.</li>
            <li>Для старых размещений без канала в ссылке (например, <code>1october</code>) очистите поле utm_content — тогда учтутся все лиды кампании.</li>
            <li>CPL = стоимость ÷ лиды, CAC = стоимость ÷ оплаты. Оплаты учитываются за всё время.</li>
            <li>Общий расход Telegram в «Каналах и CAC» пока берётся из РнП.</li>
          </ul>
        </section>
      </div>
      <section class="ki-panel">
        <div class="ki-ph"><h3>Размещения</h3><span>${list.length ? list.length + ' шт. · запущены ' + r.label : all.length ? 'В этом периоде размещений нет' : 'Пока пусто'}</span></div>
        <div class="ki-bar-row ki-bar-in">${periodBar([['all', 'Всё время']])}</div>
        ${list.length ? `<div class="ki-tbl"><table>
          <thead><tr><th>Дата</th><th>Канал</th><th>Стоимость</th><th>Лиды</th><th>Квал</th><th>Назн. ОУ</th><th>Пришли</th><th>Оплаты</th><th>Выручка</th><th>CPL</th><th>CAC</th><th>ROMI</th><th></th></tr></thead>
          <tbody>${rowsHtml}
          <tr class="ki-tr-tot"><td></td><td><b>Итого</b></td><td>${sum(tot.cost)}</td><td>${tot.leads}</td><td>${tot.qual}</td><td>${tot.scheduled}</td><td>${tot.attended}</td><td>${tot.paid}</td>
            <td>${tot.revenue ? (tot.revenue / 1e6).toFixed(1).replace('.', ',') + ' млн' : '—'}</td><td>${tot.leads ? sum(tot.cost / tot.leads) : '—'}</td>
            <td>${tot.paid ? `<span class="ki-cac ki-${cacToneUzs(tot.cost / tot.paid)}">${sum(tot.cost / tot.paid)}</span>` : '—'}</td>
            <td>${tot.cost && tot.paid ? `<span class="ki-cac ki-${tot.revenue >= 2 * tot.cost ? 'good' : tot.revenue >= tot.cost ? 'warn' : 'bad'}">${Math.round((tot.revenue - tot.cost) / tot.cost * 100)}%</span>` : '—'}</td><td></td></tr></tbody></table></div>`
        : all.length ? '<p class="ki-note">Выберите другой период или «Всё время».</p>' : '<p class="ki-note">Добавьте первое размещение в форме выше — оно появится здесь с лидами и оплатами.</p>'}
      </section>`;

    bindPeriod(el, () => renderTg(el, d));
    const $ = id => el.querySelector('#' + id);
    const f = { channel: $('tgChannel'), start: $('tgStart'), cost: $('tgCost'), landing: $('tgLanding'),
      source: $('tgSource'), medium: $('tgMedium'), campaign: $('tgCampaign'), content: $('tgContent'), term: $('tgTerm') };
    const dirty = { campaign: false, content: false };
    f.campaign.addEventListener('input', () => { dirty.campaign = true; update(); });
    f.content.addEventListener('input', () => { dirty.content = true; update(); });
    function utm() {
      return { utm_source: f.source.value.trim(), utm_medium: f.medium.value.trim(), utm_campaign: f.campaign.value.trim(),
        utm_content: f.content.value.trim(), utm_term: f.term.value.trim() };
    }
    function update() {
      if (!dirty.campaign) f.campaign.value = campaignFor(f.start.value);
      if (!dirty.content) f.content.value = slug(f.channel.value);
      const url = f.landing.value.trim() ? buildUrl(f.landing.value, utm()) : '';
      $('tgUrl').textContent = url || (f.landing.value.trim() ? 'Проверьте адрес: он должен начинаться с https://' : 'Заполните посадочную страницу');
      $('tgUrl').dataset.url = url;
    }
    ['channel', 'start', 'landing', 'source', 'medium', 'term'].forEach(k => f[k].addEventListener('input', update));
    update();

    function copy(text, btn) {
      if (!text) return;
      const done = () => { const t = btn.textContent; btn.textContent = 'Скопировано'; setTimeout(() => { btn.textContent = t; }, 1400); };
      if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(text).then(done, () => window.prompt('Скопируйте ссылку:', text));
      else window.prompt('Скопируйте ссылку:', text);
    }
    $('tgCopy').addEventListener('click', e => copy($('tgUrl').dataset.url, e.currentTarget));

    $('tgForm').addEventListener('submit', async e => {
      e.preventDefault();
      const msg = $('tgMsg');
      const url = $('tgUrl').dataset.url;
      if (!f.channel.value.trim() || !f.start.value || !(parseNum(f.cost.value) >= 0) || !url) {
        msg.textContent = 'Заполните канал, дату, стоимость и посадочную страницу.';
        msg.className = 'ki-msg ki-msg-bad';
        return;
      }
      lsSet('sk-tg-landing', f.landing.value.trim());
      const p = {
        id: 'tg' + Date.now().toString(36), channel: f.channel.value.trim(), start: f.start.value,
        cost: parseNum(f.cost.value), currency: 'uzs', landing: f.landing.value.trim(), utm: utm(), url,
        createdAt: new Date().toISOString(),
      };
      msg.textContent = 'Сохраняю…'; msg.className = 'ki-msg';
      const ok = await persistPlacements(
        () => { if (!Array.isArray(SHARED_PLANS.tgPlacements)) SHARED_PLANS.tgPlacements = []; if (!SHARED_PLANS.tgPlacements.some(x => x.id === p.id)) SHARED_PLANS.tgPlacements.push(p); },
        () => SHARED_PLANS.tgPlacements && SHARED_PLANS.tgPlacements.some(x => x.id === p.id));
      if (!ok) {
        SHARED_PLANS.tgPlacements = placements().filter(x => x.id !== p.id);
        msg.textContent = 'Не сохранилось. Нужен GitHub-токен: кнопка ⚙️ в шапке.';
        msg.className = 'ki-msg ki-msg-bad';
        return;
      }
      renderTg(el, d);
      const m2 = el.querySelector('#tgMsg');
      if (m2) { m2.textContent = 'Размещение сохранено, ссылка в списке ниже.'; m2.className = 'ki-msg ki-msg-good'; }
    });

    el.querySelectorAll('[data-copy]').forEach(b => b.addEventListener('click', () => copy(b.dataset.copy, b)));
    el.querySelectorAll('[data-del]').forEach(b => b.addEventListener('click', async () => {
      if (b.dataset.armed !== '1') { b.dataset.armed = '1'; b.textContent = 'Точно удалить?'; return; }
      const id = b.dataset.del;
      const before = placements().slice();
      b.textContent = 'Удаляю…';
      const ok = await persistPlacements(
        () => { SHARED_PLANS.tgPlacements = placements().filter(x => x.id !== id); },
        () => !placements().some(x => x.id === id));
      if (!ok) { SHARED_PLANS.tgPlacements = before; b.textContent = 'Не удалилось'; return; }
      renderTg(el, d);
    }));
  }

  // Воронка берёт цифры у основного кода (computeStats по ALL_DEALS), а он
  // грузит сделки уже после входа. Ждём их, иначе «Главная» покажет нули.
  function waitMainData(timeoutMs) {
    return new Promise(resolve => {
      const t0 = Date.now();
      (function check() {
        const ready = typeof ALL_DEALS !== 'undefined' && ALL_DEALS.length > 0;
        if (ready || Date.now() - t0 > timeoutMs) return resolve(ready);
        setTimeout(check, 300);
      })();
    });
  }

  async function render(page, el) {
    el.innerHTML = '<div class="ki-loading">Загружаю сделки и расходы…</div>';
    try {
      const [d] = await Promise.all([loadData(), page === 'home' ? waitMainData(30000) : null]);
      if (page === 'home') renderHome(el, d);
      else if (page === 'tg') renderTg(el, d);
      else if (page === 'channels') renderChannels(el, d);
      else if (window.V2Mkt && window.V2Mkt.pages[page]) await window.V2Mkt.pages[page](el, d);
      else el.innerHTML = '<div class="ki-notice">Страница не найдена.</div>';
    } catch (e) {
      dataPromise = null;
      el.innerHTML = `<div class="ki-notice">Не удалось загрузить данные: ${esc(e.message)}. Нажмите «Обновить».</div>`;
    }
  }

  window.V2Insights = {
    render, loadData, resetData,
    renewalBase, util: { periodGet, periodRange, periodBar, bindPeriod, spendFor, rangeChannels, fmtNumStr, parseNum, numVal, esc, fmt, usd, pct, pd, monday, key, dm, addDays, ym, tone, cacTone, MONTHS_RU, META_FROM, monthPace, placements, placementStats },
  };
})();
