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

  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const fmt = n => Math.round(n).toLocaleString('ru-RU');
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
    if (!dataPromise) dataPromise = Promise.all([loadDeals(), loadSpend()]).then(([deals, spend]) => ({ deals, spend }));
    return dataPromise;
  }

  async function loadDeals() {
    const cols = 'id,date_created,date_prepay,closed_at,status,budget,utm_source,utm_medium,utm_campaign,utm_content,tags';
    const out = [];
    for (let off = 0; ; off += 1000) {
      const r = await fetch(`${SUPABASE_URL}/rest/v1/deals?select=${cols}&order=id.asc&limit=1000&offset=${off}`, {
        headers: { apikey: SUPABASE_ANON_KEY, Authorization: 'Bearer ' + SUPABASE_ANON_KEY },
      });
      if (!r.ok) throw new Error('Supabase ' + r.status);
      const b = await r.json();
      out.push(...b);
      if (b.length < 1000) break;
    }
    return out.map(d => {
      const st = String(d.status || '').toLowerCase();
      const src = String(d.utm_source || '').toLowerCase();
      const ch = src === 'telegram' ? 'tg' : /\bfb\d/.test(d.tags || '') ? 'meta' : 'other';
      const created = pd(d.date_created);
      const paidAt = pd(d.date_prepay) || (st.includes('успешно реализовано') ? pd(d.closed_at) : null);
      return { ch, created, paidAt, budget: Number(d.budget) || 0,
        src, campaign: String(d.utm_campaign || '').trim().toLowerCase(), content: String(d.utm_content || '').trim().toLowerCase() };
    }).filter(d => d.created);
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
    // Берём большее из кабинета и РнП: подключён не каждый кабинет (в сентябре
    // реклама шла и с другого), а свежую неделю в РнП ещё не внесли.
    // Когда подключат все кабинеты, кабинет всегда будет ≥ РнП.
    if (meta) {
      const put = (obj, k, v) => { const o = (obj[k] ??= { meta: 0, tg: 0 }); o.meta = Math.max(o.meta || 0, v); };
      Object.entries(meta.weeks || {}).forEach(([w, v]) => put(base.weeks, w, v));
      Object.entries(meta.months || {}).forEach(([m, v]) => put(base.months, m, v));
    }
    return { weeks: base.weeks, month: base.months[ym(new Date())] || null, metaSource: meta ? 'кабинет' : 'РнП' };
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
  function cohorts(deals, spend) {
    const today = new Date(); today.setHours(0, 0, 0, 0);
    const thisMon = monday(today);
    const list = [];
    for (let i = WEEKS_BACK - 1; i >= 0; i--) {
      const mon = addDays(thisMon, -7 * i);
      const end = addDays(mon, 7);
      const age = Math.round((today - mon) / 864e5);
      const note = i === 0 ? 'идёт неделя' : age < 14 ? 'дозревает' : '';
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

  function monthPace() {
    const now = new Date();
    const dim = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
    return { day: now.getDate(), dim, pace: now.getDate() / dim, label: `${MONTHS_RU[now.getMonth()]} ${now.getFullYear()} · прошло ${now.getDate()} из ${dim} дней` };
  }

  // ---------- Отрисовка ----------
  const CH = { meta: { name: 'Meta', sub: 'лид-форма' }, tg: { name: 'Telegram', sub: 'баннер' } };

  function funnelHtml() {
    const p = monthPace();
    const now = new Date();
    const from = new Date(now.getFullYear(), now.getMonth(), 1);
    const to = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59);
    let st = null;
    try { st = computeStats('all', from, to); } catch (e) { return ''; }
    const plan = (typeof SHARED_PLANS !== 'undefined' && SHARED_PLANS.salesPlans && SHARED_PLANS.salesPlans[ym(now)]) || {};
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
        <div class="ki-big num">${c.cac != null ? '$' + Math.round(c.cac) : '—'}<small>CAC за месяц</small></div>
        <dl class="ki-kv num">
          <dt>Расход</dt><dd>${c.spend != null ? usd(c.spend) : '—'}</dd>
          <dt>Лиды · CPL</dt><dd>${fmt(c.leads)}${c.cpl != null ? ' · ' + usd(c.cpl, 2) : ''}</dd>
          <dt>Оплат в месяце</dt><dd>${c.paid}</dd>
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

  function renderHome(el, d) {
    const rows = cohorts(d.deals, d.spend);
    const mc = monthChannels(d.deals, d.spend);
    el.innerHTML = `
      <p class="ki-sub">${monthPace().label}</p>
      ${funnelHtml()}
      <div class="ki-row">
        <section class="ki-panel"><div class="ki-ph"><h3>Лиды по неделям и каналам</h3><span>Неделя создания лида</span></div>${legend}<div class="ki-chart">${leadsChart(rows)}</div></section>
        <section class="ki-panel"><div class="ki-ph"><h3>Каналы · ${MONTHS_RU[new Date().getMonth()].toLowerCase()}</h3><span>${d.spend && d.spend.metaSource === 'кабинет' ? 'Meta — из кабинета, Telegram — из РнП' : 'Расход из РнП'}</span></div><div class="ki-chs">${channelCards(mc, !!d.spend)}</div></section>
      </div>
      ${d.spend ? '' : noSpend}
      <section class="ki-panel"><div class="ki-ph"><h3>Когорты по неделе создания лида</h3><span>Оплаты за всё время, CAC = расход недели ÷ оплаты когорты</span></div>${cohortTable(rows)}
        <p class="ki-note">От лида до оплаты обычно 3–8 дней. Когорты двух последних недель ещё дозревают, их CAC снизится.</p></section>`;
  }

  function renderChannels(el, d) {
    const rows = cohorts(d.deals, d.spend);
    const mc = monthChannels(d.deals, d.spend);
    el.innerHTML = `
      <p class="ki-sub">${monthPace().label}</p>
      ${d.spend ? '' : noSpend}
      <div class="ki-chs ki-chs-wide">${channelCards(mc, !!d.spend)}</div>
      <div class="ki-row">
        <section class="ki-panel"><div class="ki-ph"><h3>Стоимость лида по неделям</h3><span>CPL, $</span></div>${legend}<div class="ki-chart">${cplChart(rows) || '<p class="ki-note">Нет данных о расходе.</p>'}</div></section>
        <section class="ki-panel"><div class="ki-ph"><h3>CAC по закрытым когортам</h3><span>Расход недели ÷ оплаты когорты</span></div><div class="ki-cacs">${cacBars(rows)}</div>
          <p class="ki-note">Пунктир — пороги $25 и $45. Выше $45 клиент не окупает первую оплату (чек 800 000 сум ≈ $65).</p></section>
      </div>
      <section class="ki-panel"><div class="ki-ph"><h3>Когорты по неделе создания лида</h3><span>Лиды, оплаты за всё время и CAC по каждой неделе</span></div>${cohortTable(rows)}</section>`;
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
    return { leads: ds.length, paid, cpl: ds.length ? cost / ds.length : null, cac: paid ? cost / paid : null };
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
    const list = placements().slice().sort((a, b) => (b.start || '').localeCompare(a.start || ''));
    const rowsHtml = list.map(p => {
      const s = placementStats(p, d.deals);
      return `<tr data-id="${esc(p.id)}">
        <td>${esc(isoToDm(p.start))}</td>
        <td class="ki-wk">${esc(p.channel)}</td>
        <td>${sum(Number(p.cost) || 0)}</td>
        <td>${s.leads}</td><td>${s.paid}</td>
        <td>${sum(s.cpl)}</td>
        <td>${s.cac != null ? `<span class="ki-cac ki-${cacToneUzs(s.cac)}">${sum(s.cac)}</span>` : '—'}</td>
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
            <label class="ki-f"><span>Стоимость, сум *</span><input id="tgCost" type="number" min="0" step="1000" inputmode="numeric" required placeholder="500 000"></label>
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
        <div class="ki-ph"><h3>Размещения</h3><span>${list.length ? list.length + ' шт.' : 'Пока пусто'}</span></div>
        ${list.length ? `<div class="ki-tbl"><table>
          <thead><tr><th>Дата</th><th>Канал</th><th>Стоимость</th><th>Лиды</th><th>Оплаты</th><th>CPL</th><th>CAC</th><th></th></tr></thead>
          <tbody>${rowsHtml}</tbody></table></div>`
        : '<p class="ki-note">Добавьте первое размещение в форме выше — оно появится здесь с лидами и оплатами.</p>'}
      </section>`;

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
      if (!f.channel.value.trim() || !f.start.value || f.cost.value === '' || !url) {
        msg.textContent = 'Заполните канал, дату, стоимость и посадочную страницу.';
        msg.className = 'ki-msg ki-msg-bad';
        return;
      }
      lsSet('sk-tg-landing', f.landing.value.trim());
      const p = {
        id: 'tg' + Date.now().toString(36), channel: f.channel.value.trim(), start: f.start.value,
        cost: Number(f.cost.value), currency: 'uzs', landing: f.landing.value.trim(), utm: utm(), url,
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
      else renderChannels(el, d);
    } catch (e) {
      dataPromise = null;
      el.innerHTML = `<div class="ki-notice">Не удалось загрузить данные: ${esc(e.message)}. Нажмите «Обновить».</div>`;
    }
  }

  window.V2Insights = { render };
})();
