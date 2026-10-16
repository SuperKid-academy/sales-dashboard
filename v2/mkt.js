// =====================================================================
// Superkid Dashboard v2 — маркетинговые страницы и аналитика обработки лидов.
//
// Маркетинг: Сводка, План, Реклама Meta (кампании / группы / объявления +
// выгорание креативов), Качество лидов, блок окупаемости для «Когорт».
// Продажи: Скорость обработки, Дни и часы.
//
// Данные берёт у V2Insights.loadData() (сделки, продления, расходы) и у
// сервера (/api/meta/insights — статистика кабинета, токен на сервере).
// План маркетинга хранится в plans.json → SHARED_PLANS.mktPlan[YYYY-MM].
// =====================================================================
(function () {
  const U = () => window.V2Insights.util;
  const DEFAULT_RATE = 12000; // сум за $, если в плане месяца курс не задан
  const WD = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'];

  // ---------- общие ----------
  const nowYm = () => U().ym(new Date());
  function mktPlan(ymKey) {
    const all = (typeof SHARED_PLANS !== 'undefined' && SHARED_PLANS.mktPlan) || {};
    return all[ymKey || nowYm()] || {};
  }
  const rateOf = ymKey => Number(mktPlan(ymKey).rate) || DEFAULT_RATE;
  const mln = v => (v / 1e6).toFixed(1).replace('.', ',') + ' млн';
  const pctTxt = (a, b) => b ? Math.round(a / b * 100) + '%' : '—';
  const toneRomi = r => r >= 100 ? 'good' : r >= 0 ? 'warn' : 'bad';
  const monthRange = (d = new Date()) => [new Date(d.getFullYear(), d.getMonth(), 1), new Date(d.getFullYear(), d.getMonth() + 1, 1)];
  const inR = (dt, a, b) => dt && dt >= a && dt < b;
  const seg = (id, items, cur) => `<div class="ki-seg" role="group" id="${id}">${items.map(([v, l]) =>
    `<button type="button" data-v="${v}" class="${v === cur ? 'on' : ''}">${l}</button>`).join('')}</div>`;
  const lsGet = (k, d) => { try { return localStorage.getItem(k) || d; } catch (e) { return d; } };
  const lsSet = (k, v) => { try { localStorage.setItem(k, v); } catch (e) {} };

  // ---------- Meta API через сервер ----------
  function proxyBase() {
    const b = (typeof getAmoEndpoint === 'function' && getAmoEndpoint()) || 'https://amo-proxy-production.up.railway.app';
    return b.replace(/\/+$/, '');
  }
  async function metaInsights(params) {
    const out = [];
    let after = '';
    for (let page = 0; page < 10; page++) {
      const q = new URLSearchParams({ ...params, limit: '500' });
      if (after) q.set('after', after);
      const r = await fetch(`${proxyBase()}/api/meta/insights?${q}`);
      const j = await r.json();
      if (j.error) throw new Error(j.error.message || 'Meta API');
      out.push(...(j.data || []));
      const next = j.paging && j.paging.cursors && j.paging.cursors.after;
      if (!next || !(j.data || []).length || (j.data || []).length < 500) break;
      after = next;
    }
    return out;
  }
  const metaLeads = actions => {
    const a = actions || [];
    const g = a.find(x => x.action_type === 'onsite_conversion.lead_grouped') || a.find(x => x.action_type === 'lead');
    return g ? Number(g.value) || 0 : 0;
  };
  const META_PERIODS = [['this_month', 'Этот месяц'], ['last_7d', '7 дней'], ['last_30d', '30 дней'], ['last_month', 'Прошлый месяц']];

  // Выгорание: по каждому объявлению сравниваем последние 3 дня с 7 днями до них.
  let fatigueCache = null;
  async function loadFatigue() {
    if (fatigueCache && Date.now() - fatigueCache.t < 10 * 60 * 1000) return fatigueCache.v;
    const rows = await metaInsights({ level: 'ad', time_increment: '1', date_preset: 'last_14d',
      fields: 'ad_id,ad_name,adset_name,spend,impressions,inline_link_clicks,actions' });
    const byAd = new Map();
    rows.forEach(r => {
      const a = byAd.get(r.ad_id) || { name: r.ad_name, adset: r.adset_name, days: [] };
      a.days.push({ d: r.date_start, spend: +r.spend || 0, imp: +r.impressions || 0, clicks: +r.inline_link_clicks || 0, leads: metaLeads(r.actions) });
      byAd.set(r.ad_id, a);
    });
    const sumD = ds => ds.reduce((a, x) => ({ spend: a.spend + x.spend, imp: a.imp + x.imp, clicks: a.clicks + x.clicks, leads: a.leads + x.leads }), { spend: 0, imp: 0, clicks: 0, leads: 0 });
    const res = [];
    byAd.forEach((a, id) => {
      const days = a.days.sort((x, y) => x.d.localeCompare(y.d)).filter(x => x.spend > 0);
      if (days.length < 5) return;
      const recent = sumD(days.slice(-3)), base = sumD(days.slice(-10, -3));
      if (recent.spend < 8 || base.spend < 8 || !base.leads) return;
      const cplB = base.spend / base.leads;
      const cplR = recent.leads ? recent.spend / recent.leads : Infinity;
      const ctrB = base.imp ? base.clicks / base.imp : 0, ctrR = recent.imp ? recent.clicks / recent.imp : 0;
      const cplUp = cplR / cplB - 1, ctrDown = ctrB ? 1 - ctrR / ctrB : 0;
      if (cplUp >= 0.4 || ctrDown >= 0.3) res.push({ id, name: a.name, adset: a.adset, cplB, cplR, cplUp, ctrDown, spendR: recent.spend });
    });
    res.sort((x, y) => y.spendR - x.spendR);
    fatigueCache = { t: Date.now(), v: res };
    return res;
  }

  // ---------- Сводка маркетинга ----------
  function monthChannelStats(d, ymKey) {
    const [from, to] = ymKey ? [new Date(ymKey + '-01T00:00:00'), null] : monthRange();
    const end = to || new Date(from.getFullYear(), from.getMonth() + 1, 1);
    const k = U().ym(from);
    const spendM = (d.spend && d.spend.months) ? d.spend.months[k] : (d.spend && d.spend.month) || null;
    const rate = rateOf(k);
    const out = {};
    ['meta', 'tg', 'other'].forEach(ch => {
      const ds = d.deals.filter(x => x.ch === ch);
      const leads = ds.filter(x => inR(x.created, from, end)).length;
      const paidDs = ds.filter(x => inR(x.paidAt, from, end));
      const revenue = paidDs.reduce((a, x) => a + x.revenue, 0);
      const spend = ch === 'other' ? 0 : (spendM ? (spendM[ch] || 0) : 0);
      out[ch] = { leads, paid: paidDs.length, revenue, spend,
        cpl: spend && leads ? spend / leads : null, cac: spend && paidDs.length ? spend / paidDs.length : null,
        romi: spend ? (revenue / rate - spend) / spend * 100 : null };
    });
    return out;
  }

  async function renderSummary(el, d) {
    const u = U(), p = u.monthPace(), plan = mktPlan(), rate = rateOf();
    const st = monthChannelStats(d);
    const spend = st.meta.spend + st.tg.spend;
    const spendPlan = (+plan.metaBudget || 0) + (+plan.tgBudget || 0);
    const leads = st.meta.leads + st.tg.leads + st.other.leads;
    const leadsPlan = (+plan.metaLeads || 0) + (+plan.tgLeads || 0);
    const paid = st.meta.paid + st.tg.paid;
    const cac = paid ? spend / paid : null;
    const cplPaid = (st.meta.leads + st.tg.leads) ? spend / (st.meta.leads + st.tg.leads) : null;
    const revenue = st.meta.revenue + st.tg.revenue;
    const romi = spend ? (revenue / rate - spend) / spend * 100 : null;
    const kpi = (lbl, val, sub, tone) => `<div class="ki-kpi"><div class="ki-lbl">${lbl}</div><div class="ki-val num">${val}</div><div class="ki-kpi-sub">${tone ? `<span class="ki-chip ki-${tone}">${sub}</span>` : sub}</div></div>`;
    const paceTone = (f, pl) => pl ? u.tone((f / pl) / p.pace) : null;
    const cplTarget = leadsPlan ? spendPlan / leadsPlan : null;

    const chRow = (ch, name) => {
      const c = st[ch];
      return `<tr><td><span class="ki-chan"><i class="ki-sw" style="background:var(--${ch === 'other' ? 'border' : ch})"></i>${name}</span></td>
        <td>${c.spend ? u.usd(c.spend) : '—'}</td><td>${c.leads}</td><td>${c.cpl != null ? u.usd(c.cpl, 2) : '—'}</td>
        <td>${c.paid}</td><td>${c.cac != null ? `<span class="ki-cac ki-${u.cacTone(c.cac)}">$${Math.round(c.cac)}</span>` : '—'}</td>
        <td>${c.revenue ? mln(c.revenue) : '—'}</td>
        <td>${c.romi != null && c.paid ? `<span class="ki-cac ki-${toneRomi(c.romi)}">${Math.round(c.romi)}%</span>` : '—'}</td></tr>`;
    };

    el.innerHTML = `
      <p class="ki-sub">${p.label} · курс ${u.fmt(rate)} сум за $ (меняется в «Плане»)</p>
      <div class="ki-kpis">
        ${kpi('Расход', u.usd(spend, 0), spendPlan ? `${pctTxt(spend, spendPlan)} плана · ${Math.round((spend / spendPlan) / p.pace * 100)}% от темпа` : 'план не задан', spendPlan ? (Math.abs(spend / spendPlan / p.pace - 1) <= 0.15 ? 'good' : 'warn') : null)}
        ${kpi('Лиды', u.fmt(leads), leadsPlan ? `${pctTxt(leads, leadsPlan)} плана · ${Math.round((leads / leadsPlan) / p.pace * 100)}% от темпа` : 'план не задан', paceTone(leads, leadsPlan))}
        ${kpi('CPL (Meta + TG)', cplPaid != null ? u.usd(cplPaid, 2) : '—', cplTarget ? `цель ${u.usd(cplTarget, 2)}` : 'цель не задана', cplTarget && cplPaid ? u.tone(cplTarget / cplPaid) : null)}
        ${kpi('CAC', cac != null ? '$' + Math.round(cac) : '—', plan.cacTarget ? `цель $${plan.cacTarget}` : `оплат: ${paid}`, plan.cacTarget && cac ? u.tone(plan.cacTarget / cac) : null)}
        ${kpi('ROMI первой продажи', romi != null ? Math.round(romi) + '%' : '—', `выручка ${mln(revenue)}`, romi != null ? toneRomi(romi) : null)}
      </div>
      <section class="ki-panel"><div class="ki-ph"><h3>Сигналы</h3><span>Что требует внимания сейчас</span></div><ul class="ki-signals" id="mkSignals"><li class="ki-muted">Проверяю креативы…</li></ul></section>
      <section class="ki-panel"><div class="ki-ph"><h3>Каналы за месяц</h3><span>Оплаты и выручка — сделки, оплаченные в этом месяце</span></div>
        <div class="ki-tbl"><table><thead><tr><th>Канал</th><th>Расход</th><th>Лиды</th><th>CPL</th><th>Оплаты</th><th>CAC</th><th>Выручка</th><th>ROMI</th></tr></thead>
        <tbody>${chRow('meta', 'Meta')}${chRow('tg', 'Telegram')}${chRow('other', 'Без источника и прочее')}</tbody></table></div></section>`;

    // Сигналы
    const sig = [];
    const [from] = monthRange();
    const monthDeals = d.deals.filter(x => inR(x.created, from, new Date(2100, 0)));
    const noSrc = monthDeals.filter(x => x.ch === 'other').length;
    if (monthDeals.length && noSrc / monthDeals.length > 0.05) sig.push(['warn', `${Math.round(noSrc / monthDeals.length * 100)}% лидов месяца без источника (${noSrc} шт.) — их не видно ни в одном канале.`]);
    u.placements().forEach(pl => {
      const s = u.placementStats(pl, d.deals);
      if (s.leads >= 5 && !s.paid && pl.cost) sig.push(['bad', `TG «${u.esc(pl.channel)}»: ${s.leads} лидов, ни одной оплаты при расходе ${u.fmt(pl.cost)} сум.`]);
    });
    if (plan.cacTarget && st.meta.cac && st.meta.cac > plan.cacTarget * 1.3) sig.push(['bad', `CAC Meta $${Math.round(st.meta.cac)} — на ${Math.round((st.meta.cac / plan.cacTarget - 1) * 100)}% выше цели.`]);
    const render = list => { const box = el.querySelector('#mkSignals'); if (box) box.innerHTML = list.length ? list.map(([t, m]) => `<li><span class="ki-dot ki-${t}"></span>${m}</li>`).join('') : '<li class="ki-muted">Всё в норме — сигналов нет.</li>'; };
    render(sig);
    try {
      const fat = await loadFatigue();
      fat.slice(0, 5).forEach(f => sig.push(['warn', `Креатив «${u.esc(f.name)}» дорожает: ${f.cplR === Infinity ? 'нет лидов за 3 дня' : `лид ${u.usd(f.cplB, 2)} → ${u.usd(f.cplR, 2)}`}${f.ctrDown >= 0.3 ? `, CTR −${Math.round(f.ctrDown * 100)}%` : ''}.`]));
      render(sig);
    } catch (e) { render(sig); }
  }

  // ---------- План маркетинга ----------
  function renderPlan(el) {
    const u = U();
    const now = new Date();
    const months = [-1, 0, 1].map(o => u.ym(new Date(now.getFullYear(), now.getMonth() + o, 1)));
    let cur = lsGet('sk-mkt-plan-month', u.ym(now));
    if (!months.includes(cur)) cur = u.ym(now);
    const fields = [
      ['metaBudget', 'Бюджет Meta, $'], ['metaLeads', 'Лиды Meta'],
      ['tgBudget', 'Бюджет Telegram, $'], ['tgLeads', 'Лиды Telegram'],
      ['cacTarget', 'Целевой CAC, $'], ['rate', 'Курс, сум за $'],
    ];
    const draw = () => {
      const pl = mktPlan(cur);
      const spend = (+pl.metaBudget || 0) + (+pl.tgBudget || 0), leads = (+pl.metaLeads || 0) + (+pl.tgLeads || 0);
      el.innerHTML = `
        <div class="ki-bar-row">${seg('mpMonth', months.map(m => [m, u.MONTHS_RU[+m.slice(5) - 1] + ' ' + m.slice(0, 4)]), cur)}</div>
        <section class="ki-panel"><div class="ki-ph"><h3>План маркетинга</h3><span>Сравнивается с фактом в «Сводке» с учётом темпа месяца</span></div>
          <form class="ki-form" id="mpForm" novalidate>
            ${fields.map(([k, l]) => `<label class="ki-f"><span>${l}</span><input id="mp_${k}" type="number" min="0" step="any" inputmode="decimal" value="${pl[k] != null ? pl[k] : (k === 'rate' ? DEFAULT_RATE : '')}"></label>`).join('')}
            <div class="ki-f2 ki-plan-sum num">Итого: бюджет <b>${u.usd(spend, 0)}</b> · лиды <b>${u.fmt(leads)}</b> · CPL <b>${leads ? u.usd(spend / leads, 2) : '—'}</b></div>
            <div class="ki-f2 ki-actions"><button type="submit" class="ki-primary">Сохранить план</button><span class="ki-msg" id="mpMsg" role="status"></span></div>
          </form></section>`;
      el.querySelector('#mpMonth').addEventListener('click', e => { const b = e.target.closest('button'); if (!b) return; cur = b.dataset.v; lsSet('sk-mkt-plan-month', cur); draw(); });
      el.querySelector('#mpForm').addEventListener('submit', async e => {
        e.preventDefault();
        const msg = el.querySelector('#mpMsg');
        const val = {};
        fields.forEach(([k]) => { const v = el.querySelector('#mp_' + k).value; if (v !== '') val[k] = Number(v); });
        const apply = () => { SHARED_PLANS.mktPlan = SHARED_PLANS.mktPlan || {}; SHARED_PLANS.mktPlan[cur] = val; };
        apply();
        msg.textContent = 'Сохраняю…'; msg.className = 'ki-msg';
        let ok = typeof saveSharedPlans === 'function' && await saveSharedPlans();
        if (ok && JSON.stringify(mktPlan(cur)) !== JSON.stringify(val)) { apply(); ok = await saveSharedPlans(); }
        if (!ok) { msg.textContent = 'Не сохранилось. Нужен GitHub-токен: кнопка ⚙️ в шапке.'; msg.className = 'ki-msg ki-msg-bad'; return; }
        draw();
        const m2 = el.querySelector('#mpMsg'); if (m2) { m2.textContent = 'План сохранён.'; m2.className = 'ki-msg ki-msg-good'; }
      });
    };
    draw();
  }

  // ---------- Реклама Meta ----------
  // Связка «сделка → объявление» приходит с сервера (sync_state.meta_attribution,
  // amo-proxy/metaLeads.js: лиды форм ↔ сделки по телефону).
  async function loadAttribution() {
    try {
      const r = await fetch(`${SUPABASE_URL}/rest/v1/sync_state?key=eq.meta_attribution&select=value`, {
        headers: { apikey: SUPABASE_ANON_KEY, Authorization: 'Bearer ' + SUPABASE_ANON_KEY },
      });
      const rows = r.ok ? await r.json() : [];
      return rows[0] && rows[0].value ? JSON.parse(rows[0].value) : null;
    } catch (e) { return null; }
  }

  async function renderMeta(el, d) {
    const u = U();
    let level = lsGet('sk-meta-level', 'adset'), period = lsGet('sk-meta-period', 'this_month');
    let sortKey = 'spend';
    const LEVELS = [['campaign', 'Кампании'], ['adset', 'Группы объявлений'], ['ad', 'Объявления']];
    el.innerHTML = `
      <div class="ki-bar-row">${seg('mtLevel', LEVELS, level)}${seg('mtPeriod', META_PERIODS, period)}</div>
      <section class="ki-panel"><div id="mtBody"><div class="ki-loading">Загружаю статистику кабинета…</div></div></section>`;
    const body = el.querySelector('#mtBody');
    const nameField = { campaign: 'campaign_name', adset: 'adset_name', ad: 'ad_name' };
    const idField = { campaign: 'campaign_id', adset: 'adset_id', ad: 'ad_id' };
    const attrP = loadAttribution();
    const dealsById = new Map(d.deals.map(x => [String(x.id), x]));

    async function load() {
      body.innerHTML = '<div class="ki-loading">Загружаю статистику кабинета…</div>';
      try {
        const [rows, fat, attr] = await Promise.all([
          metaInsights({ level, date_preset: period,
            fields: 'campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,frequency,inline_link_click_ctr,cpm,actions' }),
          level === 'ad' ? loadFatigue().catch(() => []) : Promise.resolve([]),
          attrP,
        ]);
        // Сделки, созданные в периоде отчёта, сгруппированные по id нужного уровня.
        const ds = rows[0] ? new Date(rows[0].date_start + 'T00:00:00') : null;
        const de = rows[0] ? u.addDays(new Date(rows[0].date_stop + 'T00:00:00'), 1) : null;
        const crm = new Map();
        let attributed = 0;
        if (attr && ds) {
          Object.entries(attr.deals || {}).forEach(([dealId, adId]) => {
            const deal = dealsById.get(dealId);
            if (!deal || !(deal.created >= ds && deal.created < de)) return;
            attributed++;
            const adset = (attr.ads[adId] || [])[1];
            const camp = adset ? (attr.adsets[adset] || [])[1] : '';
            const k = level === 'ad' ? adId : level === 'adset' ? adset : camp;
            if (!k) return;
            const c = crm.get(k) || { leads: 0, qual: 0, sched: 0, att: 0, paid: 0, rev: 0 };
            c.leads++; if (deal.qualAt) c.qual++; if (deal.scheduledAt) c.sched++; if (deal.attendedAt) c.att++;
            if (deal.paidAt) { c.paid++; c.rev += deal.revenue; }
            crm.set(k, c);
          });
        }
        const fatIds = new Map(fat.map(f => [f.id, f]));
        const data = rows.map(r => {
          const spend = +r.spend || 0, leads = metaLeads(r.actions);
          const c = crm.get(r[idField[level]]) || { leads: 0, qual: 0, sched: 0, att: 0, paid: 0, rev: 0 };
          return { id: r[idField[level]], adId: r.ad_id, name: r[nameField[level]] || '—', sub: level === 'ad' ? r.adset_name : level === 'adset' ? r.campaign_name : '',
            spend, imp: +r.impressions || 0, freq: +r.frequency || 0, ctr: +r.inline_link_click_ctr || 0, cpm: +r.cpm || 0,
            leads, cpl: leads ? spend / leads : null,
            crmLeads: c.leads, qual: c.qual, sched: c.sched, att: c.att, paid: c.paid, rev: c.rev,
            cpq: c.qual ? spend / c.qual : null, cac: c.paid ? spend / c.paid : null,
            romi: spend && c.paid ? (c.rev / rateOf() - spend) / spend * 100 : null };
        }).filter(r => r.spend > 0);
        draw(data, fatIds, attr, attributed);
      } catch (e) {
        body.innerHTML = `<div class="ki-notice">Не удалось получить данные кабинета: ${u.esc(e.message)}</div>`;
      }
    }

    function draw(data, fatIds, attr, attributed) {
      const T = data.reduce((a, r) => { ['spend', 'imp', 'leads', 'crmLeads', 'qual', 'sched', 'att', 'paid', 'rev'].forEach(k => a[k] += r[k]); return a; },
        { spend: 0, imp: 0, leads: 0, crmLeads: 0, qual: 0, sched: 0, att: 0, paid: 0, rev: 0 });
      const avgCpl = T.leads ? T.spend / T.leads : null, avgCac = T.paid ? T.spend / T.paid : null;
      const asc = ['cpl', 'cpm', 'cpq', 'cac'];
      const sorted = data.slice().sort((a, b) => ((b[sortKey] ?? -1e9) - (a[sortKey] ?? -1e9)) * (asc.includes(sortKey) ? -1 : 1));
      const fb = [['spend', 'Потрачено'], ['imp', 'Показы'], ['freq', 'Частота'], ['ctr', 'CTR'], ['cpm', 'CPM'], ['leads', 'Лиды FB'], ['cpl', 'Цена лида']];
      const cr = [['crmLeads', 'Лиды CRM'], ['qual', 'Квал'], ['sched', 'Назн. ОУ'], ['att', 'Пришли'], ['paid', 'Оплаты'], ['rev', 'Выручка'], ['cpq', 'Цена квала'], ['cac', 'CAC'], ['romi', 'ROMI']];
      const th = ([k, l]) => `<th><button type="button" data-sort="${k}" class="${k === sortKey ? 'on' : ''}">${l}</button></th>`;
      const share = (n, of) => of ? `<small>${Math.round(n / of * 100)}%</small>` : '';
      const toneVs = (v, avg, lowGood) => v == null || !avg ? '' : (lowGood ? (v <= avg * 0.85 ? 'good' : v >= avg * 1.3 ? 'bad' : '') : '');
      const money = v => v ? mln(v) : '—';
      body.innerHTML = `<div class="ki-tbl"><table class="ki-sortable ki-mtable">
        <thead>
          <tr class="ki-grp"><th></th><th colspan="${fb.length}" class="ki-g-fb">Facebook</th><th colspan="${cr.length}" class="ki-g-crm">AmoCRM · лиды периода и их путь</th></tr>
          <tr><th>${LEVELS.find(l => l[0] === level)[1]}</th>${fb.map(th).join('')}${cr.map(th).join('')}</tr>
        </thead>
        <tbody>${sorted.map(r => {
          const f = level === 'ad' && fatIds.get(r.adId);
          return `<tr><td class="ki-name"><b>${u.esc(r.name)}</b>${f ? ' <span class="ki-pill ki-bad">дорожает</span>' : ''}${r.sub ? `<small>${u.esc(r.sub)}</small>` : ''}</td>
            <td>${u.usd(r.spend, 2)}</td><td>${u.fmt(r.imp)}</td>
            <td>${r.freq ? `<span class="${r.freq >= 3 ? 'ki-t-bad' : ''}">${r.freq.toFixed(2).replace('.', ',')}</span>` : '—'}</td>
            <td>${r.ctr.toFixed(2).replace('.', ',')}%</td><td>${u.usd(r.cpm, 2)}</td><td>${r.leads}</td>
            <td>${r.cpl != null ? `<span class="ki-cac ${toneVs(r.cpl, avgCpl, true) ? 'ki-' + toneVs(r.cpl, avgCpl, true) : ''}">${u.usd(r.cpl, 2)}</span>` : '—'}</td>
            <td class="ki-c1">${r.crmLeads}${share(r.crmLeads, r.leads).replace('%', '% от FB')}</td>
            <td>${r.qual}${share(r.qual, r.crmLeads)}</td><td>${r.sched}${share(r.sched, r.crmLeads)}</td>
            <td>${r.att}${share(r.att, r.crmLeads)}</td><td><b>${r.paid}</b>${share(r.paid, r.crmLeads)}</td>
            <td>${money(r.rev)}</td><td>${r.cpq != null ? u.usd(r.cpq, 2) : '—'}</td>
            <td>${r.cac != null ? `<span class="ki-cac ki-${u.cacTone(r.cac)}">$${Math.round(r.cac)}</span>` : '—'}</td>
            <td>${r.romi != null ? `<span class="ki-cac ki-${toneRomi(r.romi)}">${Math.round(r.romi)}%</span>` : '—'}</td></tr>`;
        }).join('')}</tbody>
        <tfoot><tr><td>Итого · ${data.length}</td><td>${u.usd(T.spend, 2)}</td><td>${u.fmt(T.imp)}</td><td></td>
          <td></td><td>${T.imp ? u.usd(T.spend / T.imp * 1000, 2) : '—'}</td><td>${T.leads}</td><td>${avgCpl != null ? u.usd(avgCpl, 2) : '—'}</td>
          <td class="ki-c1">${T.crmLeads}</td><td>${T.qual}</td><td>${T.sched}</td><td>${T.att}</td><td>${T.paid}</td><td>${money(T.rev)}</td>
          <td>${T.qual ? u.usd(T.spend / T.qual, 2) : '—'}</td><td>${avgCac != null ? '$' + Math.round(avgCac) : '—'}</td>
          <td>${T.paid ? Math.round((T.rev / rateOf() - T.spend) / T.spend * 100) + '%' : '—'}</td></tr></tfoot>
      </table></div>
      <p class="ki-note">${attr ? `CRM: лиды из форм Meta сопоставлены со сделками AmoCRM по телефону (обновление раз в час, последнее — ${new Date(attr.read_at).toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' })}). За период нашли сделку ${T.leads ? Math.round(attributed / T.leads * 100) + '%' : '—'} лидов FB. Оплаты и выручка — за всё время по лидам периода.` : 'CRM-колонки пусты: связка лидов со сделками ещё не загружена.'}
        Цена лида: зелёным — дешевле среднего на 15%+, красным — дороже на 30%+. Частота выше 3 — аудитория выгорает. «Дорожает» — за 3 дня лид подорожал на 40%+ или CTR упал на 30%+.</p>`;
      body.querySelectorAll('[data-sort]').forEach(b => b.addEventListener('click', () => { sortKey = b.dataset.sort; draw(data, fatIds, attr, attributed); }));
    }
    el.querySelector('#mtLevel').addEventListener('click', e => { const b = e.target.closest('button'); if (!b) return; level = b.dataset.v; lsSet('sk-meta-level', level); el.querySelectorAll('#mtLevel button').forEach(x => x.classList.toggle('on', x === b)); load(); });
    el.querySelector('#mtPeriod').addEventListener('click', e => { const b = e.target.closest('button'); if (!b) return; period = b.dataset.v; lsSet('sk-meta-period', period); el.querySelectorAll('#mtPeriod button').forEach(x => x.classList.toggle('on', x === b)); load(); });
    load();
  }

  // ---------- Окупаемость (блок для «Когорт и окупаемости») ----------
  function romiHtml(d) {
    const u = U();
    if (!d.spend || !d.spend.months) return '';
    const now = new Date();
    const months = [];
    for (let o = -3; o <= 0; o++) months.push(new Date(now.getFullYear(), now.getMonth() + o, 1));
    let rows = '';
    months.forEach(m => {
      const k = u.ym(m), end = new Date(m.getFullYear(), m.getMonth() + 1, 1), sp = d.spend.months[k];
      if (!sp) return;
      const rate = rateOf(k);
      ['meta', 'tg'].forEach(ch => {
        const spend = sp[ch] || 0;
        if (!spend) return;
        const ds = d.deals.filter(x => x.ch === ch && inR(x.created, m, end));
        const paid = ds.filter(x => x.paidAt);
        const first = paid.reduce((a, x) => a + x.revenue, 0), ren = paid.reduce((a, x) => a + x.renewalRevenue, 0);
        const revUsd = (first + ren) / rate;
        const romi = (revUsd - spend) / spend * 100;
        rows += `<tr><td class="ki-wk">${u.MONTHS_RU[m.getMonth()]}${k === u.ym(now) ? ' <span class="ki-pill">идёт</span>' : ''}</td>
          <td><span class="ki-chan"><i class="ki-sw" style="background:var(--${ch})"></i>${ch === 'meta' ? 'Meta' : 'Telegram'}</span></td>
          <td>${u.usd(spend, 0)}</td><td>${ds.length}</td><td>${paid.length}</td><td>${paid.length ? '$' + Math.round(spend / paid.length) : '—'}</td>
          <td>${first ? mln(first) : '—'}</td><td>${ren ? mln(ren) : '—'}</td>
          <td>${paid.length ? '$' + Math.round(revUsd / paid.length) : '—'}</td>
          <td>${paid.length ? `<span class="ki-cac ki-${toneRomi(romi)}">${Math.round(romi)}%</span>` : '—'}</td></tr>`;
      });
    });
    if (!rows) return '';
    return `<section class="ki-panel"><div class="ki-ph"><h3>Окупаемость по месяцу привлечения</h3><span>Выручка первой продажи + продления этих же клиентов</span></div>
      <div class="ki-tbl"><table><thead><tr><th>Месяц</th><th>Канал</th><th>Расход</th><th>Лиды</th><th>Клиенты</th><th>CAC</th><th>Первая продажа</th><th>Продления</th><th>LTV на клиента</th><th>ROMI</th></tr></thead>
      <tbody>${rows}</tbody></table></div>
      <p class="ki-note">ROMI = (выручка в $ − расход) ÷ расход. Курс — из «Плана маркетинга» (по умолчанию ${u.fmt(DEFAULT_RATE)} сум за $). Продления привязаны к клиенту по телефону; свежие месяцы ещё растут.</p></section>`;
  }

  // ---------- Качество лидов ----------
  function sourceOf(x) {
    if (x.ch === 'meta') return { k: 'meta', name: 'Meta · лид-форма' };
    if (x.ch === 'tg') return { k: 'tg:' + (x.campaign || '-'), name: 'Telegram · ' + (x.campaign || 'без кампании') };
    if (x.src) return { k: 'src:' + x.src + ':' + x.medium, name: x.src + (x.medium ? ' · ' + x.medium : '') };
    return { k: 'none', name: 'Без источника' };
  }
  const ageGroup = a => !a ? 'н/д' : a <= 9 ? '6–9' : a <= 12 ? '10–12' : a <= 15 ? '13–15' : '16+';

  function renderQuality(el, d) {
    const u = U();
    let days = +lsGet('sk-q-days', '30');
    const draw = () => {
      const from = u.addDays(new Date(), -days); from.setHours(0, 0, 0, 0);
      const ds = d.deals.filter(x => x.created >= from);
      const groups = new Map();
      ds.forEach(x => { const s = sourceOf(x); const g = groups.get(s.k) || { name: s.name, ch: x.ch, list: [] }; g.list.push(x); groups.set(s.k, g); });
      const rowsArr = [...groups.values()].sort((a, b) => b.list.length - a.list.length);
      const cell = (n, tot, warnBelow, goodAbove) => {
        const v = tot ? n / tot * 100 : 0;
        const t = tot >= 10 ? (v >= goodAbove ? 'good' : v < warnBelow ? 'bad' : '') : '';
        return `<td><span class="ki-cac ${t ? 'ki-' + t : ''}">${tot ? Math.round(v) + '%' : '—'}</span><small>${n}</small></td>`;
      };
      const reasonsOf = list => {
        const c = new Map(); list.filter(x => x.lost).forEach(x => { const r = x.lossReason || 'Причина не указана'; c.set(r, (c.get(r) || 0) + 1); });
        return [...c.entries()].sort((a, b) => b[1] - a[1]);
      };
      const tbody = rowsArr.map(g => {
        const L = g.list, n = L.length, lost = L.filter(x => x.lost).length;
        const top = reasonsOf(L).slice(0, 2).map(([r, c]) => `${u.esc(r)} — ${Math.round(c / Math.max(lost, 1) * 100)}%`).join('<br>');
        const ages = ['6–9', '10–12', '13–15'].map(a => L.filter(x => ageGroup(x.age) === a).length);
        const agesTot = ages.reduce((a, b) => a + b, 0);
        const ageBar = agesTot ? `<span class="ki-agebar" title="6–9: ${ages[0]}, 10–12: ${ages[1]}, 13–15: ${ages[2]}">${ages.map((v, i) => `<i style="width:${v / agesTot * 100}%" class="ki-age${i}"></i>`).join('')}</span>` : '—';
        return `<tr><td class="ki-name"><span class="ki-chan"><i class="ki-sw" style="background:var(--${g.ch === 'other' ? 'border' : g.ch})"></i><b>${u.esc(g.name)}</b></span></td>
          <td>${n}</td>${cell(L.filter(x => x.workedAt).length, n, 70, 90)}${cell(L.filter(x => x.qualAt).length, n, 45, 65)}
          ${cell(L.filter(x => x.scheduledAt).length, n, 30, 50)}${cell(L.filter(x => x.attendedAt).length, n, 15, 30)}
          ${cell(L.filter(x => x.paidAt).length, n, 3, 8)}${cell(lost, n, -1, 101)}
          <td class="ki-reasons">${top || '—'}</td><td>${ageBar}</td></tr>`;
      }).join('');
      // Причины отказа по каналам
      const chans = [['meta', 'Meta'], ['tg', 'Telegram'], ['other', 'Без источника']];
      const allReasons = new Map();
      chans.forEach(([ch]) => reasonsOf(ds.filter(x => x.ch === ch)).forEach(([r, c]) => allReasons.set(r, (allReasons.get(r) || 0) + c)));
      const topReasons = [...allReasons.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8);
      const lostBy = Object.fromEntries(chans.map(([ch]) => [ch, ds.filter(x => x.ch === ch && x.lost).length]));
      el.innerHTML = `
        <div class="ki-bar-row">${seg('qDays', [['30', '30 дней'], ['60', '60 дней'], ['90', '90 дней']], String(days))}</div>
        <section class="ki-panel"><div class="ki-ph"><h3>Воронка по источникам</h3><span>Лиды, созданные за ${days} дней; % от лидов источника</span></div>
          <div class="ki-tbl"><table class="ki-quality"><thead><tr><th>Источник</th><th>Лиды</th><th>В работе</th><th>Квал</th><th>Назн. ОУ</th><th>Пришли</th><th>Оплаты</th><th>Отказ</th><th>Главные причины отказа</th><th>Возраст 6–9 / 10–12 / 13–15</th></tr></thead>
          <tbody>${tbody}</tbody></table></div>
          <p class="ki-note">Цвет — только у источников от 10 лидов. Возраст — полоса: голубой 6–9, зелёный 10–12, фиолетовый 13–15.</p></section>
        <section class="ki-panel"><div class="ki-ph"><h3>Причины отказа</h3><span>Доля от отказов канала</span></div>
          <div class="ki-tbl"><table><thead><tr><th>Причина</th>${chans.map(([, n]) => `<th>${n}</th>`).join('')}</tr></thead>
          <tbody>${topReasons.map(([r]) => `<tr><td>${u.esc(r)}</td>${chans.map(([ch]) => { const c = reasonsOf(ds.filter(x => x.ch === ch)).find(z => z[0] === r); return `<td>${c ? Math.round(c[1] / lostBy[ch] * 100) + '%' : '—'}<small>${c ? c[1] : ''}</small></td>`; }).join('')}</tr>`).join('') || '<tr><td colspan="4">Отказов за период нет</td></tr>'}</tbody></table></div></section>`;
      el.querySelector('#qDays').addEventListener('click', e => { const b = e.target.closest('button'); if (!b) return; days = +b.dataset.v; lsSet('sk-q-days', String(days)); draw(); });
    };
    draw();
  }

  // ---------- Продажи: скорость обработки ----------
  function renderSpeed(el, d) {
    const u = U();
    let days = +lsGet('sk-speed-days', '30');
    const draw = () => {
      const from = u.addDays(new Date(), -days); from.setHours(0, 0, 0, 0);
      const ds = d.deals.filter(x => x.createdAt && x.created >= from);
      const lag = x => x.workedAt ? Math.round((x.workedAt - x.created) / 864e5) : null;
      const bucket = list => {
        const n = list.length, l = list.map(lag);
        return { n, d0: l.filter(v => v === 0).length, d1: l.filter(v => v === 1).length, d2: l.filter(v => v != null && v >= 2).length, none: l.filter(v => v == null).length,
          paid: list.filter(x => x.paidAt).length };
      };
      const row = (name, b) => `<tr><td class="ki-name"><b>${u.esc(name)}</b></td><td>${b.n}</td>
        <td><span class="ki-cac ki-${b.n >= 10 ? (b.d0 / b.n >= 0.8 ? 'good' : b.d0 / b.n >= 0.6 ? 'warn' : 'bad') : ''}">${pctTxt(b.d0, b.n)}</span></td>
        <td>${pctTxt(b.d1, b.n)}</td><td>${pctTxt(b.d2, b.n)}</td><td>${pctTxt(b.none, b.n)}</td><td>${pctTxt(b.paid, b.n)}</td></tr>`;
      const byMgr = new Map(); ds.forEach(x => { const m = x.manager || 'Без ответственного'; (byMgr.get(m) || byMgr.set(m, []).get(m)).push(x); });
      const mgrRows = [...byMgr.entries()].filter(([, l]) => l.length >= 5).sort((a, b) => b[1].length - a[1].length).map(([m, l]) => row(m, bucket(l))).join('');
      const slots = [[0, 9, '00–09'], [9, 13, '09–13'], [13, 18, '13–18'], [18, 21, '18–21'], [21, 24, '21–24']];
      const slotRows = slots.map(([a, b, l]) => row('Заявка в ' + l, bucket(ds.filter(x => x.createdAt.getHours() >= a && x.createdAt.getHours() < b)))).join('');
      // Связь скорости и оплаты
      const sameDay = ds.filter(x => lag(x) === 0), later = ds.filter(x => lag(x) != null && lag(x) >= 1);
      const conv = l => l.length ? (l.filter(x => x.paidAt).length / l.length * 100).toFixed(1).replace('.', ',') + '%' : '—';
      el.innerHTML = `
        <div class="ki-bar-row">${seg('spDays', [['30', '30 дней'], ['60', '60 дней'], ['90', '90 дней']], String(days))}</div>
        <div class="ki-kpis">
          <div class="ki-kpi"><div class="ki-lbl">Взяли в день заявки</div><div class="ki-val num">${pctTxt(sameDay.length, ds.length)}</div><div class="ki-kpi-sub">${sameDay.length} из ${ds.length}</div></div>
          <div class="ki-kpi"><div class="ki-lbl">Оплата, если взяли в тот же день</div><div class="ki-val num">${conv(sameDay)}</div><div class="ki-kpi-sub">лид → оплата</div></div>
          <div class="ki-kpi"><div class="ki-lbl">Оплата, если взяли позже</div><div class="ki-val num">${conv(later)}</div><div class="ki-kpi-sub">лид → оплата</div></div>
        </div>
        <section class="ki-panel"><div class="ki-ph"><h3>По менеджерам</h3><span>Когда лид взяли в работу относительно дня заявки</span></div>
          <div class="ki-tbl"><table><thead><tr><th>Менеджер</th><th>Лиды</th><th>В тот же день</th><th>На следующий</th><th>Позже</th><th>Не взяли</th><th>Лид → оплата</th></tr></thead><tbody>${mgrRows}</tbody></table></div></section>
        <section class="ki-panel"><div class="ki-ph"><h3>По времени заявки</h3><span>Вечерние заявки чаще ждут до утра</span></div>
          <div class="ki-tbl"><table><thead><tr><th>Время</th><th>Лиды</th><th>В тот же день</th><th>На следующий</th><th>Позже</th><th>Не взяли</th><th>Лид → оплата</th></tr></thead><tbody>${slotRows}</tbody></table></div>
          <p class="ki-note">«Дата ВР» в AmoCRM хранится без времени, поэтому скорость считается в днях. Чтобы видеть минуты до первого контакта, нужно писать в Amo и время взятия в работу.</p></section>`;
      el.querySelector('#spDays').addEventListener('click', e => { const b = e.target.closest('button'); if (!b) return; days = +b.dataset.v; lsSet('sk-speed-days', String(days)); draw(); });
    };
    draw();
  }

  // ---------- Продажи: дни и часы ----------
  function renderHours(el, d) {
    const u = U();
    let days = +lsGet('sk-hours-days', '60');
    let metric = lsGet('sk-hours-metric', 'leads');
    const draw = () => {
      const from = u.addDays(new Date(), -days); from.setHours(0, 0, 0, 0);
      const ds = d.deals.filter(x => x.createdAt && x.created >= from);
      const grid = Array.from({ length: 7 }, () => Array.from({ length: 24 }, () => ({ n: 0, q: 0, p: 0 })));
      ds.forEach(x => { const c = grid[(x.createdAt.getDay() + 6) % 7][x.createdAt.getHours()]; c.n++; if (x.qualAt) c.q++; if (x.paidAt) c.p++; });
      const val = c => metric === 'leads' ? c.n : metric === 'qual' ? (c.n >= 5 ? c.q / c.n * 100 : null) : (c.n >= 5 ? c.p / c.n * 100 : null);
      const vals = grid.flat().map(val).filter(v => v != null);
      const max = Math.max(1, ...vals);
      const fmtV = v => v == null ? '' : metric === 'leads' ? v : Math.round(v) + '%';
      const head = `<tr><th></th>${Array.from({ length: 24 }, (_, h) => `<th>${h}</th>`).join('')}<th>Итого</th></tr>`;
      const body = grid.map((row, i) => {
        const tot = row.reduce((a, c) => ({ n: a.n + c.n, q: a.q + c.q, p: a.p + c.p }), { n: 0, q: 0, p: 0 });
        return `<tr><th>${WD[i]}</th>${row.map(c => { const v = val(c); return `<td style="--o:${v == null ? 0 : (v / max).toFixed(2)}" title="${WD[i]}: лидов ${c.n}, квал ${c.q}, оплат ${c.p}">${fmtV(v)}</td>`; }).join('')}<td class="ki-hm-tot">${metric === 'leads' ? tot.n : pctTxt(metric === 'qual' ? tot.q : tot.p, tot.n)}</td></tr>`;
      }).join('');
      el.innerHTML = `
        <div class="ki-bar-row">${seg('hmDays', [['30', '30 дней'], ['60', '60 дней'], ['90', '90 дней']], String(days))}${seg('hmMetric', [['leads', 'Лиды'], ['qual', '% квала'], ['paid', '% оплат']], metric)}</div>
        <section class="ki-panel"><div class="ki-ph"><h3>Когда приходят заявки</h3><span>День недели × час создания лида, ${days} дней</span></div>
          <div class="ki-tbl"><table class="ki-heat"><thead>${head}</thead><tbody>${body}</tbody></table></div>
          <p class="ki-note">Проценты квала и оплат показаны только для ячеек от 5 лидов. Используйте карту, чтобы ставить менеджеров на смены в часы пик и снижать ставки рекламы в часы, когда лиды не квалифицируются.</p></section>`;
      el.querySelector('#hmDays').addEventListener('click', e => { const b = e.target.closest('button'); if (!b) return; days = +b.dataset.v; lsSet('sk-hours-days', String(days)); draw(); });
      el.querySelector('#hmMetric').addEventListener('click', e => { const b = e.target.closest('button'); if (!b) return; metric = b.dataset.v; lsSet('sk-hours-metric', metric); draw(); });
    };
    draw();
  }

  window.V2Mkt = {
    pages: { mktSummary: renderSummary, mktPlan: renderPlan, meta: renderMeta, quality: renderQuality, speed: renderSpeed, hours: renderHours },
    romiHtml,
  };
})();
