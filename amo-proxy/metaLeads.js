// =====================================================================
// Meta Lead Ads → привязка сделок AmoCRM к кампании / группе / объявлению.
//
// У лидов из Meta в Amo нет ссылки на объявление (только тег формы fb<id>).
// Поэтому берём лиды из лид-форм страницы (у каждого есть ad_id, adset_id,
// campaign_id) и сопоставляем со сделками по телефону и времени создания.
// Результат — только связка «сделка → объявление» и названия объявлений;
// телефоны и имена никуда не сохраняются.
//
// Пишем в Supabase → sync_state, ключ «meta_attribution»:
//   { deals: { <deal_id>: <ad_id> }, ads: { <ad_id>: [name, adset_id] },
//     adsets: { <adset_id>: [name, campaign_id] }, campaigns: { <id>: name }, ... }
//
// Нужно: META_ADS_TOKEN системного пользователя с leads_retrieval,
// pages_show_list и доступом к странице с задачей «Лиды».
// =====================================================================
import { sbFetch, setSyncState } from './supabase.js';

const API = 'https://graph.facebook.com/v25.0';
const DAYS_BACK = 120;
const MATCH_WINDOW_H = 72; // лид и сделка — не дальше 3 суток друг от друга

const phoneKey = p => { const d = String(p || '').replace(/\D/g, ''); return d.length >= 9 ? d.slice(-9) : ''; };

async function graph(path, params) {
  const url = path.startsWith('http') ? path : `${API}/${path}?${new URLSearchParams(params)}`;
  const r = await fetch(url);
  const j = await r.json();
  if (!r.ok || j.error) throw new Error(`Meta ${path.split('?')[0]}: ${j.error ? j.error.message : r.status}`);
  return j;
}
async function graphAll(path, params, cap = 50) {
  const out = [];
  let j = await graph(path, params);
  for (let i = 0; ; i++) {
    out.push(...(j.data || []));
    if (!j.paging || !j.paging.next || i >= cap) break;
    j = await graph(j.paging.next);
  }
  return out;
}

// Дата сделки в Supabase — «dd.MM.yyyy HH:mm» по Ташкенту (UTC+5).
function dealTime(s) {
  const m = String(s || '').match(/(\d{2})\.(\d{2})\.(\d{4})(?:\s+(\d{1,2}):(\d{2}))?/);
  return m ? Date.UTC(+m[3], +m[2] - 1, +m[1], +(m[4] || 0) - 5, +(m[5] || 0)) : null;
}

async function loadDeals() {
  const out = [];
  for (let off = 0; ; off += 1000) {
    const b = await sbFetch(`/deals?select=id,phone,date_created,tags&order=id.asc&limit=1000&offset=${off}`);
    out.push(...(b || []));
    if (!b || b.length < 1000) break;
  }
  return out.map(d => ({ id: d.id, phone: phoneKey(d.phone), t: dealTime(d.date_created), tags: String(d.tags || '') }))
    .filter(d => d.phone && d.t);
}

export async function syncMetaAttribution() {
  const token = process.env.META_ADS_TOKEN || '';
  if (!token) return { skipped: true };
  const pages = await graphAll('me/accounts', { fields: 'id,name,access_token,tasks', access_token: token });
  const leadPages = pages.filter(p => p.access_token && (p.tasks || []).includes('MANAGE_LEADS'));
  if (!leadPages.length) throw new Error('Нет страниц с доступом к лидам (задача «Лиды» для системного пользователя).');

  const since = Math.floor((Date.now() - DAYS_BACK * 864e5) / 1000);
  const leads = [];
  for (const p of leadPages) {
    const forms = (await graphAll(`${p.id}/leadgen_forms`, { fields: 'id,name,leads_count', limit: '100', access_token: p.access_token }))
      .filter(f => !/менеджер|моп/i.test(f.name || ''));
    // Формы найма («Менеджер…», «МОП…») к рекламе продаж не относятся.
    const salesForms = forms.filter(f => (f.leads_count || 0) > 0);
    for (const f of salesForms) {
      const rows = await graphAll(`${f.id}/leads`, {
        fields: 'created_time,ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,field_data',
        filtering: JSON.stringify([{ field: 'time_created', operator: 'GREATER_THAN', value: since }]),
        limit: '500', access_token: p.access_token,
      });
      for (const l of rows) {
        const ph = (l.field_data || []).find(x => /phone/i.test(x.name));
        leads.push({ form: f.id, t: Date.parse(l.created_time), phone: phoneKey(ph && ph.values && ph.values[0]),
          ad: l.ad_id, adName: l.ad_name, adset: l.adset_id, adsetName: l.adset_name, camp: l.campaign_id, campName: l.campaign_name });
      }
    }
  }

  const deals = await loadDeals();
  const byPhone = new Map();
  deals.forEach(d => { (byPhone.get(d.phone) || byPhone.set(d.phone, []).get(d.phone)).push(d); });

  const out = { deals: {}, ads: {}, adsets: {}, campaigns: {} };
  let matched = 0, organic = 0;
  // Сначала самые свежие лиды: если клиент оставлял заявку дважды, сделка
  // достаётся ближайшему по времени лиду.
  leads.sort((a, b) => b.t - a.t);
  for (const l of leads) {
    if (!l.ad) { organic++; continue; }
    if (l.ad) out.ads[l.ad] = [l.adName || '', l.adset || ''];
    if (l.adset) out.adsets[l.adset] = [l.adsetName || '', l.camp || ''];
    if (l.camp) out.campaigns[l.camp] = l.campName || '';
    const cands = (byPhone.get(l.phone) || []).filter(d => !out.deals[d.id] && Math.abs(d.t - l.t) <= MATCH_WINDOW_H * 3600e3);
    if (!cands.length) continue;
    // Предпочитаем сделку с тегом этой формы, затем ближайшую по времени.
    cands.sort((a, b) => (b.tags.includes('fb' + l.form) - a.tags.includes('fb' + l.form)) || (Math.abs(a.t - l.t) - Math.abs(b.t - l.t)));
    out.deals[cands[0].id] = l.ad;
    matched++;
  }
  const value = { ...out, leads_total: leads.length, matched, organic, days_back: DAYS_BACK, read_at: new Date().toISOString() };
  await setSyncState('meta_attribution', JSON.stringify(value));
  console.log(`[meta-attribution] лидов ${leads.length}, сопоставлено со сделками ${matched}`);
  return { leads: leads.length, matched, organic };
}
