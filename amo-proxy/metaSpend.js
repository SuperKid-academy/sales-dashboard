// =====================================================================
// Meta Ads → Supabase: расход по дням из рекламных кабинетов, сгруппированный
// по неделям (с понедельника) и месяцам. Пишется в sync_state, ключ
// «meta_spend»; дашборд v2 берёт расход Meta оттуда, Telegram — из РнП.
//
// Env:
//   META_ADS_TOKEN    токен системного пользователя с ads_read
//   META_AD_ACCOUNTS  ID кабинетов через запятую, без «act_» (например 908141738368323)
//
// Токен живёт только на сервере — в код страницы он не попадает.
// =====================================================================
import { setSyncState } from './supabase.js';

const API = 'https://graph.facebook.com/v25.0';
const DAYS_BACK = 75; // 7 недель когорт + запас на начало месяца

function cfg() {
  const token = process.env.META_ADS_TOKEN || '';
  const accounts = String(process.env.META_AD_ACCOUNTS || '')
    .split(',').map(s => s.trim().replace(/^act_/, '')).filter(Boolean);
  return { token, accounts };
}

export function metaConfigured() {
  const c = cfg();
  return !!(c.token && c.accounts.length);
}

const iso = d => d.toISOString().slice(0, 10);
function mondayKey(dateStr) {
  const [y, m, d] = dateStr.split('-').map(Number);
  const x = new Date(Date.UTC(y, m - 1, d));
  x.setUTCDate(x.getUTCDate() - ((x.getUTCDay() + 6) % 7));
  return iso(x);
}

async function dailySpend(token, account) {
  const until = new Date();
  const since = new Date(until.getTime() - DAYS_BACK * 864e5);
  const params = new URLSearchParams({
    fields: 'spend',
    time_increment: '1',
    time_range: JSON.stringify({ since: iso(since), until: iso(until) }),
    limit: '500',
    access_token: token,
  });
  let url = `${API}/act_${account}/insights?${params}`;
  const out = [];
  // Даты в ответе — в часовом поясе кабинета (у нас Asia/Tashkent).
  while (url) {
    const r = await fetch(url);
    const j = await r.json();
    if (!r.ok || j.error) throw new Error(`Meta act_${account}: ${j.error ? j.error.message : r.status}`);
    for (const row of j.data || []) out.push({ date: row.date_start, spend: Number(row.spend) || 0 });
    url = j.paging && j.paging.next ? j.paging.next : null;
  }
  return out;
}

export async function syncMetaSpend() {
  const { token, accounts } = cfg();
  if (!token || !accounts.length) return { skipped: true };
  const weeks = {}, months = {}, days = {}, perAccount = {};
  for (const acc of accounts) {
    const daily = await dailySpend(token, acc);
    perAccount[acc] = Math.round(daily.reduce((a, b) => a + b.spend, 0) * 100) / 100;
    for (const { date, spend } of daily) {
      // По дням — чтобы дашборд считал расход за любой выбранный период.
      days[date] = Math.round(((days[date] || 0) + spend) * 100) / 100;
      const w = mondayKey(date), m = date.slice(0, 7);
      weeks[w] = Math.round(((weeks[w] || 0) + spend) * 100) / 100;
      months[m] = Math.round(((months[m] || 0) + spend) * 100) / 100;
    }
  }
  const value = { source: 'meta_api', accounts, weeks, months, days, per_account: perAccount, read_at: new Date().toISOString() };
  await setSyncState('meta_spend', JSON.stringify(value));
  console.log(`[meta-spend] записано: недель ${Object.keys(weeks).length}, кабинетов ${accounts.length}`);
  return { weeks: Object.keys(weeks).length, months };
}
