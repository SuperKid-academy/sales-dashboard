// Забирает историю звонков из OnlinePBX и складывает готовые цифры
// по внутренним номерам в calls.json. Запускается GitHub Actions по
// расписанию (.github/workflows/pbx-sync.yml), дашборд читает результат.
//
// Секреты репозитория (Settings → Secrets and variables → Actions):
//   PBX_DOMAIN    pbx36077.onpbx.ru
//   PBX_AUTH_KEY  API-ключ из кабинета OnlinePBX
//
// Запуск: node scripts/pbx-sync.mjs <prev.json> <out.json>
// В файл попадают только счётчики — никаких номеров клиентов.

import fs from 'node:fs';

const DOMAIN = (process.env.PBX_DOMAIN || '').trim().replace(/^https?:\/\//, '').replace(/\/+$/, '');
const AUTH_KEY = (process.env.PBX_AUTH_KEY || '').trim();
const HOSTS = [process.env.PBX_API_HOST, 'api2.onlinepbx.ru', 'api.onlinepbx.ru'].filter(Boolean);
const TZ_OFFSET_SEC = 5 * 3600;   // Ташкент
const MIN_OK_SEC = 30;            // «дозвонились» — разговор от 30 секунд
const REFRESH_DAYS = 3;           // каждый запуск пересчитываем сегодня и 2 прошлых дня
const BACKFILL_DAYS = 45;         // при первом запуске — полтора месяца назад
const KEEP_DAYS = 120;
// Внутренние номера сотрудников трёхзначные. Группа «10» и правила
// «5100»/«6100» под шаблон не попадают — звонок засчитывается тому, кто ответил.
const EXT_RE = new RegExp(process.env.PBX_EXT_RE || '^\\d{3}$');

const [prevPath, outPath] = process.argv.slice(2);
if (!DOMAIN || !AUTH_KEY) { console.error('Не заданы секреты PBX_DOMAIN и PBX_AUTH_KEY'); process.exit(1); }

const sleep = ms => new Promise(r => setTimeout(r, ms));
let host = null, session = null;

async function auth() {
  const errors = [];
  for (const h of HOSTS) {
    try {
      const resp = await fetch(`https://${h}/${DOMAIN}/auth.json`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ auth_key: AUTH_KEY, new: 'true' }),
      });
      const text = await resp.text();
      let data = null; try { data = JSON.parse(text); } catch {}
      if (data?.data?.key_id && data?.data?.key) {
        host = h; session = { keyId: data.data.key_id, key: data.data.key };
        console.log(`Авторизация OnlinePBX: ок (${h})`);
        return;
      }
      errors.push(`${h}: ${resp.status} ${text.slice(0, 200)}`);
    } catch (e) { errors.push(`${h}: ${e.message}`); }
  }
  throw new Error('Не удалось авторизоваться в OnlinePBX:\n' + errors.join('\n'));
}

async function request(path, params, retry = true) {
  if (!session) await auth();
  const resp = await fetch(`https://${host}/${DOMAIN}/${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'x-pbx-authentication': `${session.keyId}:${session.key}`,
    },
    body: new URLSearchParams(params),
  });
  const text = await resp.text();
  let data = null; try { data = JSON.parse(text); } catch {}
  if (data && String(data.status) === '1') return data.data;
  if (retry && (resp.status === 401 || resp.status === 403 || /auth|key/i.test(text))) {
    session = null;
    return request(path, params, false);
  }
  throw new Error(`OnlinePBX ${path}: ${resp.status} ${text.slice(0, 300)}`);
}

const isExt = v => EXT_RE.test(String(v ?? '').trim());

function findExtension(rec, dir) {
  const primary = dir === 'outbound' ? rec.caller_id_number : rec.destination_number;
  if (isExt(primary)) return String(primary).trim();
  const found = [];
  const walk = o => {
    if (!o || typeof o !== 'object') return;
    for (const [k, v] of Object.entries(o)) {
      if (v && typeof v === 'object') walk(v);
      else if (/number|user|ext|dst|destination/i.test(k) && isExt(v)) found.push(String(v).trim());
    }
  };
  walk(rec.events);
  return found.length ? found[found.length - 1] : null;
}

const dayKey = unix => new Date((Number(unix) + TZ_OFFSET_SEC) * 1000).toISOString().slice(0, 10);
const empty = () => ({ out: 0, outOk: 0, outTalk: 0, in: 0, inAnswered: 0, inOk: 0, inTalk: 0, missed: 0 });

function aggregate(records) {
  const byExt = {};
  for (const rec of records) {
    const dir = String(rec.accountcode || '').toLowerCase();
    if (dir !== 'outbound' && dir !== 'inbound') continue; // внутренние звонки не считаем
    const talk = Number(rec.user_talk_time) || 0;
    const s = (byExt[findExtension(rec, dir) || '—'] ||= empty());
    if (dir === 'outbound') {
      s.out++; s.outTalk += talk;
      if (talk >= MIN_OK_SEC) s.outOk++;
    } else {
      s.in++; s.inTalk += talk;
      if (talk > 0) s.inAnswered++; else s.missed++;
      if (talk >= MIN_OK_SEC) s.inOk++;
    }
  }
  return byExt;
}

// Несколько записей для проверки распознавания — телефоны клиентов скрыты
function sampleOf(records) {
  const mask = v => (isExt(v) ? String(v) : String(v ?? '').replace(/\d(?=\d{2})/g, '•'));
  return records.slice(-5).map(r => ({
    accountcode: r.accountcode,
    caller: mask(r.caller_id_number),
    destination: mask(r.destination_number),
    talk: r.user_talk_time,
    ext: findExtension(r, String(r.accountcode || '').toLowerCase()),
    fields: Object.keys(r),
  }));
}

let prev = {};
try { prev = JSON.parse(fs.readFileSync(prevPath, 'utf8')); } catch {}
const days = prev.days || {};

const today = dayKey(Date.now() / 1000);
const firstRun = Object.keys(days).length === 0;
const count = firstRun ? BACKFILL_DAYS : REFRESH_DAYS;
let sample = prev.sample || [];

for (let i = count - 1; i >= 0; i--) {
  const d = new Date(today + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() - i);
  const key = d.toISOString().slice(0, 10);
  const from = Math.floor(d.getTime() / 1000) - TZ_OFFSET_SEC;
  const records = await request('mongo_history/search.json', {
    start_stamp_from: String(from),
    start_stamp_to: String(from + 86399),
  });
  const list = Array.isArray(records) ? records : [];
  days[key] = aggregate(list);
  if (list.length) sample = sampleOf(list);
  console.log(`${key}: ${list.length} звонков`);
  await sleep(400); // не упираемся в лимит АТС
}

// Старое чистим
const cutoff = new Date(today + 'T00:00:00Z');
cutoff.setUTCDate(cutoff.getUTCDate() - KEEP_DAYS);
for (const k of Object.keys(days)) if (k < cutoff.toISOString().slice(0, 10)) delete days[k];

const sorted = Object.fromEntries(Object.keys(days).sort().map(k => [k, days[k]]));
fs.writeFileSync(outPath, JSON.stringify({
  updatedAt: new Date().toISOString(),
  minOkSec: MIN_OK_SEC,
  days: sorted,
  sample,
}));
console.log(`Готово: ${outPath}`);
