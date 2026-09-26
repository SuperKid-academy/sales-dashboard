// Прокси между дашбордом и AmoCRM.
//
// Зачем отдельный сервис: у Apps Script дневная квота на исходящие запросы
// общая с синхронизацией таблицы. Синк её выедал, и преподаватель во время
// урока получал «Служба была вызвана слишком много раз за день». Здесь таких
// ограничений нет.
//
// Ключи задаются переменными окружения, в коде их нет:
//   AMO_DOMAIN      superkid.amocrm.ru
//   AMO_TOKEN       долгосрочный токен AmoCRM
//   OPENAI_API_KEY  ключ OpenAI (без него обратная связь соберётся из оценок)
//   ALLOWED_ORIGIN  https://dashboard.superkid.uz (по умолчанию — он же)
//   PBX_DOMAIN      домен OnlinePBX, например superkid.onpbx.ru
//   PBX_AUTH_KEY    API-ключ OnlinePBX

import express from 'express';

const app = express();
app.use(express.json({ limit: '1mb' }));

// Имена переменных окружения регистрозависимы, и «Amo_token» молча не
// подхватывался — сервис брал соседний протухший токен и падал с 401.
// Ищем без учёта регистра и подчёркиваний.
function envAny(...names) {
  const norm = s => s.toLowerCase().replace(/[_-]/g, '');
  const wanted = names.map(norm);
  for (const [key, value] of Object.entries(process.env)) {
    if (value && wanted.includes(norm(key))) return { value, key };
  }
  return { value: '', key: null };
}

const amoTokenFound = envAny('AMO_TOKEN', 'AMO_LONG_TOKEN');
const amoAccessFound = envAny('AMO_ACCESS_TOKEN');

const CONFIG = {
  amoDomain: process.env.AMO_DOMAIN || 'superkid.amocrm.ru',
  // Долгосрочный токен приоритетнее: AMO_ACCESS_TOKEN — это OAuth-токен на
  // сутки, он протухает, а обновлять его этот сервис не умеет.
  amoToken: amoTokenFound.value || amoAccessFound.value || '',
  amoTokenSource: amoTokenFound.key || amoAccessFound.key || null,
  openaiKey: process.env.OPENAI_API_KEY || '',
  allowedOrigin: process.env.ALLOWED_ORIGIN || 'https://dashboard.superkid.uz',
  // Куда переводить сделку после отметки посещения
  pipelineId: Number(process.env.OU_PIPELINE_ID || 5326345),
  statusId: Number(process.env.OU_ATTENDED_STATUS_ID || 87908298),
};

// Дашборд открывают и с dashboard.superkid.uz, и с github.io — пускаем оба.
// ALLOWED_ORIGIN может содержать несколько адресов через запятую.
const ALLOWED_ORIGINS = new Set([
  ...CONFIG.allowedOrigin.split(',').map(s => s.trim()).filter(Boolean),
  'https://dashboard.superkid.uz',
  'https://superkid-academy.github.io',
]);

// Дашборд открыт на другом домене, поэтому нужен CORS. Preflight отвечаем
// сразу, иначе браузер не отправит сам запрос.
app.use((req, res, next) => {
  const origin = req.get('Origin');
  res.set('Access-Control-Allow-Origin',
    origin && ALLOWED_ORIGINS.has(origin) ? origin : CONFIG.allowedOrigin.split(',')[0].trim());
  res.set('Vary', 'Origin');
  res.set('Access-Control-Allow-Headers', 'Content-Type');
  res.set('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ============================================================
// AmoCRM
// ============================================================

async function amoFetch(path, options = {}) {
  if (!CONFIG.amoToken) throw new Error('Не задан AMO_TOKEN');
  const resp = await fetch(`https://${CONFIG.amoDomain}${path}`, {
    method: options.method || 'GET',
    headers: {
      'Authorization': `Bearer ${CONFIG.amoToken}`,
      'Content-Type': 'application/json',
    },
    body: options.body ? JSON.stringify(options.body) : undefined,
  });

  if (resp.status === 401) throw new Error('AmoCRM: токен недействителен или отозван (401)');
  if (resp.status === 204) return null;

  const text = await resp.text();
  if (!resp.ok) throw new Error(`AmoCRM ${resp.status}: ${text.slice(0, 300)}`);
  try { return text ? JSON.parse(text) : null; }
  catch { return null; }
}

async function addNote(dealId, text) {
  return amoFetch(`/api/v4/leads/${dealId}/notes`, {
    method: 'POST',
    body: [{ note_type: 'common', params: { text } }],
  });
}

async function moveDeal(dealId, statusId, pipelineId) {
  return amoFetch(`/api/v4/leads/${dealId}`, {
    method: 'PATCH',
    body: {
      status_id: Number(statusId) || CONFIG.statusId,
      pipeline_id: Number(pipelineId) || CONFIG.pipelineId,
    },
  });
}

// ============================================================
// Обратная связь
// ============================================================

const SKILL_ICONS = {
  'Коммуникация': '🗣',
  'Работа в команде': '🤝',
  'Компьютерные навыки': '💻',
  'Самостоятельность': '🎯',
  'Характер': '🧠',
};

// Запасной вариант, если ChatGPT недоступен: сделка всё равно должна
// переехать, а оценки — попасть в примечание.
function buildPlainFeedback(name, age, grades) {
  const lines = [
    `📋 Обратная связь после открытого урока — ${name || '—'}${age ? ` (${age} лет)` : ''}`,
    '',
  ];
  for (const [k, v] of Object.entries(grades)) {
    if (v) lines.push(`${SKILL_ICONS[k] || '•'} ${k}: ${v}`);
  }
  return lines.join('\n');
}

function buildPrompt(name, age, grades) {
  const facts = Object.entries(grades)
    .filter(([, v]) => v)
    .map(([k, v]) => `${k}: ${v}`)
    .join('\n');
  const childName = name || 'ученик';

  return `Ты методист IT академии Superkid. Напиши родителям развёрнутую обратную связь по итогам открытого урока, на русском языке.

Ученик: ${childName}${age ? `, ${age} лет` : ''}
Наблюдения преподавателя:
${facts}

СТРУКТУРА (соблюдай точно, каждый раздел с новой строки):
Первая строка — заголовок: «📋 Обратная связь после открытого урока»
Вторая строка — «Обратная связь после открытого урока ученика ${childName}. » и одно предложение о том, чем преподаватель остался доволен.
Далее разделы, заголовок каждого на отдельной строке, строго с этим эмодзи в начале:
  ✨ Общее впечатление — 3-4 предложения: активность и заинтересованность, как характер помогает в общении, сильные стороны, что полезно развивать.
  🗣 Коммуникация — 2-3 предложения.
  🤝 Работа в команде — 2-3 предложения.
  💻 Компьютерные навыки — 2-3 предложения.
  🎯 Самостоятельность — 2-3 предложения.
  🚀 Рекомендация — 2-3 предложения: пригласи продолжить обучение в IT академии Superkid, назови сильные стороны ученика и что академия поможет усилить, упомяни программирование, критическое мышление и креативность.

ТОН И ПРАВИЛА:
- Пиши о преподавателе в третьем лице: «Преподаватель отметил…», «Преподаватель подчеркнул…».
- Тон доброжелательный и поддерживающий. Слабые стороны подавай как зону роста, а не как недостаток: не «плохо работает в команде», а «командные проекты помогут раскрыть потенциал».
- В каждом разделе упоминай, как академия будет развивать этот навык: мини-презентации, командные проекты, групповые обсуждения, мини-проекты, практические задания.
- Опирайся только на наблюдения выше, не выдумывай фактов о ребёнке.
- Пиши разделы только для тех навыков, по которым есть наблюдения.
- Обращайся к ученику по имени.
- Эмодзи ставь только в заголовках разделов, внутри текста — не нужно.
- Между разделами оставляй пустую строку.
- Без приветствия, без подписи, без markdown-разметки и звёздочек.`;
}

async function generateFeedback(name, age, grades) {
  if (!CONFIG.openaiKey) return '';
  const facts = Object.values(grades).filter(Boolean);
  if (facts.length === 0) return '';

  const resp = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${CONFIG.openaiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: process.env.OPENAI_MODEL || 'gpt-4o-mini',
      messages: [{ role: 'user', content: buildPrompt(name, age, grades) }],
      temperature: 0.7,
      max_tokens: 1600,
    }),
  });

  if (!resp.ok) {
    const t = await resp.text();
    throw new Error(`OpenAI ${resp.status}: ${t.slice(0, 200)}`);
  }
  const data = await resp.json();
  const text = data?.choices?.[0]?.message?.content;
  if (!text) return '';
  // В примечании AmoCRM markdown не отображается
  return text.trim().replace(/\*\*/g, '').replace(/^#+\s*/gm, '');
}

// ============================================================
// Эндпоинты
// ============================================================

app.get('/', (req, res) => {
  res.json({
    ok: true,
    service: 'superkid-amo-proxy',
    amo: Boolean(CONFIG.amoToken),
    openai: Boolean(CONFIG.openaiKey),
    pbx: Boolean(PBX.domain && PBX.authKey),
    time: new Date().toISOString(),
  });
});

// Проверка готовности без побочных эффектов: читаем аккаунт AmoCRM и
// спрашиваем у OpenAI одно слово. Нужна, чтобы убедиться в работоспособности
// до урока, а не выяснять это на живой сделке.
app.get('/api/check', async (req, res) => {
  const out = { ok: true, amo: null, openai: null };

  // Длину и края токена показываем намеренно: длинный токен часто копируют
  // не целиком, и по одной ошибке 401 не понять, в этом дело или он отозван.
  const t = CONFIG.amoToken;
  const tokenInfo = t
    ? { length: t.length, starts: t.slice(0, 8), ends: t.slice(-6),
        source: CONFIG.amoTokenSource }
    : { length: 0, error: 'токен не задан ни в одной переменной' };

  try {
    const acc = await amoFetch('/api/v4/account');
    out.amo = { ok: true, account: acc?.name || acc?.subdomain || 'подключено', token: tokenInfo };
  } catch (e) {
    out.ok = false;
    out.amo = { ok: false, error: e.message, token: tokenInfo };
  }

  if (!CONFIG.openaiKey) {
    out.openai = { ok: false, error: 'ключ не задан — обратная связь будет без ИИ' };
  } else {
    try {
      const r = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${CONFIG.openaiKey}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          model: process.env.OPENAI_MODEL || 'gpt-4o-mini',
          messages: [{ role: 'user', content: 'Ответь одним словом: готов' }],
          max_tokens: 5,
        }),
      });
      if (r.ok) {
        out.openai = { ok: true };
      } else {
        out.ok = false;
        out.openai = { ok: false, error: `OpenAI ${r.status}: ${(await r.text()).slice(0, 120)}` };
      }
    } catch (e) {
      out.ok = false;
      out.openai = { ok: false, error: e.message };
    }
  }

  res.json(out);
});

app.post('/api/add-note', async (req, res) => {
  const { dealId, text } = req.body || {};
  if (!dealId || !text) return res.json({ ok: false, error: 'Нужны dealId и text' });
  try {
    await addNote(dealId, text);
    res.json({ ok: true });
  } catch (e) {
    console.error('add-note:', e.message);
    res.json({ ok: false, error: e.message });
  }
});

app.post('/api/move-deal', async (req, res) => {
  const { dealId, statusId, pipelineId } = req.body || {};
  if (!dealId) return res.json({ ok: false, error: 'Нужен dealId' });
  try {
    await moveDeal(dealId, statusId, pipelineId);
    res.json({ ok: true });
  } catch (e) {
    console.error('move-deal:', e.message);
    res.json({ ok: false, error: e.message });
  }
});

// Полный сценарий отметки посещения. Шаги возвращаются в steps, чтобы при
// частичном сбое было видно, что успело примениться.
app.post('/api/process-attended', async (req, res) => {
  const b = req.body || {};
  if (!b.dealId) return res.json({ ok: false, error: 'Нужен dealId' });

  const grades = {
    'Коммуникация': b.communication || '',
    'Работа в команде': b.teamwork || '',
    'Компьютерные навыки': b.compSkills || '',
    'Самостоятельность': b.independence || '',
    'Характер': b.character || '',
  };

  const steps = [];
  let feedback = '';
  try {
    feedback = await generateFeedback(b.name, b.age, grades);
    if (feedback) steps.push('chatgpt_ok');
  } catch (e) {
    console.error('openai:', e.message);
    steps.push('chatgpt_failed');
  }
  if (!feedback) {
    feedback = buildPlainFeedback(b.name, b.age, grades);
    steps.push('used_fallback');
  }

  try {
    await addNote(b.dealId, feedback);
    steps.push('note_added');
  } catch (e) {
    console.error('note:', e.message);
    return res.json({ ok: false, steps, error: `Не удалось добавить примечание: ${e.message}` });
  }

  try {
    await moveDeal(b.dealId, b.statusId, b.pipelineId);
    steps.push('deal_moved');
  } catch (e) {
    console.error('move:', e.message);
    return res.json({ ok: false, steps, error: `Примечание добавлено, но сделка не переведена: ${e.message}` });
  }

  res.json({ ok: true, steps, feedback });
});

// ============================================================
// OnlinePBX — статистика звонков менеджеров
// ============================================================
//
// Переменные окружения:
//   PBX_DOMAIN    домен АТС, например superkid.onpbx.ru
//   PBX_AUTH_KEY  API-ключ из кабинета OnlinePBX (Интеграция → API)
//   PBX_API_HOST  по умолчанию api2.onlinepbx.ru
//
// Ключ живёт только здесь. Дашборд получает готовые цифры по внутренним
// номерам, без телефонов клиентов.

const PBX = {
  domain: (envAny('PBX_DOMAIN').value || '').replace(/^https?:\/\//, '').replace(/\/+$/, ''),
  authKey: envAny('PBX_AUTH_KEY', 'PBX_API_KEY', 'ONLINEPBX_KEY').value,
  host: process.env.PBX_API_HOST || 'api2.onlinepbx.ru',
  session: null, // { keyId, key }
};
const TZ_OFFSET_SEC = 5 * 3600; // Ташкент, без перехода на летнее время
const MIN_OK_SEC = 30;          // «дозвонились» = разговор от 30 секунд

async function pbxAuth() {
  if (!PBX.domain || !PBX.authKey) throw new Error('Не заданы PBX_DOMAIN и PBX_AUTH_KEY');
  const resp = await fetch(`https://${PBX.host}/${PBX.domain}/auth.json`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ auth_key: PBX.authKey, new: 'true' }),
  });
  const data = await resp.json().catch(() => null);
  const d = data?.data;
  if (!resp.ok || !d?.key_id || !d?.key) {
    throw new Error(`OnlinePBX: не удалось авторизоваться (${resp.status}) ${JSON.stringify(data).slice(0, 200)}`);
  }
  PBX.session = { keyId: d.key_id, key: d.key };
  return PBX.session;
}

async function pbxRequest(path, params, retry = true) {
  const s = PBX.session || await pbxAuth();
  const resp = await fetch(`https://${PBX.host}/${PBX.domain}/${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'x-pbx-authentication': `${s.keyId}:${s.key}`,
    },
    body: new URLSearchParams(params),
  });
  const text = await resp.text();
  let data = null;
  try { data = JSON.parse(text); } catch {}
  // Сессионный ключ протух — получаем новый и повторяем один раз
  const authFail = resp.status === 401 || resp.status === 403 ||
    (data && String(data.status) === '0' && /auth|key/i.test(JSON.stringify(data.comment || data.errorCode || '')));
  if (authFail && retry) {
    PBX.session = null;
    return pbxRequest(path, params, false);
  }
  if (!resp.ok || !data || String(data.status) !== '1') {
    throw new Error(`OnlinePBX ${resp.status}: ${text.slice(0, 300)}`);
  }
  return data.data;
}

const isExt = v => /^\d{2,5}$/.test(String(v ?? '').trim());

// Внутренний номер менеджера в записи звонка. Для исходящего это звонящий,
// для входящего — тот, кто ответил. Если в верхних полях его нет, ищем в
// событиях звонка (там видно, на какой номер ушёл вызов из очереди).
function findExtension(rec, dir) {
  const primary = dir === 'outbound' ? rec.caller_id_number : rec.destination_number;
  if (isExt(primary)) return String(primary).trim();
  const found = [];
  const walk = (o) => {
    if (!o || typeof o !== 'object') return;
    for (const [k, v] of Object.entries(o)) {
      if (typeof v === 'object') walk(v);
      else if (/number|user|ext|dst|destination/i.test(k) && isExt(v)) found.push(String(v).trim());
    }
  };
  walk(rec.events);
  return found.length ? found[found.length - 1] : null;
}

function dayKey(unixSec) {
  return new Date((Number(unixSec) + TZ_OFFSET_SEC) * 1000).toISOString().slice(0, 10);
}

function emptyStats() {
  return { out: 0, outOk: 0, outTalk: 0, in: 0, inAnswered: 0, inOk: 0, inTalk: 0, missed: 0 };
}

function aggregate(records) {
  const days = {};
  for (const rec of records) {
    const dir = String(rec.accountcode || '').toLowerCase();
    if (dir !== 'outbound' && dir !== 'inbound') continue; // внутренние звонки не считаем
    const talk = Number(rec.user_talk_time) || 0;
    const ext = findExtension(rec, dir) || '—';
    const day = dayKey(rec.start_stamp);
    const s = ((days[day] ||= {})[ext] ||= emptyStats());
    if (dir === 'outbound') {
      s.out++;
      s.outTalk += talk;
      if (talk >= MIN_OK_SEC) s.outOk++;
    } else {
      s.in++;
      s.inTalk += talk;
      if (talk > 0) s.inAnswered++; else s.missed++;
      if (talk >= MIN_OK_SEC) s.inOk++;
    }
  }
  return days;
}

// Кеш по дням: прошедшие дни не меняются — держим час, сегодняшний — минуту.
const dayCache = new Map(); // 'YYYY-MM-DD' -> { at, stats }

function dayBounds(dateStr) {
  const from = Math.floor(Date.parse(dateStr + 'T00:00:00Z') / 1000) - TZ_OFFSET_SEC;
  return { from, to: from + 86400 - 1 };
}

async function loadDay(dateStr) {
  const today = dayKey(Date.now() / 1000);
  const ttl = dateStr === today ? 60_000 : 3_600_000;
  const cached = dayCache.get(dateStr);
  if (cached && Date.now() - cached.at < ttl) return cached.stats;

  const { from, to } = dayBounds(dateStr);
  const records = await pbxRequest('mongo_history/search.json', {
    start_stamp_from: String(from),
    start_stamp_to: String(to),
  });
  const stats = aggregate(Array.isArray(records) ? records : [])[dateStr] || {};
  dayCache.set(dateStr, { at: Date.now(), stats });
  return stats;
}

// GET /api/pbx-calls?from=2026-09-01&to=2026-09-26
// → { ok, days: { '2026-09-26': { '100': {out, outOk, ...}, ... } } }
app.get('/api/pbx-calls', async (req, res) => {
  const re = /^\d{4}-\d{2}-\d{2}$/;
  const today = dayKey(Date.now() / 1000);
  const from = re.test(req.query.from) ? req.query.from : today;
  const to = re.test(req.query.to) ? req.query.to : from;
  const dates = [];
  for (let d = new Date(from + 'T00:00:00Z'); d <= new Date(to + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + 1)) {
    const key = d.toISOString().slice(0, 10);
    if (key > today) break;
    dates.push(key);
    if (dates.length > 93) return res.json({ ok: false, error: 'Период не больше 3 месяцев' });
  }
  try {
    const days = {};
    // Не шлём все дни разом — у АТС есть лимит на частоту запросов
    for (const d of dates) days[d] = await loadDay(d);
    res.json({ ok: true, minOkSec: MIN_OK_SEC, days, updatedAt: new Date().toISOString() });
  } catch (e) {
    console.error('pbx-calls:', e.message);
    res.json({ ok: false, error: e.message });
  }
});

// Проверка подключения: пара последних звонков с замаскированными номерами
// клиентов — чтобы убедиться, что внутренние номера определяются верно.
app.get('/api/pbx-check', async (req, res) => {
  try {
    const now = Math.floor(Date.now() / 1000);
    const records = await pbxRequest('mongo_history/search.json', {
      start_stamp_from: String(now - 3 * 86400),
      start_stamp_to: String(now),
    });
    const list = Array.isArray(records) ? records : [];
    const mask = v => (isExt(v) ? v : String(v ?? '').replace(/\d(?=\d{2})/g, '•'));
    const sample = list.slice(-5).map(r => ({
      accountcode: r.accountcode,
      caller_id_number: mask(r.caller_id_number),
      destination_number: mask(r.destination_number),
      user_talk_time: r.user_talk_time,
      duration: r.duration,
      detected_ext: findExtension(r, String(r.accountcode || '').toLowerCase()),
      fields: Object.keys(r),
    }));
    res.json({ ok: true, domain: PBX.domain, callsLast3Days: list.length, sample });
  } catch (e) {
    res.json({ ok: false, domain: PBX.domain || null, error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`amo-proxy слушает :${PORT}`);
  if (!CONFIG.amoToken) console.warn('ВНИМАНИЕ: AMO_TOKEN не задан — запросы к AmoCRM будут падать');
  if (!CONFIG.openaiKey) console.warn('OPENAI_API_KEY не задан — обратная связь соберётся из оценок');
});
