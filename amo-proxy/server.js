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

import express from 'express';

const app = express();
app.use(express.json({ limit: '1mb' }));

const CONFIG = {
  amoDomain: process.env.AMO_DOMAIN || 'superkid.amocrm.ru',
  amoToken: process.env.AMO_TOKEN || '',
  openaiKey: process.env.OPENAI_API_KEY || '',
  allowedOrigin: process.env.ALLOWED_ORIGIN || 'https://dashboard.superkid.uz',
  // Куда переводить сделку после отметки посещения
  pipelineId: Number(process.env.OU_PIPELINE_ID || 5326345),
  statusId: Number(process.env.OU_ATTENDED_STATUS_ID || 87908298),
};

// Дашборд открыт на другом домене, поэтому нужен CORS. Preflight отвечаем
// сразу, иначе браузер не отправит сам запрос.
app.use((req, res, next) => {
  res.set('Access-Control-Allow-Origin', CONFIG.allowedOrigin);
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
    time: new Date().toISOString(),
  });
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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`amo-proxy слушает :${PORT}`);
  if (!CONFIG.amoToken) console.warn('ВНИМАНИЕ: AMO_TOKEN не задан — запросы к AmoCRM будут падать');
  if (!CONFIG.openaiKey) console.warn('OPENAI_API_KEY не задан — обратная связь соберётся из оценок');
});
