// ============================================================
// РнП → Supabase: недельные расходы на маркетинг (Meta, Telegram).
//
// Зачем: дашборд v2 считает CPL и CAC по каналам, а расходы живут
// в РнП. Таблица закрыта для чтения по ссылке, поэтому читаем её
// отсюда (скрипт работает от аккаунта с доступом) и кладём в
// Supabase → sync_state, ключ «marketing_spend». Дашборд берёт оттуда.
//
// Настройка один раз:
//   1. Этот файл — отдельным файлом в проекте Apps Script («+» → Скрипт).
//   2. Настройки проекта → Свойства скрипта → добавить
//      SUPABASE_SERVICE_KEY = sb_secret_...
//   3. Запустить syncRnpSpend (разрешить доступ к таблицам).
//   4. Запустить setupRnpSpendTrigger — дальше раз в час само.
//
// Расход: 1 запрос в Supabase за запуск, 24 в сутки — квоту не трогает.
// ============================================================

const RNP_SPEND = {
  SHEET_ID: '1L24JvptR0cvEo7EM-WOnvQFqHVG332C6OpCarU4Us3c',
  SUPABASE_URL: 'https://rxnasmyanpeagprvldql.supabase.co',
  MONTHS_RU: ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'],
  MONTHS_BACK: 2, // текущий месяц + два предыдущих: хватает на 7 недель когорт
};

function syncRnpSpend() {
  const key = PropertiesService.getScriptProperties().getProperty('SUPABASE_SERVICE_KEY');
  if (!key) throw new Error('Нет SUPABASE_SERVICE_KEY в свойствах скрипта.');

  const ss = SpreadsheetApp.openById(RNP_SPEND.SHEET_ID);
  const now = new Date();
  const weeks = {}, months = {};
  for (let o = -RNP_SPEND.MONTHS_BACK; o <= 0; o++) {
    const d = new Date(now.getFullYear(), now.getMonth() + o, 1);
    const sheet = rnpFindSheet_(ss, d.getFullYear(), d.getMonth());
    if (!sheet) { Logger.log('Нет листа РнП за ' + RNP_SPEND.MONTHS_RU[d.getMonth()] + ' ' + d.getFullYear()); continue; }
    const res = rnpParse_(sheet.getDataRange().getDisplayValues(), d.getFullYear(), d.getMonth());
    if (!res) { Logger.log('Лист «' + sheet.getName() + '»: не нашёл недели или строки расходов'); continue; }
    // Неделя на стыке месяцев есть в двух листах; полные цифры — в листе
    // нового месяца, поэтому более поздний месяц перезаписывает.
    Object.assign(weeks, res.weeks);
    months[rnpYm_(d)] = res.month;
    Logger.log('Лист «' + sheet.getName() + '»: Meta $' + res.month.meta + ', Telegram $' + res.month.tg);
  }

  const value = { source: 'rnp', weeks: weeks, months: months, read_at: new Date().toISOString() };
  const res = UrlFetchApp.fetch(RNP_SPEND.SUPABASE_URL + '/rest/v1/sync_state?on_conflict=key', {
    method: 'post',
    contentType: 'application/json',
    headers: { apikey: key, Authorization: 'Bearer ' + key, Prefer: 'resolution=merge-duplicates,return=minimal' },
    payload: JSON.stringify([{ key: 'marketing_spend', value: JSON.stringify(value), updated_at: new Date().toISOString() }]),
    muteHttpExceptions: true,
  });
  const code = res.getResponseCode();
  if (code >= 300) throw new Error('Supabase ' + code + ': ' + res.getContentText().slice(0, 300));
  Logger.log('Расходы записаны: недель ' + Object.keys(weeks).length);
}

// Листы называются по-разному: «Октябрь 26», «Август 2026».
// Требуем год в названии, чтобы не схватить «Апрель Маркетинг».
function rnpFindSheet_(ss, y, m) {
  const name = RNP_SPEND.MONTHS_RU[m].toLowerCase();
  const yy = String(y).slice(2), yyyy = String(y);
  return ss.getSheets().find(function (s) {
    const t = s.getName().toLowerCase().trim();
    return t.indexOf(name) === 0 && (t.indexOf(yyyy) !== -1 || new RegExp('\\b' + yy + '\\b').test(t));
  }) || null;
}

// Ищем по содержимому, не по номерам строк. Факт недели — ячейка справа
// от метки «[ n ] Неделя d1 - d2»; факт месяца — первый «Факт» в строке
// под метками.
function rnpParse_(rows, y, m) {
  let weekRow = -1, weekCols = [];
  for (let i = 0; i < rows.length && weekRow < 0; i++) {
    const cols = [];
    rows[i].forEach(function (c, j) {
      const mm = String(c).match(/^\[\s*\d+\s*\]\s*Неделя\s+(\d+)\s*-\s*(\d+)/);
      if (mm) cols.push({ j: j, d1: +mm[1] });
    });
    if (cols.length >= 3) { weekRow = i; weekCols = cols; }
  }
  if (weekRow < 0) return null;
  const head = rows[weekRow + 1] || [];
  const monthFactCol = head.findIndex(function (c) { return String(c).trim() === 'Факт'; });
  const weeks = weekCols.map(function (w, idx) {
    const start = (idx === 0 && w.d1 > 7) ? new Date(y, m - 1, w.d1) : new Date(y, m, w.d1);
    return { mon: rnpKey_(rnpMonday_(start)), col: w.j + 1 };
  });
  const out = { month: { meta: 0, tg: 0 }, weeks: {} };
  let found = false;
  rows.forEach(function (r) {
    const i = r.findIndex(function (c) { return String(c).trim() === 'Расходы на маркетинг'; });
    if (i < 0) return;
    const name = String(r[i + 1] || '').trim().toLowerCase();
    const ch = name === 'meta' ? 'meta' : name === 'telegram' ? 'tg' : null;
    if (!ch) return;
    found = true;
    if (monthFactCol >= 0) out.month[ch] = rnpMoney_(r[monthFactCol]);
    weeks.forEach(function (w) {
      if (!out.weeks[w.mon]) out.weeks[w.mon] = { meta: 0, tg: 0 };
      out.weeks[w.mon][ch] = rnpMoney_(r[w.col]);
    });
  });
  return found ? out : null;
}

function rnpMoney_(s) {
  const v = parseFloat(String(s || '').replace(/[$\s ]/g, '').replace(',', '.'));
  return isFinite(v) ? v : 0;
}
function rnpMonday_(d) { const x = new Date(d.getFullYear(), d.getMonth(), d.getDate()); x.setDate(x.getDate() - ((x.getDay() + 6) % 7)); return x; }
function rnpKey_(d) { return d.getFullYear() + '-' + ('0' + (d.getMonth() + 1)).slice(-2) + '-' + ('0' + d.getDate()).slice(-2); }
function rnpYm_(d) { return d.getFullYear() + '-' + ('0' + (d.getMonth() + 1)).slice(-2); }

function setupRnpSpendTrigger() {
  ScriptApp.getProjectTriggers().forEach(function (t) {
    if (t.getHandlerFunction() === 'syncRnpSpend') ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger('syncRnpSpend').timeBased().everyHours(1).create();
  Logger.log('Готово: syncRnpSpend раз в час');
}
