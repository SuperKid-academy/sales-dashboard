// Собирает v2/index.html — новый дизайн поверх рабочего index.html.
//
// Источник логики один — index.html в корне. v2 добавляет только тему
// (v2/theme.css) и боковое меню (v2/shell.js). После любой правки
// index.html пересобрать:  node scripts/build-v2.mjs
//
// Что делает сборка:
//  1. Пути к plans.json и version.json — на уровень выше (страница в /v2/).
//  2. Жёсткие цвета старой тёмной темы → токены, чтобы работала светлая тема.
//     В канвас-графиках CSS-переменные не работают — там цвет читается
//     из темы в момент отрисовки через cssVar().
//  3. Подключает шрифты, theme.css и shell.js; тему ставит до первой отрисовки.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
let h = fs.readFileSync(path.join(root, 'index.html'), 'utf8');

function must(from, to, label) {
  if (!h.includes(from)) throw new Error(`build-v2: не найдено «${label}» — index.html изменился, поправь сборку`);
  h = h.split(from).join(to);
}

// 1. Пути.
must("fetch('version.json?t='", "fetch('../version.json?t='", 'version.json');
must('const PLANS_URL = PLANS_FILE;', "const PLANS_URL = '../' + PLANS_FILE;", 'PLANS_URL');

// 1b. План продаж в v2 — общий на отдел, без «Кол-во менеджеров» и плана на
// одного (их прячет theme.css). Колонку с итогом называем просто «План на месяц».
must('<th style="width:180px">Итого на отдел</th>', '<th style="width:180px">План на месяц</th>', 'plan header');

// 2a. Цвета в основном <style> (кроме блока :root — его переопределяет theme.css).
{
  const s = h.indexOf('<style>');
  const e = h.indexOf('</style>', s);
  const rootEnd = h.indexOf('}', h.indexOf(':root', s)) + 1;
  let css = h.slice(rootEnd, e);
  css = css
    .replace(/rgba\(99,\s*102,\s*241,\s*([0-9.]+)\)/g, (_, a) => `color-mix(in srgb, var(--accent) ${Math.round(parseFloat(a) * 100)}%, transparent)`)
    .replace(/#1a1d27\b/gi, 'var(--card)')
    .replace(/#2a2d3a\b/gi, 'var(--border)')
    .replace(/#0f1117\b/gi, 'var(--bg)')
    .replace(/#e4e4e7\b/gi, 'var(--text)')
    .replace(/#6366f1\b/gi, 'var(--accent)')
    .replace(/#818cf8\b/gi, 'var(--accent-hover)');
  h = h.slice(0, rootEnd) + css + h.slice(e);
}

// 2b. Инлайн-стили и канвас.
must('background:rgba(30,32,44,0.95)', 'background:var(--card)', 'tooltip bg');
h = h
  .replace(/ctx\.(fillStyle|strokeStyle)\s*=\s*'#8b8d97'/g, "ctx.$1 = cssVar('--text-dim')")
  .replace(/ctx\.(fillStyle|strokeStyle)\s*=\s*'#2a2d3a'/g, "ctx.$1 = cssVar('--border')")
  .replace(/ctx\.(fillStyle|strokeStyle)\s*=\s*'#6366f1'/g, "ctx.$1 = cssVar('--accent')")
  .replace("addColorStop(0, 'rgba(99,102,241,0.3)')", "addColorStop(0, cssVar('--chart-fill-top'))")
  .replace("addColorStop(1, 'rgba(99,102,241,0)')", "addColorStop(1, cssVar('--chart-fill-bottom'))")
  .replace('<div style="color:#8b8d97;margin-bottom:2px">', '<div style="color:var(--text-dim);margin-bottom:2px">');

// 3. Подключения.
const headInject = `
    <script>
      // Тема до первой отрисовки, чтобы не мигало. cssVar — для канвас-графиков.
      try { var t = localStorage.getItem('sk-theme'); if (t) document.documentElement.dataset.theme = t; } catch (e) {}
      window.cssVar = function (n) { return getComputedStyle(document.documentElement).getPropertyValue(n).trim(); };
    </script>`;
must('<head>', '<head>' + headInject, '<head>');
must('<title>Superkid — Дашборд ОП</title>', '<title>Superkid — Дашборд v2</title>', 'title');
{
  const e = h.indexOf('</style>') + '</style>'.length;
  h = h.slice(0, e) + `
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=Unbounded:wght@500;600&display=swap">
    <link rel="stylesheet" href="theme.css?v=${Date.now()}">` + h.slice(e);
}
// insights.js — до shell.js: меню вызывает V2Insights.render для своих страниц.
must('</body>', `<script src="insights.js?v=${Date.now()}"></script>\n<script src="shell.js?v=${Date.now()}"></script>\n</body>`, '</body>');

// Комментарий — после <!DOCTYPE>, иначе браузер уйдёт в quirks mode.
must('<!DOCTYPE html>', '<!DOCTYPE html>\n<!-- СГЕНЕРИРОВАНО scripts/build-v2.mjs из index.html. Не править вручную. -->', 'doctype');
fs.writeFileSync(path.join(root, 'v2', 'index.html'), h);
console.log('v2/index.html собран,', (h.length / 1024).toFixed(0), 'KB');
