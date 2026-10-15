// =====================================================================
// Superkid Dashboard v2 — оболочка поверх рабочего index.html.
//
// Не дублирует логику: боковое меню просто нажимает существующие кнопки
// (data-dept / data-tab / data-ou-tab / data-ftab), поэтому вход, доступы,
// загрузка данных, «Был на уроке», билеты, планы работают как в основном
// дашборде. Пункт меню скрывается, если соответствующая кнопка скрыта
// правами пользователя (класс .hidden).
// =====================================================================
(function () {
  const ICONS = {
    marketing: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 11l18-8-8 18-2-8z"/></svg>',
    sales: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 17l6-6 4 4 8-8"/><path d="M15 7h6v6"/></svg>',
    ou: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="12" rx="2"/><path d="M8 21h8M12 17v4"/></svg>',
  };

  // Раздел → блоки. sel — кнопка, которую нажимаем; dept — отдел старого дашборда.
  const SECTIONS = [
    { id: 'marketing', title: 'Маркетинг', items: [
      // custom — страница только v2 (v2/insights.js), без кнопки в старом дашборде.
      // Когорты входят в неё же: это одна и та же разбивка по неделям.
      { label: 'Каналы и CAC', custom: 'channels', dept: 'marketing' },
      { label: 'Размещения в Telegram', custom: 'tg', dept: 'marketing' },
      { label: 'Реклама Meta', dept: 'marketing' },
    ]},
    { id: 'sales', title: 'Продажи', items: [
      { label: 'Статистика', dept: 'sales', sel: '[data-tab="statistics"]' },
      { label: 'План', dept: 'sales', sel: '[data-tab="salesplan"]' },
      { label: 'KPI менеджеров', dept: 'sales', sel: '[data-tab="plans"]' },
      { label: 'Зарплата', dept: 'sales', sel: '[data-tab="salary"]' },
      { label: 'Старт групп', dept: 'sales', sel: '[data-tab="groupstarts"]' },
      { label: 'Продления', dept: 'sales', sel: '[data-tab="renewals"]' },
      { label: 'Продления 2.0', dept: 'sales', sel: '[data-tab="renewals2"]' },
      { label: 'Звонки', dept: 'sales', sel: '[data-tab="calls"]' },
      { sub: 'Прогноз' },
      { label: 'Прогноз продаж', dept: 'forecast', sel: '[data-ftab="fsales"]' },
      { label: 'Доплаты', dept: 'forecast', sel: '[data-ftab="fsurcharges"]' },
      { label: 'Повторные продажи', dept: 'forecast', sel: '[data-ftab="frenewals"]' },
    ]},
    { id: 'ou', title: 'Открытые уроки', items: [
      { label: 'Уроки', dept: 'ou', sel: '[data-ou-tab="lessons"]' },
      { label: 'Оплаты после ОУ', dept: 'ou', sel: '[data-ou-tab="payments"]' },
      { label: 'Расписание', dept: 'ou', sel: '[data-ou-tab="schedule"]' },
    ]},
  ];

  const HOME_ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 11l9-7 9 7"/><path d="M5 10v10h14V10"/><path d="M10 20v-6h4v6"/></svg>';
  const $ = (s, r = document) => r.querySelector(s);
  const app = $('#mainApp');
  if (!app) return;

  // ---------- Тема ----------
  const root = document.documentElement;
  function isDark() {
    return root.dataset.theme ? root.dataset.theme === 'dark' : matchMedia('(prefers-color-scheme: dark)').matches;
  }
  function toggleTheme() {
    root.dataset.theme = isDark() ? 'light' : 'dark';
    try { localStorage.setItem('sk-theme', root.dataset.theme); } catch (e) {}
    // Канвас-графики берут цвета темы в момент отрисовки — перерисуем текущий экран.
    window.dispatchEvent(new Event('resize'));
  }

  // ---------- Разметка оболочки ----------
  const side = document.createElement('aside');
  side.className = 'v2-side';
  side.setAttribute('aria-label', 'Разделы дашборда');
  side.innerHTML = `
    <div class="v2-logo"><i></i>Superkid</div>
    <nav class="v2-nav">
      <button class="v2-home-btn" type="button" id="v2HomeBtn">${HOME_ICON}Главная</button>${SECTIONS.map(sec => `
      <div class="v2-sec" data-sec="${sec.id}">
        <button class="v2-sec-h" type="button" aria-expanded="true">${ICONS[sec.id]}${sec.title}<span class="v2-caret">▾</span></button>
        <div class="v2-sec-b">${sec.items.map((it, i) => it.sub
          ? `<div class="v2-sec-sub">${it.sub}</div>`
          : `<button class="v2-item" type="button" data-sec="${sec.id}" data-i="${i}">${it.label}</button>`).join('')}
        </div>
      </div>`).join('')}
    </nav>
    <div class="v2-foot"><span><span class="v2-dot"></span>AmoCRM → Supabase</span><span>обновление каждые 15 мин</span></div>`;

  const main = document.createElement('div');
  main.className = 'v2-main';
  while (app.firstChild) main.appendChild(app.firstChild);
  app.appendChild(side);
  app.appendChild(main);

  // Контейнер для страниц, которых нет в старом дашборде («Главная», «Каналы и CAC»).
  const customPage = document.createElement('section');
  customPage.className = 'v2-custom-page';
  customPage.id = 'v2CustomPage';
  main.appendChild(customPage);
  let currentCustom = null;
  const homeBtn = $('#v2HomeBtn', side);

  // Заголовок страницы и переключатель темы — в шапку.
  const header = $('.header', main);
  if (header) {
    const title = document.createElement('div');
    title.className = 'v2-title';
    title.innerHTML = '<span class="v2-crumb" id="v2Crumb"></span><h2 class="v2-page" id="v2Page"></h2>';
    header.insertBefore(title, header.firstChild);
    const controls = header.lastElementChild;
    const tbtn = document.createElement('button');
    tbtn.className = 'v2-theme';
    tbtn.type = 'button';
    tbtn.title = 'Светлая / тёмная тема';
    tbtn.setAttribute('aria-label', 'Переключить тему');
    tbtn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>';
    tbtn.addEventListener('click', toggleTheme);
    controls && controls.insertBefore(tbtn, controls.firstChild);
  }

  // ---------- Навигация ----------
  const itemOf = btn => SECTIONS.find(s => s.id === btn.dataset.sec).items[+btn.dataset.i];
  const deptBtn = dept => $(`.dept-tab[data-dept="${dept}"]`);

  function openCustom(id) {
    currentCustom = id;
    main.classList.add('v2-custom');
    if (window.V2Insights) window.V2Insights.render(id, customPage);
    else customPage.innerHTML = '<div class="ki-notice">Модуль страницы не загрузился. Обновите страницу.</div>';
    window.scrollTo(0, 0);
  }
  function closeCustom() {
    currentCustom = null;
    main.classList.remove('v2-custom');
  }

  function open(btn) {
    const it = itemOf(btn);
    if (it.custom) {
      openCustom(it.custom);
    } else {
      closeCustom();
      const d = deptBtn(it.dept);
      if (d && !d.classList.contains('active')) d.click();
      if (it.sel) {
        const t = $(it.sel);
        if (t && !t.classList.contains('active')) t.click();
      }
    }
    try { localStorage.setItem('sk-v2-item', btn.dataset.sec + ':' + btn.dataset.i); } catch (e) {}
    sync();
  }
  function openHome() {
    openCustom('home');
    try { localStorage.setItem('sk-v2-item', 'home'); } catch (e) {}
    sync();
  }

  side.addEventListener('click', e => {
    if (e.target.closest('#v2HomeBtn')) { openHome(); return; }
    const h = e.target.closest('.v2-sec-h');
    if (h) { h.setAttribute('aria-expanded', h.getAttribute('aria-expanded') === 'true' ? 'false' : 'true'); return; }
    const b = e.target.closest('.v2-item');
    if (b) open(b);
  });

  // Видимость и подсветка пунктов повторяют состояние старых кнопок.
  function available(it) {
    const d = deptBtn(it.dept);
    if (!d || d.classList.contains('hidden')) return false;
    if (it.sel) { const t = $(it.sel); if (!t || t.classList.contains('hidden')) return false; }
    return true;
  }
  // «Главная» — сводка продаж и маркетинга: видна тем, у кого есть
  // «Статистика» продаж или отдел маркетинга.
  function homeAvailable() {
    const st = $('[data-tab="statistics"]'), sd = deptBtn('sales'), md = deptBtn('marketing');
    const sales = sd && !sd.classList.contains('hidden') && st && !st.classList.contains('hidden');
    const mkt = md && !md.classList.contains('hidden');
    return !!(sales || mkt);
  }
  function isActive(it) {
    if (it.custom) return currentCustom === it.custom;
    if (currentCustom) return false;
    const d = deptBtn(it.dept);
    if (!d || !d.classList.contains('active')) return false;
    if (!it.sel) return true;
    const t = $(it.sel);
    return !!(t && t.classList.contains('active'));
  }
  let syncing = false;
  function sync() {
    if (syncing) return;
    syncing = true;
    let current = null;
    side.querySelectorAll('.v2-sec').forEach(secEl => {
      let any = false, hasOn = false;
      secEl.querySelectorAll('.v2-item').forEach(b => {
        const it = itemOf(b);
        const ok = available(it);
        b.hidden = !ok;
        const on = ok && isActive(it);
        b.classList.toggle('on', on);
        if (on) { hasOn = true; current = { sec: secEl, it }; }
        any = any || ok;
      });
      secEl.hidden = !any;
      secEl.classList.toggle('has-on', hasOn);
    });
    homeBtn.hidden = !homeAvailable();
    homeBtn.classList.toggle('on', currentCustom === 'home');
    const crumb = $('#v2Crumb'), page = $('#v2Page');
    if (currentCustom === 'home') {
      if (crumb) crumb.textContent = 'Сводка';
      if (page) page.textContent = 'Главная';
    } else if (current) {
      const secTitle = SECTIONS.find(s => s.id === current.sec.dataset.sec).title;
      if (crumb) crumb.textContent = secTitle;
      if (page) page.textContent = current.it.label;
    }
    syncing = false;
  }

  // Права пользователя и активные вкладки меняет основной код (applyAccess,
  // обработчики вкладок) — следим за классами кнопок и пересчитываем меню.
  const watched = document.querySelectorAll('.dept-tab, #mainTabs .tab, #ouTabs .tab, #forecastTabs .tab');
  const mo = new MutationObserver(() => requestAnimationFrame(sync));
  watched.forEach(el => mo.observe(el, { attributes: true, attributeFilter: ['class'] }));

  // После входа — восстановить последний открытый блок.
  let restored = false;
  function restore() {
    if (restored || app.style.display === 'none') return;
    restored = true;
    sync();
    let saved = null;
    try { saved = localStorage.getItem('sk-v2-item'); } catch (e) {}
    // По умолчанию открываем «Главную», если она доступна пользователю.
    if (!saved || saved === 'home') {
      if (homeAvailable()) setTimeout(openHome, 0);
      return;
    }
    const [sec, i] = saved.split(':');
    const b = side.querySelector(`.v2-item[data-sec="${sec}"][data-i="${i}"]`);
    if (b && !b.hidden) setTimeout(() => open(b), 0);
    else if (homeAvailable()) setTimeout(openHome, 0);
  }
  new MutationObserver(restore).observe(app, { attributes: true, attributeFilter: ['style'] });

  sync();
  // Если пользователь уже был залогинен, основной код показал приложение
  // раньше, чем загрузилась оболочка — восстанавливаем сразу.
  restore();
})();
