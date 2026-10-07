// Тонкий REST-клиент к Supabase. Используем сервисный ключ (service_role) —
// он есть только у сервера, обходит RLS, может писать в любые таблицы.
//
// Env vars:
//   SUPABASE_URL          = https://xxxx.supabase.co
//   SUPABASE_SERVICE_KEY  = sb_secret_... (или старый eyJ... — обе формы работают)

function cfg() {
    const url = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
    const key = process.env.SUPABASE_SERVICE_KEY || '';
    if (!url || !key) {
        throw new Error('SUPABASE_URL и SUPABASE_SERVICE_KEY должны быть заданы в env.');
    }
    return { url, key };
}

export async function sbFetch(path, options = {}) {
    const { url, key } = cfg();
    const full = url + '/rest/v1' + path;
    const headers = {
        apikey: key,
        Authorization: 'Bearer ' + key,
        'Content-Type': 'application/json',
        ...(options.headers || {}),
    };
    const res = await fetch(full, {
        method: options.method || 'GET',
        headers,
        body: options.body ? JSON.stringify(options.body) : undefined,
    });
    if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`Supabase ${options.method || 'GET'} ${path} → ${res.status}: ${text.slice(0, 500)}`);
    }
    if (res.status === 204) return null;
    const ct = res.headers.get('content-type') || '';
    if (!ct.includes('application/json')) return null;
    return res.json();
}

/**
 * Пакетный upsert по primary key. PostgREST ждёт заголовок
 * Prefer: resolution=merge-duplicates. Разбиваем на чанки по 500 —
 * очень большие тела он отклоняет.
 */
export async function upsertRows(table, rows, { onConflict = 'id', chunk = 500 } = {}) {
    if (!rows || !rows.length) return 0;
    let total = 0;
    for (let i = 0; i < rows.length; i += chunk) {
        const batch = rows.slice(i, i + chunk);
        await sbFetch(`/${table}?on_conflict=${encodeURIComponent(onConflict)}`, {
            method: 'POST',
            headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
            body: batch,
        });
        total += batch.length;
    }
    return total;
}

/** Простое key/value: храним маркеры last_sync_ts в таблице public.sync_state. */
export async function getSyncState(key) {
    const rows = await sbFetch(`/sync_state?key=eq.${encodeURIComponent(key)}&select=value`);
    return rows && rows[0] ? rows[0].value : null;
}

export async function setSyncState(key, value) {
    await sbFetch(`/sync_state?on_conflict=key`, {
        method: 'POST',
        headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
        body: [{ key, value: String(value), updated_at: new Date().toISOString() }],
    });
}
