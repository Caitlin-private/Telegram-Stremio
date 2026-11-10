// Selection is scoped to this page visit and retained while paging through results.
const selectedMedia = new Map();
let visibleMedia = [];
let bulkBusy = false;
let bulkCatalogs = [];

function mediaSelectionKey(item) {
    return `${item.media_type}:${item.db_index}:${item.tmdb_id}`;
}

function syncMediaSelection() {
    document.querySelectorAll('[data-media-select]').forEach(input => {
        input.checked = selectedMedia.has(input.dataset.key);
        input.disabled = bulkBusy || isLoading;
        input.closest('[data-media-card]').classList.toggle('ring-2', input.checked);
        input.closest('[data-media-card]').classList.toggle('ring-primary', input.checked);
    });
    const count = visibleMedia.filter(item => selectedMedia.has(mediaSelectionKey(item))).length;
    const all = document.getElementById('bulk-select-page');
    all.checked = visibleMedia.length > 0 && count === visibleMedia.length;
    all.indeterminate = count > 0 && count < visibleMedia.length;
    all.disabled = bulkBusy || isLoading || !visibleMedia.length;
    document.getElementById('bulk-count').textContent = `${selectedMedia.size} selected (maximum 100)`;
    document.getElementById('bulk-clear').disabled = bulkBusy || !selectedMedia.size;
    document.getElementById('bulk-delete').disabled = bulkBusy || isLoading || !selectedMedia.size;
    document.getElementById('bulk-add').disabled = bulkBusy || isLoading || !selectedMedia.size || !getBulkCatalogIds().length;
    document.getElementById('bulk-catalog-controls').disabled = bulkBusy;
    document.querySelectorAll('[data-single-delete]').forEach(button => { button.disabled = bulkBusy; });
}

function toggleMediaSelection(input) {
    if (bulkBusy) return;
    const item = visibleMedia.find(item => mediaSelectionKey(item) === input.dataset.key);
    if (!item) return;
    if (input.checked) {
        if (selectedMedia.size >= 100 && !selectedMedia.has(input.dataset.key)) {
            showToast('Select up to 100 titles per batch.', 'error', 'Selection limit');
        } else selectedMedia.set(input.dataset.key, item);
    } else selectedMedia.delete(input.dataset.key);
    syncMediaSelection();
}

function selectVisibleMedia(checked) {
    if (bulkBusy) return;
    for (const item of visibleMedia) {
        const key = mediaSelectionKey(item);
        if (!checked) selectedMedia.delete(key);
        else if (selectedMedia.size < 100 || selectedMedia.has(key)) selectedMedia.set(key, item);
        else {
            showToast('Select up to 100 titles per batch.', 'error', 'Selection limit');
            break;
        }
    }
    syncMediaSelection();
}

function clearMediaSelection() {
    if (bulkBusy) return;
    selectedMedia.clear();
    syncMediaSelection();
}

function getBulkCatalogIds() {
    return [...document.querySelectorAll('#bulk-catalog-list input:checked')].map(input => input.value);
}

function bulkCatalogChanged(input) {
    const selected = bulkCatalogs.find(c => c._id === input.value);
    if (input.checked) {
        document.querySelectorAll('#bulk-catalog-list input').forEach(other => {
            const catalog = bulkCatalogs.find(c => c._id === other.value);
            if (other !== input && (selected?.exclusive || catalog?.exclusive)) other.checked = false;
        });
    }
    syncMediaSelection();
}

async function loadBulkCatalogs() {
    const box = document.getElementById('bulk-catalog-list');
    try {
        const res = await fetch('/api/media/manual-add/catalogs');
        if (!res.ok) throw new Error('Could not load catalogues.');
        const data = await res.json();
        bulkCatalogs = data.catalogs || [];
        box.replaceChildren();
        if (!bulkCatalogs.length) box.textContent = 'No custom catalogues. Create one from Catalogs first.';
        for (const catalog of bulkCatalogs) {
            const label = document.createElement('label');
            label.className = 'flex items-center gap-2 p-2 rounded-xl hover:bg-surface-2 cursor-pointer';
            const input = document.createElement('input');
            input.type = 'checkbox';
            input.value = catalog._id;
            input.className = 'accent-primary';
            input.addEventListener('change', () => bulkCatalogChanged(input));
            const name = document.createElement('span');
            name.textContent = `${catalog.name}${catalog.exclusive ? ' — Exclusive' : ''}${catalog.visibility !== 'public' ? ' — Restricted' : ''}`;
            label.append(input, name);
            box.append(label);
        }
        syncMediaSelection();
    } catch (error) {
        box.textContent = error.message;
    }
}

async function runMediaBatch(action) {
    if (bulkBusy || isLoading || !selectedMedia.size) return;
    const chosen = [...selectedMedia.values()];
    const catalogIds = getBulkCatalogIds();
    if (action === 'add_to_catalogs' && !catalogIds.length) {
        showToast('Select at least one catalogue.', 'error', 'Catalogue required');
        return;
    }
    const warning = action === 'delete'
        ? `Delete ${chosen.length} selected titles?\n\nThis removes entire movies/series, all their streams and catalogue references, and queues their backing Telegram messages for deletion. This cannot be undone.`
        : `Add ${chosen.length} selected titles to ${catalogIds.length} catalogue(s)?\n\nRestricted catalogues apply their visibility to the titles. Exclusive catalogues remove titles from other catalogues.`;
    if (!confirm(warning)) return;
    const payload = {
        action, catalog_ids: catalogIds, confirm_delete: action === 'delete',
        items: chosen.map(item => ({media_type: item.media_type, tmdb_id: item.tmdb_id, db_index: item.db_index}))
    };
    const status = document.getElementById('bulk-result');
    status.textContent = `Processing ${chosen.length} titles…`;
    bulkBusy = true;
    syncMediaSelection();
    try {
        const res = await fetch('/api/media/bulk', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || 'Batch request failed. Refresh before retrying.');
        const errors = [];
        for (const result of data.results || []) {
            const key = mediaSelectionKey(result);
            if (result.ok) selectedMedia.delete(key);
            else errors.push(`${selectedMedia.get(key)?.title || result.tmdb_id}: ${result.error}${result.catalog_ids?.length ? ' (some catalogue assignments completed)' : ''}`);
        }
        status.textContent = `${data.succeeded} succeeded; ${data.failed} failed.${errors.length ? '\nFailed titles remain selected:\n' + errors.join('\n') : ''}`;
        showToast(`${data.succeeded} succeeded; ${data.failed} failed.`, data.failed ? 'error' : 'success', 'Batch complete');
    } catch (error) {
        status.textContent = error.message;
        showToast(error.message, 'error', 'Batch failed');
    } finally {
        bulkBusy = false;
        syncMediaSelection();
        await loadMedia(currentPage, currentSearch);
    }
}

// Delegated pagination avoids embedding search text inside inline event handlers.
function renderLibraryPagination(page, totalPages) {
    const wrapper = document.getElementById('pagination');
    const box = document.getElementById('pagination-inner');
    box.replaceChildren();
    wrapper.classList.toggle('hidden', totalPages <= 1);
    if (totalPages <= 1) return;
    function button(label, target, active = false) {
        const b = document.createElement('button');
        b.type = 'button';
        b.textContent = label;
        b.className = `px-4 py-2 rounded-2xl text-sm font-bold border border-hairline ${active ? 'bg-primary text-white' : 'glass-panel hover:bg-surface-2 text-text'}`;
        if (active) b.setAttribute('aria-current', 'page');
        b.addEventListener('click', () => loadMedia(target, currentSearch));
        box.append(b);
    }
    if (page > 1) button('Previous', page - 1);
    const pages = [...new Set([1, ...Array.from({length: 5}, (_, i) => page - 2 + i), totalPages])]
        .filter(p => p >= 1 && p <= totalPages).sort((a, b) => a - b);
    let last = 0;
    for (const p of pages) {
        if (last && p > last + 1) box.append(document.createTextNode(' … '));
        button(String(p), p, p === page);
        last = p;
    }
    if (page < totalPages) button('Next', page + 1);
}

document.addEventListener('DOMContentLoaded', () => {
    loadBulkCatalogs();
    syncMediaSelection();
});
