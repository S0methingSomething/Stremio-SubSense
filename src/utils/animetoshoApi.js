'use strict';

/**
 * AnimeTosho.net JSON API wrapper (unified).
 *
 * One host, one queue, one detail cache, one proxy token scheme. Two search surfaces:
 *   - Classic feed: ?eids=X (AniDB episode id, TV precision), ?aids=X (whole anime), ?q=X
 *   - v1 series:    /json/v1/series/anidb/{aid} — paginated releases carrying
 *                   release_group / resolution / seeders / info_hash for free
 *   - Detail:       ?show=torrent&id=X (shared) — files[] whose subtitle attachments
 *                   each carry a language_code, a format and a working storage URL
 *
 * Storage (storage.animetosho.net) attachment URLs are not derivable from the id, so the
 * proxy token carries the real path. Attachment info uses language_code/format; the old
 * .org shapes lang/codec are still accepted so SQLite rows cached before the migration
 * keep working.
 *
 * Rate limit: one sequential queue for every request, foreground before background so a
 * speculative walk cannot push a user-facing fetch past the provider deadline, spaced by
 * the shared throttle with adaptive 429 backoff:
 *   - Base interval: 3.5s between requests
 *   - On 429: retry up to 3 times at 2s intervals
 *   - If all retries fail: penalty +10s per consecutive failure
 *   - On success: reset to base interval
 */

const { log } = require('../utils');
const rateLimit = require('../infra/rate-limit');

const FEED_URL = 'https://feed.animetosho.net/json';
const STORAGE_URL = 'https://storage.animetosho.net';
const FETCH_TIMEOUT_MS = 15000;
const USER_AGENT = 'SubSense-Stremio/2.0';
const PAGE_SIZE = 100;

const BASE_RATE_MS = 3500;
const RETRY_DELAY_MS = 2000;
const MAX_RETRIES = 3;
const PENALTY_BACKOFF_MS = 10000;

// --- Request queue: one at a time, foreground before background ---
const pending = { foreground: [], background: [] };
let running = false;
let currentInterval = BASE_RATE_MS;
let consecutiveFailures = 0;

function enqueue(fn, { background = false } = {}) {
    return new Promise((resolve, reject) => {
        pending[background ? 'background' : 'foreground'].push({ fn, resolve, reject });
        runQueue();
    });
}

async function runQueue() {
    if (running) return;
    running = true;
    try {
        for (;;) {
            const job = pending.foreground.shift() || pending.background.shift();
            if (!job) return;
            // Slots are reserved across every worker, so the interval is the addon's rate, not one process's.
            try {
                await rateLimit.throttle('animetosho', currentInterval);
                job.resolve(await job.fn());
            } catch (err) {
                job.reject(err);
            }
        }
    } finally {
        running = false;
    }
}

// --- Persistent detail cache (SQLite) ---
let _db = null;

/**
 * Initialize the torrent detail cache table.
 * Call once at startup with the shared libSQL database instance.
 */
async function initDetailCache(database) {
    _db = database;
    await _db.executeMultiple(`
        CREATE TABLE IF NOT EXISTS at_torrent_details (
            torrent_id  INTEGER PRIMARY KEY,
            data        TEXT NOT NULL,
            created_at  INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );
    `);
    log('info', '[AT-API] Detail cache table initialized');
}

async function _getCachedDetail(torrentId) {
    if (!_db) return null;
    try {
        const row = await _db.execute('SELECT data FROM at_torrent_details WHERE torrent_id = ?', [torrentId]);
        if (row.rows.length === 0) return null;
        return JSON.parse(row.rows[0].data);
    } catch { return null; }
}

async function _setCachedDetail(torrentId, data) {
    if (!_db || !data) return;
    try {
        await _db.execute(
            'INSERT OR REPLACE INTO at_torrent_details (torrent_id, data) VALUES (?, ?)',
            [torrentId, JSON.stringify(data)]
        );
    } catch (err) {
        log('debug', `[AT-API] Cache write failed for ${torrentId}: ${err.message}`);
    }
}

// --- Searches ---

/**
 * Search by AniDB episode ID — returns entries for ONE specific episode.
 * This is the PRIMARY search method for TV episodes.
 * Returns ALL entries containing this episode (including batch packs).
 */
async function searchByEpisodeId(eid) {
    return enqueue(() => fetchEntries(`${FEED_URL}?eids=${eid}`));
}

/**
 * Search by AniDB anime ID — returns entries for ALL episodes of that anime.
 * Used for MOVIES (no eid needed) and as the episode-search fallback.
 */
async function searchByAnidbId(anidbId) {
    return enqueue(() => fetchEntries(`${FEED_URL}?aids=${anidbId}`));
}

/**
 * One page of v1 series releases, newest first. The feed caps a page at 100
 * whatever ?limit asks for. Release objects carry release_group, resolution,
 * seeders and info_hash without an extra detail fetch.
 */
async function searchSeriesReleases(anidbId, offset = 0, { background = false } = {}) {
    const url = `${FEED_URL}/v1/series/anidb/${anidbId}?limit=${PAGE_SIZE}&offset=${offset}`;
    const json = await enqueue(() => _fetchWithRetry(url, `Series ${anidbId}#${offset}`), { background });
    return json?.data?.releases || [];
}

/**
 * Full torrent details including file attachments (subtitles, fonts).
 * Checks persistent SQLite cache first - torrent details are immutable, so a
 * cache hit skips the queue entirely. Otherwise the fetch is queued (foreground
 * or background) and the result cached.
 */
async function getTorrentDetail(torrentId, { background = false } = {}) {
    const cached = await _getCachedDetail(torrentId);
    if (cached) return cached;
    const data = await enqueue(
        () => _fetchWithRetry(`${FEED_URL}?show=torrent&id=${torrentId}`, `Detail ${torrentId}`),
        { background }
    );
    if (data) await _setCachedDetail(torrentId, data);
    return data;
}

async function fetchEntries(url) {
    const data = await _fetchWithRetry(url, url);
    if (!data) return [];
    if (!Array.isArray(data)) return [];
    return data.filter(e => e.status === 'complete').sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
}

/**
 * Core fetch with retry logic for 429 handling.
 * Retries up to MAX_RETRIES times at RETRY_DELAY_MS intervals.
 * If all retries fail, applies penalty backoff for the next queue item.
 * Returns parsed JSON or null on failure.
 */
async function _fetchWithRetry(url, label) {
    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
        try {
            if (attempt > 0) {
                await new Promise(r => setTimeout(r, RETRY_DELAY_MS));
            }
            const response = await fetch(url, {
                signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
                headers: { 'User-Agent': USER_AGENT }
            });
            if (response.status === 429) {
                if (attempt < MAX_RETRIES) {
                    log('debug', `[AT-API] ${label}: 429, retry ${attempt + 1}/${MAX_RETRIES}`);
                    continue;
                }
                consecutiveFailures++;
                const penalty = Math.min(PENALTY_BACKOFF_MS * consecutiveFailures, 30000);
                currentInterval = BASE_RATE_MS + penalty;
                log('warn', `[AT-API] ${label}: 429 after ${MAX_RETRIES} retries, backoff ${currentInterval}ms`);
                return null;
            }
            if (!response.ok) {
                log('warn', `[AT-API] ${label}: HTTP ${response.status}`);
                return null;
            }
            consecutiveFailures = 0;
            currentInterval = BASE_RATE_MS;
            return await response.json();
        } catch (err) {
            if (attempt === MAX_RETRIES) {
                log('error', `[AT-API] ${label}: ${err.message}`);
                return null;
            }
            log('debug', `[AT-API] ${label}: ${err.message}, retry ${attempt + 1}/${MAX_RETRIES}`);
        }
    }
    return null;
}

// --- Detail parsing ---

/** Every subtitle attachment of a release, across all of its files. */
function extractSubtitles(detail) {
    const out = [];
    for (const file of detail?.files || []) {
        const fileName = file.filename || file.name || null;
        for (const a of file.attachments || []) {
            if (a.type !== 'subtitle' || !a.url) continue;
            const info = a.info || {};
            const fmt = String(info.format || '').toLowerCase();
            out.push({
                id: a.id,
                url: a.url,
                fileName,
                languageCode: info.language_code || null,
                languageName: info.language || null,
                format: fmt === 'ssa' ? 'ass' : fmt === 'subrip' ? 'srt' : fmt,
                forced: !!info.forced,
                isDefault: !!info.default,
                title: info.title || null
            });
        }
    }
    return out;
}

/** Tracks mediainfo reports but that have no attachment, so no URL. Coverage only. */
function countMediainfoTracks(detail) {
    let count = 0;
    for (const file of detail?.files || []) {
        const mediainfo = file.info && file.info.mediainfoj;
        if (mediainfo && Array.isArray(mediainfo.subtitles)) count += mediainfo.subtitles.length;
    }
    return count;
}

// --- Proxy helpers ---

/**
 * Return the XZ download URL for an attachment.
 * On the .net storage the path is not derivable from the id alone, so the
 * URL must come from the API response.
 */
function buildAttachmentUrl(attachment) {
    if (!attachment || !attachment.url) return null;
    return String(attachment.url);
}

/**
 * Build the proxy URL for a SubSense proxy endpoint.
 * The token carries the attachment id and its storage path relative to the
 * storage root, which is all the proxy needs.
 */
function buildProxyUrl(baseUrl, attachment, fmt) {
    if (!attachment || attachment.id == null || !attachment.url) return null;
    const url = String(attachment.url);
    if (!url.startsWith(`${STORAGE_URL}/`)) return null;
    const path = url.slice(STORAGE_URL.length + 1);
    const payload = JSON.stringify({ i: attachment.id, p: path });
    const encoded = Buffer.from(payload).toString('base64url');
    return `${baseUrl}/api/animetosho/proxy/${encoded}?fmt=${fmt}`;
}

function decodeProxyToken(token) {
    try {
        const json = Buffer.from(token, 'base64url').toString('utf8');
        return JSON.parse(json);
    } catch (err) {
        return null;
    }
}

/**
 * Rebuild a storage URL from a token path. The payload uses more than one path shape, so the
 * guard pins the host instead of matching a shape. Paths may carry a query string
 * (e.g. ?filename=...), which is preserved on rebuild.
 */
function storageUrlForPath(path) {
    const p = String(path || '');
    if (!p || p.startsWith('/') || p.includes('://') || p.includes('..') || p.includes('\\')) return null;
    const file = p.split('?')[0];
    if (!file.toLowerCase().endsWith('.xz')) return null;
    return `${STORAGE_URL}/${p}`;
}

module.exports = {
    searchByEpisodeId,
    searchByAnidbId,
    searchSeriesReleases,
    getTorrentDetail,
    initDetailCache,
    extractSubtitles,
    countMediainfoTracks,
    buildAttachmentUrl,
    buildProxyUrl,
    decodeProxyToken,
    storageUrlForPath,
    PAGE_SIZE,
    FEED_URL,
    STORAGE_URL
};
