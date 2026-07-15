/**
 * Local backend for the Facebook scraper web UI.
 * ==============================================
 * Runs the Puppeteer scraper and exposes its progress via a simple pollable
 * status endpoint (the UI polls it ~once/second for the live post count). We use
 * polling rather than a streamed SSE connection because the Vite dev proxy
 * buffers streams, which made the live count appear stuck.
 *
 * Endpoints:
 *   POST /api/start    { pageUrl, mode, value, speed, useProxy, waitForLogin }
 *   GET  /api/status   → { phase, count, running, posts?, error? }
 *   POST /api/continue → release the manual gate (used with waitForLogin)
 *   POST /api/stop     → stop the current run early
 *   GET  /api/info     → { proxyConfigured }
 *   POST /api/csv      { posts, fields } → CSV download
 *
 * Single run at a time (local, single-user tool).
 */

'use strict';

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const express = require('express');
const { Scraper, toCsv } = require('../lib/scraperCore');

const PORT = process.env.PORT || 5174;
const app = express();
app.use(express.json());

// ---- proxy config from .env ------------------------------------------------
const PROXY_CONFIGURED = !!(process.env.PROXY_SERVER || '').trim();
function proxyConfig(useProxy) {
  if (!useProxy) return {};
  if (!PROXY_CONFIGURED) return null; // caller turns this into a 400
  return {
    USE_PROXY: true,
    PROXY_SERVER: process.env.PROXY_SERVER.trim(),
    PROXY_USERNAME: (process.env.PROXY_USERNAME || '').trim(),
    PROXY_PASSWORD: (process.env.PROXY_PASSWORD || '').trim(),
  };
}

// ---- run state (polled by the UI) ------------------------------------------
let scraper = null;
const run = {
  phase: 'idle', // idle|launching|navigating|awaiting-gate|capturing|scraping|scouting|scouted|done|error|stopped
  count: 0,
  posts: [],
  error: null,
  stopReason: null, // why pagination ended (facebook-cap | parser-gap | end-of-feed | ...)
  scout: null, // scout result { birthYear, oldestTimestamp, span, suggestedChains, ... }
  scoutYear: null, // year currently being probed (live scout feedback)
  windows: {}, // per-window progress for the live grid: key -> { key, kind, year, month, state, count }
  chainsActive: 0, // how many windows are paginating right now
  concurrency: 0, // planned max concurrent chains
};

// Recompute the number of windows currently in the 'active' state.
function countActive() {
  return Object.values(run.windows).filter((w) => w.state === 'active').length;
}

// ---- speed presets ---------------------------------------------------------
const SPEED = {
  safe: { DELAY_MIN_MS: 1500, DELAY_MAX_MS: 3000, BATCH_SIZE: 20 },
  balanced: { DELAY_MIN_MS: 900, DELAY_MAX_MS: 1800, BATCH_SIZE: 25 },
  fast: { DELAY_MIN_MS: 400, DELAY_MAX_MS: 900, BATCH_SIZE: 30 },
};
function configForSpeed(speed) {
  return SPEED[speed] || SPEED.balanced;
}

// ---- scope → stop conditions ----------------------------------------------
function configForScope(mode, value) {
  const v = Number(value) || 0;
  switch (mode) {
    case 'count':
      return { TARGET_COUNT: v > 0 ? v : 100 };
    case 'date':
      return { DATE_CUTOFF: v > 0 ? v : 0 };
    case 'pages':
      return { MAX_ROUNDS: v > 0 ? v : 10 };
    case 'all':
    default:
      return {};
  }
}

// ---- POST /api/start -------------------------------------------------------
app.post('/api/start', (req, res) => {
  if (scraper) {
    return res.status(409).json({ error: 'A scrape is already running.' });
  }
  const { pageUrl, mode, value, speed, useProxy, waitForLogin, chains, months } =
    req.body || {};
  if (!pageUrl || !/^https?:\/\/.+facebook\.com/i.test(pageUrl)) {
    return res
      .status(400)
      .json({ error: 'Please provide a valid facebook.com page URL.' });
  }

  const px = proxyConfig(useProxy);
  if (px === null) {
    return res.status(400).json({
      error:
        'Proxy requested but not configured. Add PROXY_SERVER to your .env file.',
    });
  }

  // Reset run state.
  run.phase = 'launching';
  run.count = 0;
  run.posts = [];
  run.error = null;
  run.stopReason = null;
  run.windows = {};
  run.chainsActive = 0;
  run.concurrency = 0;

  scraper = new Scraper({
    PAGE_URL: pageUrl,
    HEADLESS: false,
    // In login-wait mode, DON'T auto-close the login popup — the user needs it
    // to log in (logging in unlocks far deeper pagination than ~100 posts).
    AUTO_DISMISS_MODAL: !waitForLogin,
    // Parallel-by-year chain count (1 = classic single feed chain). Capped at
    // 21 = the max number of year windows (2006–present); the scraper clamps
    // again to the actual window count.
    CONCURRENCY: Math.max(1, Math.min(21, Number(chains) || 1)),
    // Auto-split dense years into monthly sub-chains to keep all chains busy.
    SUBDIVIDE_MONTHS: !!months,
    ...configForSpeed(speed),
    ...configForScope(mode, value),
    ...px,
  });

  // Gate handling: in normal mode auto-continue after a short pause; in
  // login-wait mode wait for the user to click Continue (POST /api/continue).
  if (!waitForLogin) {
    scraper.on('gate', () =>
      setTimeout(() => scraper && scraper.continue(), 2500)
    );
  }

  scraper.on('status', (s) => {
    run.phase = s.phase;
  });
  // Full window plan (parallel-by-year): seed every year cell up front.
  scraper.on('plan', (p) => {
    run.concurrency = p.concurrency || 0;
    run.windows = {};
    for (const w of p.windows || []) run.windows[w.key] = { ...w };
    run.chainsActive = countActive();
  });
  // Per-window lifecycle transitions (queued → active → done/split).
  scraper.on('window', (w) => {
    const cur =
      run.windows[w.key] ||
      (run.windows[w.key] = { key: w.key, count: 0, state: 'queued' });
    if (w.state) cur.state = w.state;
    if (w.kind) cur.kind = w.kind;
    if (typeof w.year === 'number') cur.year = w.year;
    if (typeof w.month === 'number') cur.month = w.month;
    if (typeof w.count === 'number') cur.count = w.count;
    run.chainsActive = countActive();
  });
  scraper.on('progress', (p) => {
    run.count = p.total;
    if (!p.window) return;
    const cur =
      run.windows[p.window] ||
      (run.windows[p.window] = { key: p.window, count: 0, state: 'active' });
    cur.state = 'active';
    if (p.kind) cur.kind = p.kind;
    if (typeof p.year === 'number') cur.year = p.year;
    if (typeof p.month === 'number') cur.month = p.month;
    // Show the real total in the window, not "new this run" (which is ~0 on a
    // resume and made every cell misleadingly show 0).
    if (typeof p.windowTotal === 'number') cur.count = p.windowTotal;
    run.chainsActive = countActive();
  });
  scraper.on('log', (msg) => console.log('[scraper]', msg));
  scraper.on('done', (d) => {
    run.stopReason = d.stopReason || null;
  });
  scraper.on('error', (e) => {
    run.error = e.message;
  });

  (async () => {
    try {
      const posts = await scraper.run();
      run.posts = posts || [];
      run.count = run.posts.length;
      // scraper sets phase to 'done' or 'stopped' via the status event.
    } catch (err) {
      run.error = err.message;
      run.phase = 'error';
    } finally {
      try {
        await scraper.close();
      } catch {}
      scraper = null;
    }
  })();

  res.json({ ok: true });
});

// ---- POST /api/scout -------------------------------------------------------
// Cheaply find a page's date span (oldest post / active years) without a full
// scrape, so the user can size their chain count before committing.
app.post('/api/scout', (req, res) => {
  if (scraper) {
    return res
      .status(409)
      .json({ error: 'Busy — a scrape or scout is already running.' });
  }
  const { pageUrl, useProxy, waitForLogin } = req.body || {};
  if (!pageUrl || !/^https?:\/\/.+facebook\.com/i.test(pageUrl)) {
    return res
      .status(400)
      .json({ error: 'Please provide a valid facebook.com page URL.' });
  }
  const px = proxyConfig(useProxy);
  if (px === null) {
    return res.status(400).json({
      error:
        'Proxy requested but not configured. Add PROXY_SERVER to your .env file.',
    });
  }

  run.phase = 'launching';
  run.error = null;
  run.scout = null;
  run.scoutYear = null;

  scraper = new Scraper({
    PAGE_URL: pageUrl,
    HEADLESS: false,
    AUTO_DISMISS_MODAL: !waitForLogin,
    ...px,
  });
  if (!waitForLogin) {
    scraper.on('gate', () =>
      setTimeout(() => scraper && scraper.continue(), 2500)
    );
  }
  scraper.on('status', (s) => {
    run.phase = s.phase;
  });
  scraper.on('progress', (p) => {
    if (p.scoutYear) run.scoutYear = p.scoutYear;
  });
  scraper.on('log', (msg) => console.log('[scout]', msg));
  scraper.on('error', (e) => {
    run.error = e.message;
  });

  (async () => {
    try {
      const result = await scraper.scout();
      run.scout = result;
      if (result) run.phase = 'scouted';
      else if (run.phase !== 'stopped') run.phase = 'error';
    } catch (err) {
      run.error = err.message;
      run.phase = 'error';
    } finally {
      try {
        await scraper.close();
      } catch {}
      scraper = null;
    }
  })();

  res.json({ ok: true });
});

// ---- GET /api/status (polled) ----------------------------------------------
app.get('/api/status', (req, res) => {
  const finished = ['done', 'stopped', 'error'].includes(run.phase);
  res.json({
    phase: run.phase,
    count: run.count,
    running: !!scraper,
    error: run.error,
    stopReason: run.stopReason,
    scout: run.scout,
    scoutYear: run.scoutYear,
    // Live window grid (parallel-by-year runs only; empty for single-chain).
    windows: Object.values(run.windows),
    chainsActive: run.chainsActive,
    concurrency: run.concurrency,
    // Only ship the (potentially large) posts array once finished.
    posts: finished ? run.posts : undefined,
  });
});

// ---- POST /api/continue (release the manual gate) --------------------------
app.post('/api/continue', (req, res) => {
  if (scraper) scraper.continue();
  res.json({ ok: true });
});

// ---- POST /api/stop --------------------------------------------------------
app.post('/api/stop', (req, res) => {
  if (scraper) scraper.stop();
  res.json({ ok: true });
});

// ---- GET /api/info ---------------------------------------------------------
app.get('/api/info', (req, res) => {
  res.json({ proxyConfigured: PROXY_CONFIGURED });
});

// ---- POST /api/csv ---------------------------------------------------------
app.post('/api/csv', (req, res) => {
  const posts = (req.body && req.body.posts) || [];
  const fields = req.body && req.body.fields;
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename="posts.csv"');
  res.send(toCsv(posts, fields));
});

// ---- serve the built React app ---------------------------------------------
const dist = path.join(__dirname, '..', 'web', 'dist');
app.use(express.static(dist));
app.get('*', (req, res) => {
  res.sendFile(path.join(dist, 'index.html'), (err) => {
    if (err) {
      res
        .status(200)
        .send('Frontend not built yet. Run `pnpm build` (or use `pnpm dev`).');
    }
  });
});

app.listen(PORT, () => {
  console.log(`\n  Facebook scraper UI → http://localhost:${PORT}\n`);
});
