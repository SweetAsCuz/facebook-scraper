import React, { useState, useEffect, useRef } from 'react';

// Scope modes shown in the selector.
const MODES = [
  { key: 'all', label: 'All posts', hint: 'Scrape everything until there are no more posts.' },
  { key: 'count', label: 'By number', hint: 'Stop after a set number of posts.' },
  { key: 'date', label: 'By date', hint: 'Only posts newer than a cutoff.' },
  { key: 'pages', label: 'By pages', hint: 'Stop after N pagination requests (advanced).' },
];

const COUNT_PRESETS = [50, 100, 500];
const DAY_PRESETS = [
  { label: 'Last 7 days', days: 7 },
  { label: 'Last 30 days', days: 30 },
  { label: 'Last 90 days', days: 90 },
];

// Selectable output columns. `text` is always included and not toggleable.
const COLUMNS = [
  { key: 'timestamp', label: 'Date' },
  { key: 'author', label: 'Author' },
  { key: 'reactions', label: 'Reactions' },
  { key: 'comments', label: 'Comments' },
  { key: 'shares', label: 'Shares' },
  { key: 'url', label: 'Link' },
  { key: 'image', label: 'Image' },
];
// Sensible default selection.
const DEFAULT_COLS = ['timestamp', 'reactions', 'comments', 'shares', 'url'];

export default function App() {
  const [pageUrl, setPageUrl] = useState('https://www.facebook.com/Badmintonwithu2013');
  const [mode, setMode] = useState('all');
  const [speed, setSpeed] = useState('balanced'); // safe | balanced | fast
  const [useProxy, setUseProxy] = useState(false);
  const [proxyConfigured, setProxyConfigured] = useState(false);
  const [waitForLogin, setWaitForLogin] = useState(false);

  // per-mode inputs
  const [count, setCount] = useState(100);
  const [days, setDays] = useState(30);
  const [dateStr, setDateStr] = useState(''); // optional explicit date
  const [pages, setPages] = useState(10);

  const [status, setStatus] = useState('idle'); // idle|running|done|error|stopped
  const [phase, setPhase] = useState('idle'); // backend phase (for the gate)
  const [liveCount, setLiveCount] = useState(0);
  const [error, setError] = useState('');
  const [posts, setPosts] = useState([]);
  const [stopReason, setStopReason] = useState(null);

  // Which output columns to show/export (Set of column keys).
  const [cols, setCols] = useState(new Set(DEFAULT_COLS));
  function toggleCol(key) {
    setCols((prev) => {
      const next = new Set(prev);
      next.has(key) ? next.delete(key) : next.add(key);
      return next;
    });
  }
  // Ordered list of active columns, following COLUMNS order.
  const activeCols = COLUMNS.filter((c) => cols.has(c.key));

  const pollRef = useRef(null);

  // Ask the backend whether a proxy is configured in .env.
  useEffect(() => {
    fetch('/api/info')
      .then((r) => r.json())
      .then((d) => setProxyConfigured(!!d.proxyConfigured))
      .catch(() => {});
  }, []);

  // Stop polling on unmount.
  useEffect(() => () => clearInterval(pollRef.current), []);

  // Poll the backend for live count + finish state (robust through dev proxy).
  function startPolling() {
    clearInterval(pollRef.current);
    pollRef.current = setInterval(async () => {
      try {
        const r = await fetch('/api/status');
        const d = await r.json();
        setPhase(d.phase);
        if (typeof d.count === 'number') setLiveCount(d.count);
        if (d.stopReason) setStopReason(d.stopReason);

        if (d.phase === 'error') {
          setError(d.error || 'Scrape failed.');
          setStatus('error');
          clearInterval(pollRef.current);
        } else if (d.phase === 'done' || d.phase === 'stopped') {
          if (Array.isArray(d.posts)) setPosts(d.posts);
          setStatus((s) => (s === 'stopped' ? 'stopped' : 'done'));
          clearInterval(pollRef.current);
        }
      } catch {
        /* transient; keep polling */
      }
    }, 1000);
  }

  // Turn the selected scope into { mode, value } for the backend.
  function scopePayload() {
    if (mode === 'count') return { mode, value: Number(count) };
    if (mode === 'pages') return { mode, value: Number(pages) };
    if (mode === 'date') {
      // Explicit date wins; otherwise use the "last N days" preset.
      const cutoffMs = dateStr
        ? new Date(dateStr).getTime()
        : Date.now() - Number(days) * 86400 * 1000;
      return { mode, value: Math.floor(cutoffMs / 1000) };
    }
    return { mode: 'all', value: 0 };
  }

  async function start() {
    setStatus('running');
    setPhase('launching');
    setError('');
    setPosts([]);
    setLiveCount(0);
    setStopReason(null);
    try {
      const res = await fetch('/api/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pageUrl,
          speed,
          useProxy,
          waitForLogin,
          ...scopePayload(),
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Could not start.');
      startPolling();
    } catch (e) {
      setError(e.message);
      setStatus('error');
    }
  }

  // Release the manual gate (login-wait mode).
  async function continueRun() {
    try {
      await fetch('/api/continue', { method: 'POST' });
    } catch {
      /* ignore */
    }
  }

  async function stop() {
    setStatus('stopped');
    try {
      await fetch('/api/stop', { method: 'POST' });
    } catch {
      /* ignore */
    }
  }

  function downloadJson() {
    const blob = new Blob([JSON.stringify(posts, null, 2)], {
      type: 'application/json',
    });
    triggerDownload(blob, 'posts.json');
  }
  async function downloadCsv() {
    // Export id + text + whatever columns are currently selected.
    const fields = ['id', 'text', ...activeCols.map((c) => c.key)];
    const res = await fetch('/api/csv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ posts, fields }),
    });
    triggerDownload(await res.blob(), 'posts.csv');
  }

  // Render a single cell value based on the column key.
  function renderCell(p, key) {
    const v = p[key];
    if (key === 'timestamp')
      return v ? new Date(v * 1000).toLocaleDateString() : '';
    if (key === 'url')
      return v ? (
        <a href={v} target="_blank" rel="noreferrer">
          open
        </a>
      ) : (
        ''
      );
    if (key === 'image')
      return v ? (
        <a href={v} target="_blank" rel="noreferrer">
          <img src={v} alt="" className="thumb" />
        </a>
      ) : (
        ''
      );
    if (v === null || v === undefined) return '';
    return v;
  }
  function triggerDownload(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  const running = status === 'running';

  return (
    <div className="wrap">
      <h1>Facebook Page Scraper</h1>
      <p className="sub">
        Enter a public Facebook page URL, choose how much to scrape, and go. A
        Chrome window opens while it works — if a login/checkpoint appears, solve
        it there.
      </p>

      <div className="card">
        <label>
          Page URL
          <input
            type="text"
            value={pageUrl}
            onChange={(e) => setPageUrl(e.target.value)}
            placeholder="https://www.facebook.com/Badmintonwithu2013"
            disabled={running}
          />
        </label>

        {/* Scope selector */}
        <div className="scope">
          <span className="scope-title">How much to scrape</span>
          <div className="segmented">
            {MODES.map((m) => (
              <button
                key={m.key}
                className={mode === m.key ? 'seg active' : 'seg'}
                onClick={() => setMode(m.key)}
                disabled={running}
                type="button"
              >
                {m.label}
              </button>
            ))}
          </div>
          <p className="hint">{MODES.find((m) => m.key === mode).hint}</p>

          {/* Per-mode extra input */}
          {mode === 'count' && (
            <div className="mode-input">
              {COUNT_PRESETS.map((n) => (
                <button
                  key={n}
                  type="button"
                  className={Number(count) === n ? 'chip active' : 'chip'}
                  onClick={() => setCount(n)}
                  disabled={running}
                >
                  {n}
                </button>
              ))}
              <input
                type="number"
                min="1"
                value={count}
                onChange={(e) => setCount(e.target.value)}
                disabled={running}
                className="num"
              />
              <span className="unit">posts</span>
            </div>
          )}

          {mode === 'date' && (
            <div className="mode-input col">
              <div className="row">
                {DAY_PRESETS.map((p) => (
                  <button
                    key={p.days}
                    type="button"
                    className={
                      !dateStr && Number(days) === p.days ? 'chip active' : 'chip'
                    }
                    onClick={() => {
                      setDays(p.days);
                      setDateStr('');
                    }}
                    disabled={running}
                  >
                    {p.label}
                  </button>
                ))}
              </div>
              <div className="row">
                <span className="unit">or since date</span>
                <input
                  type="date"
                  value={dateStr}
                  onChange={(e) => setDateStr(e.target.value)}
                  disabled={running}
                  className="num"
                />
              </div>
            </div>
          )}

          {mode === 'pages' && (
            <div className="mode-input">
              <input
                type="number"
                min="1"
                value={pages}
                onChange={(e) => setPages(e.target.value)}
                disabled={running}
                className="num"
              />
              <span className="unit">pages</span>
            </div>
          )}
        </div>

        {/* Column / field selector */}
        <div className="scope">
          <span className="scope-title">Fields to collect</span>
          <div className="fields">
            <span className="chip locked">Text</span>
            {COLUMNS.map((c) => (
              <button
                key={c.key}
                type="button"
                className={cols.has(c.key) ? 'chip active' : 'chip'}
                onClick={() => toggleCol(c.key)}
                disabled={running}
              >
                {cols.has(c.key) ? '✓ ' : ''}
                {c.label}
              </button>
            ))}
          </div>
          <p className="hint">Text is always included. Toggle the rest.</p>
        </div>

        {/* Speed selector */}
        <div className="scope">
          <span className="scope-title">Speed</span>
          <div className="segmented">
            {[
              { key: 'safe', label: 'Safe' },
              { key: 'balanced', label: 'Balanced' },
              { key: 'fast', label: 'Fast' },
            ].map((s) => (
              <button
                key={s.key}
                type="button"
                className={speed === s.key ? 'seg active' : 'seg'}
                onClick={() => setSpeed(s.key)}
                disabled={running}
              >
                {s.label}
              </button>
            ))}
          </div>
          <p className="hint">
            Faster = more posts per request &amp; shorter delays, but higher
            chance of a checkpoint on long runs.
          </p>
        </div>

        {/* Proxy toggle (credentials live in .env, never here) */}
        <div className="scope">
          <label className="checkrow">
            <input
              type="checkbox"
              checked={useProxy}
              onChange={(e) => setUseProxy(e.target.checked)}
              disabled={running || !proxyConfigured}
            />
            <span>Route through proxy</span>
          </label>
          <p className="hint">
            {proxyConfigured
              ? 'Uses the proxy from your .env. Keeps your real IP off Facebook.'
              : 'No proxy set. Add PROXY_SERVER to .env (see .env.example), then restart.'}
          </p>
        </div>

        {/* Log in first — unlocks deeper history (logged-out FB caps ~100) */}
        <div className="scope">
          <label className="checkrow">
            <input
              type="checkbox"
              checked={waitForLogin}
              onChange={(e) => setWaitForLogin(e.target.checked)}
              disabled={running}
            />
            <span>Log in first (for full history)</span>
          </label>
          <p className="hint">
            Logged out, Facebook only lets you scrape ~100 posts deep. Tick this
            to log in in the Chrome window, then click <b>Continue</b> to scrape
            much further.
          </p>
        </div>

        <div className="actions">
          <button className="primary" onClick={start} disabled={running}>
            {running ? 'Scraping…' : 'Scrape'}
          </button>
          {running && (
            <button className="stop" onClick={stop} type="button">
              Stop
            </button>
          )}
        </div>
      </div>

      {running && phase === 'awaiting-gate' && (
        <div className="note gate">
          <p>
            ⏸ Waiting. If needed, log in or solve any checkpoint in the Chrome
            window, then click Continue.
          </p>
          <button className="primary" onClick={continueRun} type="button">
            Continue
          </button>
        </div>
      )}
      {running && phase !== 'awaiting-gate' && (
        <p className="note live">
          <span className="dot" /> Scraping… <b>{liveCount}</b> posts so far.
          Keep the Chrome window open.
        </p>
      )}
      {status === 'stopped' && (
        <p className="note">Stopped. Kept {posts.length || liveCount} posts.</p>
      )}
      {status === 'done' && posts.length > 0 && (
        <p className="note">
          Done — collected <b>{posts.length}</b> posts.
          {stopReason && (
            <>
              {' '}
              <br />
              <b>Why it stopped:</b>{' '}
              {stopReason.startsWith('facebook-cap') ||
              stopReason.startsWith('cursor-stuck') ? (
                <>
                  Facebook itself ended the feed
                  {!waitForLogin && (
                    <>
                      {' '}
                      — logged out it caps page feeds early. Tick “Log in first”
                      and re-run for the full history.
                    </>
                  )}
                  . <span className="mono">({stopReason})</span>
                </>
              ) : stopReason.startsWith('parser-gap') ? (
                <>
                  the response still had posts but the scraper lost the cursor —
                  this is a parser bug, <b>not</b> the login. Logging in won’t
                  fix it. <span className="mono">({stopReason})</span>
                </>
              ) : (
                <span className="mono">{stopReason}</span>
              )}
            </>
          )}
        </p>
      )}
      {status === 'error' && <p className="err">⚠ {error}</p>}

      {posts.length > 0 && (
        <div className="results">
          <div className="results-head">
            <span>{posts.length} posts</span>
            <div>
              <button onClick={downloadJson}>Download JSON</button>
              <button onClick={downloadCsv}>Download CSV</button>
            </div>
          </div>
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>Text</th>
                {activeCols.map((c) => (
                  <th key={c.key}>{c.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {posts.map((p, i) => (
                <tr key={p.id || i}>
                  <td>{i + 1}</td>
                  <td className="text">{p.text}</td>
                  {activeCols.map((c) => (
                    <td
                      key={c.key}
                      className={c.key === 'timestamp' ? 'date' : ''}
                    >
                      {renderCell(p, c.key)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {status === 'done' && posts.length === 0 && (
        <p className="note">
          Finished, but no posts were parsed. Check the Chrome window / server
          console — the page may need a login or a different URL.
        </p>
      )}
    </div>
  );
}
