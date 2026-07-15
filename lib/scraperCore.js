/**
 * Facebook Page Post Scraper — reusable event-based core
 * ======================================================
 *
 * Same GraphQL-replay strategy as the CLI, packaged as a `Scraper` class so it
 * can be driven from either:
 *   - the CLI wrapper (scraper.js), which prints events + reads stdin, or
 *   - the web backend (server/server.js), which streams events over SSE and
 *     resolves the manual gate from a "Continue" button.
 *
 * The class emits:
 *   'log'      (message)                  — human-readable log line
 *   'status'   ({ phase })                — 'launching'|'navigating'|'awaiting-gate'|
 *                                           'capturing'|'scraping'|'done'|'error'|'stopped'
 *   'gate'     ({ reason })               — waiting for the user; call continue()
 *   'progress' ({ round, added, total, hasNext, cursor, friendlyNames })
 *   'friendly' (name)                     — a graphql friendly_name was seen
 *   'done'     ({ posts })                — finished; posts collected
 *   'error'    ({ message })              — fatal error
 *
 * Nothing here is a hardcoded JSON path — responses are walked recursively.
 */

'use strict';

const fs = require('fs');
const EventEmitter = require('events');
const puppeteer = require('puppeteer');

// ---- Defaults (overridable per-run) ----------------------------------------
const DEFAULT_CONFIG = {
  PAGE_URL: 'https://www.facebook.com/nasa',
  USER_DATA_DIR: './fb-session',
  OUTPUT_JSON: './posts.json',
  OUTPUT_CSV: './posts.csv',
  PROGRESS_FILE: './progress.json',
  SAMPLE_FILE: './sample.json',
  DELAY_MIN_MS: 1500,
  DELAY_MAX_MS: 3000,

  // Page size per request. Bumping the feed's `count`/`first` variable returns
  // more posts per request → fewer requests for the same data (faster AND lower
  // block risk). 0 = leave whatever the captured request used.
  BATCH_SIZE: 0,

  // ---- Scope / stop conditions (0 or unset = that limit is off) ----------
  MAX_ROUNDS: 0, // "By pages": stop after N pagination requests
  TARGET_COUNT: 0, // "By number": stop once this many posts are collected
  DATE_CUTOFF: 0, // "By date": unix seconds; stop when posts get older than this
  // (all limits 0 = "All posts": run until Facebook has no next page)

  // Parallel-by-year: run this many independent year-window chains at once.
  // 1 = the classic single newest→oldest feed chain. >1 partitions history by
  // year (via the feed's afterTime/beforeTime params) and paginates several
  // years concurrently for ~Nx throughput. Kept modest by default — every extra
  // concurrent stream from one account raises Facebook's checkpoint risk.
  CONCURRENCY: 1,
  // Oldest year a year-window chain will probe. Facebook Pages don't predate
  // ~2007, so this floor guarantees full coverage; empty years end in 1 request.
  EARLIEST_YEAR: 2006,

  // Adaptive month subdivision: when a single year window keeps producing posts
  // (a dense year), hand its remaining span to 12 monthly sub-chains so idle
  // workers have something to do. This keeps concurrency saturated even in the
  // tail, when only a few high-volume years are left. Sparse/empty years never
  // trigger it, so no wasted requests. Only relevant when CONCURRENCY > 1.
  SUBDIVIDE_MONTHS: false,
  // A year splits into months once it has yielded at least this many new posts
  // while still having more to page — the signal that it's worth parallelizing.
  MONTH_SPLIT_THRESHOLD: 30,
  // Exhaustion backstop for a time window. Facebook can keep returning
  // has_next_page=true while re-serving in-range DUPLICATES, so a window's
  // normal end conditions never fire and it churns "+0" rounds forever. If a
  // window adds no new post for this many consecutive rounds, treat it as
  // exhausted and stop it. Higher = safer against a rare mid-window duplicate
  // patch but wastes more requests; 6 (~18 posts) is a safe floor since windows
  // page newest→oldest and only overlap neighbors by the ±2-day padding.
  STALE_ROUND_LIMIT: 6,

  HEADLESS: false,
  CAPTURE_TIMEOUT_MS: 20000,

  // Auto-close Facebook's logged-out login popup. Set false when you WANT to log
  // in manually (logging in lets you page far deeper than the ~100-post
  // logged-out cap).
  AUTO_DISMISS_MODAL: true,
  USE_PROXY: false,
  PROXY_SERVER: '',
  PROXY_USERNAME: '',
  PROXY_PASSWORD: '',
};

// ============================================================================
// Small utilities (pure, shared)
// ============================================================================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function parseForm(body) {
  const params = new URLSearchParams(body);
  const obj = {};
  for (const [k, v] of params.entries()) obj[k] = v;
  return obj;
}

function encodeForm(obj) {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(obj)) params.append(k, v);
  return params.toString();
}

// ============================================================================
// PARSER SECTION — tune after inspecting sample.json (exported for reuse/tests)
// ============================================================================

/** Generic recursive walker. cb(key, value, parent) for every property. */
function walk(node, cb, parent = null, seen = new Set()) {
  if (node === null || typeof node !== 'object') return;
  if (seen.has(node)) return;
  seen.add(node);

  if (Array.isArray(node)) {
    for (const item of node) walk(item, cb, node, seen);
    return;
  }
  for (const [key, value] of Object.entries(node)) {
    cb(key, value, node);
    if (value && typeof value === 'object') walk(value, cb, node, seen);
  }
}

/** True if a variables object references a cursor somewhere (recursively). */
function hasCursorVariable(obj) {
  let found = false;
  walk(obj, (key, value) => {
    if (
      typeof key === 'string' &&
      /cursor|after|before/i.test(key) &&
      (typeof value === 'string' || value === null)
    ) {
      found = true;
    }
  });
  return found;
}

/** Split FB's newline-concatenated JSON stream into parsed objects. */
function parseStreamedJson(text) {
  const objects = [];
  const lines = text.split('\n');
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      objects.push(JSON.parse(trimmed));
    } catch {
      /* not a standalone JSON object; skip */
    }
  }
  if (objects.length === 0) {
    try {
      objects.push(JSON.parse(text));
    } catch {
      /* give up; caller handles empty */
    }
  }
  return objects;
}

/** Extract { endCursor, hasNextPage } from any page_info in the tree. */
function extractPageInfo(objects) {
  let result = { endCursor: null, hasNextPage: false };
  for (const obj of objects) {
    walk(obj, (key, value) => {
      if (key === 'page_info' && value && typeof value === 'object') {
        const end = value.end_cursor ?? value.endCursor ?? null;
        const has = value.has_next_page ?? value.hasNextPage ?? false;
        if (end || has) result = { endCursor: end, hasNextPage: !!has };
      }
      if (
        (key === 'end_cursor' || key === 'endCursor') &&
        typeof value === 'string'
      ) {
        if (!result.endCursor) result.endCursor = value;
      }
    });
  }
  return result;
}

/**
 * Depth-first search for the first object node satisfying pred(node).
 * pred receives each object (not arrays); we anchor extractors on distinctive
 * *local shapes* (e.g. "has reaction_count.count and share_count") rather than
 * Facebook's giant fragile paths, so they survive key/path renaming.
 */
function findNode(root, pred) {
  let found = null;
  (function rec(n) {
    if (found || !n || typeof n !== 'object') return;
    if (Array.isArray(n)) {
      for (const it of n) {
        rec(it);
        if (found) return;
      }
      return;
    }
    try {
      if (pred(n)) {
        found = n;
        return;
      }
    } catch {
      /* pred touched a missing field; keep searching */
    }
    for (const k of Object.keys(n)) {
      rec(n[k]);
      if (found) return;
    }
  })(root);
  return found;
}

/**
 * Pull the "useful" extra fields off a post node. Every field is optional and
 * falls back to null if not present, so photo-only or count-less posts are fine.
 */
function extractExtras(node) {
  const out = {
    author: null,
    authorUrl: null,
    reactions: null,
    comments: null,
    shares: null,
    image: null,
  };

  // Reaction + share totals live on the UFI summary feedback node, which is the
  // one object carrying BOTH a reaction_count.count and a share_count.
  const fb = findNode(
    node,
    (o) =>
      o.reaction_count &&
      typeof o.reaction_count === 'object' &&
      typeof o.reaction_count.count === 'number' &&
      o.share_count
  );
  if (fb) {
    out.reactions = fb.reaction_count.count ?? null;
    out.shares =
      fb.share_count && typeof fb.share_count === 'object'
        ? fb.share_count.count ?? null
        : null;
  }

  // Comment total: a `comments` object with a numeric total_count.
  const cm = findNode(
    node,
    (o) =>
      o.comments &&
      typeof o.comments === 'object' &&
      typeof o.comments.total_count === 'number'
  );
  if (cm) out.comments = cm.comments.total_count;

  // Author: first `actors` array whose first entry has a name.
  const act = findNode(
    node,
    (o) => Array.isArray(o.actors) && o.actors[0] && typeof o.actors[0].name === 'string'
  );
  if (act) {
    out.author = act.actors[0].name;
    out.authorUrl = act.actors[0].url || act.actors[0].profile_url || null;
  }

  // First image: a `photo_image` (or `viewer_image`) with a uri.
  const img = findNode(
    node,
    (o) =>
      (o.photo_image && typeof o.photo_image.uri === 'string') ||
      (o.viewer_image && typeof o.viewer_image.uri === 'string')
  );
  if (img) out.image = (img.photo_image || img.viewer_image).uri;

  return out;
}

/**
 * Extract posts from the response tree, anchored on `post_id` (one per real
 * post). Each post gets id/text/timestamp/url plus the extra fields above.
 */
function extractPosts(objects) {
  const posts = [];
  const seenIds = new Set();
  for (const obj of objects) {
    walk(obj, (key, value, parent) => {
      if (key === 'post_id' && typeof value === 'string' && value.length > 4) {
        if (seenIds.has(value)) return;
        seenIds.add(value);
        posts.push({
          id: value,
          text: textFromNode(parent) || '',
          timestamp: timestampFromNode(parent),
          url: urlFromNode(parent),
          ...extractExtras(parent),
        });
      }
    });
  }
  return posts;
}

function textFromNode(node) {
  if (!node || typeof node !== 'object') return null;
  const candidates = [
    node?.message?.text,
    node?.comet_sections?.content?.story?.message?.text,
    node?.comet_sections?.content?.story?.comet_sections?.message?.story
      ?.message?.text,
    node?.story?.message?.text,
    node?.content?.story?.message?.text,
  ];
  for (const c of candidates) {
    if (typeof c === 'string' && c.trim()) return c.trim();
  }
  let found = null;
  walk(node, (key, value) => {
    if (found) return;
    if (key === 'text' && typeof value === 'string' && value.trim().length > 8) {
      found = value.trim();
    }
  });
  return found;
}

function idFromNode(node) {
  const direct =
    node?.post_id ||
    node?.id ||
    node?.story?.id ||
    node?.comet_sections?.content?.story?.id;
  if (typeof direct === 'string' && direct.length > 0) return direct;
  let found = null;
  walk(node, (key, value) => {
    if (found) return;
    if (
      (key === 'post_id' || key === 'id') &&
      typeof value === 'string' &&
      value.length > 4
    ) {
      found = value;
    }
  });
  return found;
}

function timestampFromNode(node) {
  let found = null;
  walk(node, (key, value) => {
    if (found) return;
    if (
      /creation_time|publish_time|created_time|timestamp/i.test(key) &&
      typeof value === 'number' &&
      value > 1000000000
    ) {
      found = value;
    }
  });
  return found;
}

function urlFromNode(node) {
  let found = null;
  walk(node, (key, value) => {
    if (found) return;
    if (
      /url|permalink/i.test(key) &&
      typeof value === 'string' &&
      /facebook\.com|\/posts\/|\/story|\/permalink/.test(value)
    ) {
      found = value;
    }
  });
  return found;
}

/** Inject a cursor into a variables object (only touches cursor-ish fields). */
function injectCursor(vars, cursor) {
  let touched = false;
  walk(vars, (key, value, parent) => {
    if (/^(cursor|after|end_cursor|afterCursor)$/i.test(key)) {
      parent[key] = cursor;
      touched = true;
    }
  });
  if (!touched) {
    if ('cursor' in vars) vars.cursor = cursor;
    else if ('after' in vars) vars.after = cursor;
    else vars.cursor = cursor;
  }
  return vars;
}

/**
 * Bound the feed to a [afterTime, beforeTime] window (unix seconds). These are
 * real, accepted variables on the timeline feed query — the year filter sets
 * `beforeTime`; setting both lets us carve history into independent windows that
 * paginate concurrently. Pass null to leave a bound open.
 */
function injectTimeWindow(vars, afterTime, beforeTime) {
  vars.afterTime = afterTime != null ? afterTime : null;
  vars.beforeTime = beforeTime != null ? beforeTime : null;
  return vars;
}

/**
 * Bump the feed page size by overwriting small `count`/`first` numbers in the
 * variables. We only touch existing numeric page-size fields (value < 100) so
 * unrelated numbers aren't clobbered. More posts per request = fewer requests.
 */
function setBatchSize(vars, size) {
  if (!size || size < 1) return vars;
  walk(vars, (key, value, parent) => {
    if (
      /^(count|first)$/i.test(key) &&
      typeof value === 'number' &&
      value > 0 &&
      value < 100
    ) {
      parent[key] = size;
    }
  });
  return vars;
}

/** Keep only headers that matter for a same-origin graphql POST. */
function pickForwardHeaders(headers) {
  const keep = [
    'x-fb-friendly-name',
    'x-fb-lsd',
    'x-asbd-id',
    'x-fb-qpl-active-flows',
    'sec-fetch-site',
    'sec-fetch-mode',
    'sec-fetch-dest',
    'origin',
  ];
  const out = {};
  for (const [k, v] of Object.entries(headers || {})) {
    if (keep.includes(k.toLowerCase())) out[k] = v;
  }
  return out;
}

const ALL_FIELDS = [
  'id',
  'timestamp',
  'text',
  'url',
  'author',
  'authorUrl',
  'reactions',
  'comments',
  'shares',
  'image',
];

/** Build CSV. `fields` is an optional ordered list of post keys to include. */
function toCsv(posts, fields) {
  const cols =
    Array.isArray(fields) && fields.length ? fields : ALL_FIELDS;
  const esc = (v) => {
    if (v === null || v === undefined) return '';
    const s = String(v).replace(/"/g, '""');
    return `"${s}"`;
  };
  const header = cols.join(',');
  const rows = posts.map((p) => cols.map((c) => esc(p[c])).join(','));
  return [header, ...rows].join('\n');
}

// ============================================================================
// Scraper — the event-based engine
// ============================================================================
class Scraper extends EventEmitter {
  constructor(config = {}) {
    super();
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.stopped = false;
    this.browser = null;
    this.page = null;
    this._gateResolve = null;
    this.postsById = new Map();
    this.friendlyNames = new Set();
    this.stopReason = null; // why the pagination loop ended (set at each break)
    // Shared pause used by parallel year-chains: when one hits a possible
    // checkpoint it pauses ALL chains until the user clicks Continue.
    this._paused = false;
    this._resumeWaiters = [];
  }

  // ---- event helpers -------------------------------------------------------
  log(...args) {
    const msg = args
      .map((a) => (typeof a === 'string' ? a : JSON.stringify(a)))
      .join(' ');
    this.emit('log', msg);
  }
  status(phase) {
    this.emit('status', { phase });
  }

  randDelay() {
    return Math.floor(
      this.config.DELAY_MIN_MS +
        Math.random() *
          (this.config.DELAY_MAX_MS - this.config.DELAY_MIN_MS)
    );
  }

  // ---- manual gate (replaces stdin Enter) ----------------------------------
  /** Returns a promise that resolves when continue() or stop() is called. */
  waitForGate(reason) {
    this.status('awaiting-gate');
    this.emit('gate', { reason });
    this.log('⏸  ' + reason);
    return new Promise((resolve) => {
      this._gateResolve = resolve;
    });
  }
  /** Called by the driver (button / stdin) to release the gate. */
  continue() {
    this._releasePause();
    if (this._gateResolve) {
      const r = this._gateResolve;
      this._gateResolve = null;
      this.log('▶  Continuing…');
      r();
    }
  }
  /** Request a graceful stop; also releases any open gate. */
  stop() {
    this.stopped = true;
    this.log('■  Stop requested.');
    this._releasePause();
    if (this._gateResolve) {
      const r = this._gateResolve;
      this._gateResolve = null;
      r();
    }
  }

  // ---- shared pause for parallel chains ------------------------------------
  /** A chain awaits this before each round; resolves instantly unless paused. */
  awaitResume() {
    if (!this._paused || this.stopped) return Promise.resolve();
    return new Promise((res) => this._resumeWaiters.push(res));
  }
  /** First chain to hit trouble pauses all of them and opens the gate once. */
  requestPause(reason) {
    if (this._paused || this.stopped) return;
    this._paused = true;
    this.waitForGate(reason);
  }
  _releasePause() {
    this._paused = false;
    const waiters = this._resumeWaiters;
    this._resumeWaiters = [];
    waiters.forEach((r) => r());
  }

  // ---- progress persistence ------------------------------------------------
  loadProgress() {
    const fresh = { cursor: null, posts: [], done: false };
    try {
      if (fs.existsSync(this.config.PROGRESS_FILE)) {
        const data = JSON.parse(
          fs.readFileSync(this.config.PROGRESS_FILE, 'utf8')
        );
        // Only resume progress that belongs to an UNFINISHED run of the SAME
        // page. Otherwise a fresh Scrape click would either (a) re-open a run
        // that already reported done — the loop would be skipped and we'd just
        // re-emit the old posts — or (b) replay a cursor that points into a
        // different page's feed. Either way, start clean.
        if (data.done) {
          this.log(
            `Ignoring progress file: previous run already finished ` +
              `(${data.posts?.length || 0} posts). Starting fresh.`
          );
          return fresh;
        }
        if (data.pageUrl && data.pageUrl !== this.config.PAGE_URL) {
          this.log(
            `Ignoring progress file: it belongs to a different page ` +
              `(${data.pageUrl}). Starting fresh.`
          );
          return fresh;
        }
        this.log(
          `Resuming from progress file: ${data.posts?.length || 0} posts, cursor ${
            data.cursor ? data.cursor.slice(0, 24) + '…' : '(none)'
          }`
        );
        return data;
      }
    } catch (e) {
      this.log('WARN could not read progress file, starting fresh:', e.message);
    }
    return fresh;
  }

  saveProgress(state) {
    // Stamp the page URL so a later run can tell whether this cursor/posts set
    // belongs to the page it's about to scrape.
    const stamped = { pageUrl: this.config.PAGE_URL, ...state };
    const tmp = this.config.PROGRESS_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(stamped, null, 2));
    fs.renameSync(tmp, this.config.PROGRESS_FILE);
  }

  writeOutputs(posts) {
    fs.writeFileSync(this.config.OUTPUT_JSON, JSON.stringify(posts, null, 2));
    this.log(`Wrote ${posts.length} posts → ${this.config.OUTPUT_JSON}`);
    fs.writeFileSync(this.config.OUTPUT_CSV, toCsv(posts));
    this.log(`Wrote CSV → ${this.config.OUTPUT_CSV}`);
  }

  // ---- checkpoint detection ------------------------------------------------
  async isCheckpoint() {
    try {
      const url = this.page.url();
      if (/checkpoint|login|\/login\//i.test(url)) return true;
      const bodyText = await this.page.evaluate(() =>
        document.body ? document.body.innerText.slice(0, 4000) : ''
      );
      return /(you must log in|log in to continue|confirm your identity|security check|temporarily blocked)/i.test(
        bodyText
      );
    } catch {
      return false;
    }
  }

  /**
   * Facebook shows logged-out visitors a "Log in or sign up" popup that blocks
   * scrolling. Public pages are still readable behind it, so we dismiss it:
   * click its Close button and unlock the scroll. Safe to call repeatedly.
   */
  async dismissLoginModal() {
    if (!this.config.AUTO_DISMISS_MODAL) return false; // manual-login mode
    try {
      // Escape closes most Facebook dialogs and is language-independent.
      await this.page.keyboard.press('Escape').catch(() => {});

      const closed = await this.page.evaluate(() => {
        let acted = false;
        // Common "close" labels across locales (the popup's X button).
        const CLOSE = [
          'close',
          '关闭',
          '關閉',
          'cerrar',
          'fermer',
          'schließen',
          'chiudi',
          'fechar',
          'закрыть',
          '閉じる',
          '닫기',
        ];
        const labelled = document.querySelectorAll('[aria-label]');
        for (const el of labelled) {
          const lbl = (el.getAttribute('aria-label') || '').toLowerCase();
          if (!CLOSE.some((w) => lbl.includes(w))) continue;
          const r = el.getBoundingClientRect();
          if (r.width > 0 && r.height > 0) {
            el.click();
            acted = true;
          }
        }
        // The modal locks the page by disabling scroll — re-enable it.
        for (const node of [document.body, document.documentElement]) {
          if (node && node.style && node.style.overflow === 'hidden') {
            node.style.overflow = '';
            acted = true;
          }
        }
        return acted;
      });
      if (closed) this.log('Dismissed Facebook login popup.');
      return closed;
    } catch {
      return false;
    }
  }

  // ---- capture the pagination template -------------------------------------
  async captureTemplate() {
    this.status('capturing');
    this.log('Arming request listener and triggering a scroll to capture graphql…');
    const page = this.page;
    const self = this;

    return await new Promise(async (resolve, reject) => {
      let settled = false;
      // Collect every pagination-looking candidate rather than settling on the
      // first. Facebook fires several feed queries on a Profile/Page: the media
      // "Tiles" grid, sometimes a pinned/featured feed, and the real post
      // timeline. We want the timeline, so score candidates and pick the best.
      const candidates = new Map(); // friendly -> {url, headers, postForm, friendlyName}

      // Higher = better. Real chronological post feed wins; media grid loses.
      const scoreFriendly = (name) => {
        if (/Tiles|Photo|Media|Video/i.test(name)) return 1; // media grid — last resort
        if (/Timeline/i.test(name)) return 5; // the post timeline — best
        if (/Posts/i.test(name)) return 4;
        if (/Feed/i.test(name)) return 3;
        return 2;
      };

      const pickBest = () => {
        let best = null;
        let bestScore = -1;
        for (const c of candidates.values()) {
          const s = scoreFriendly(c.friendlyName);
          if (s > bestScore) {
            bestScore = s;
            best = c;
          }
        }
        return best;
      };

      const settleWithBest = () => {
        if (settled) return false;
        const best = pickBest();
        if (!best) return false;
        settled = true;
        page.off('request', onRequest);
        const names = Array.from(candidates.keys());
        self.log(
          `Captured pagination template: "${best.friendlyName}"` +
            (names.length > 1 ? ` (candidates: ${names.join(', ')})` : '')
        );
        resolve(best);
        return true;
      };

      const onRequest = (request) => {
        try {
          const url = request.url();
          if (!url.includes('/api/graphql/')) return;
          if (request.method() !== 'POST') return;
          const postData = request.postData();
          if (!postData) return;

          const form = parseForm(postData);
          const friendly = form.fb_api_req_friendly_name || '(none)';
          if (!self.friendlyNames.has(friendly)) {
            self.friendlyNames.add(friendly);
            self.emit('friendly', friendly);
            self.log('  graphql friendly_name:', friendly);
          }

          if (!form.variables) return;
          let vars;
          try {
            vars = JSON.parse(form.variables);
          } catch {
            return;
          }

          const looksLikePagination =
            hasCursorVariable(vars) &&
            /timeline|feed|profile|page|posts/i.test(friendly);

          // Record the candidate but keep listening — a better feed query may
          // still fire as we scroll. If we capture the ideal timeline query,
          // settle immediately; otherwise wait to see if it shows up.
          if (looksLikePagination && !settled) {
            if (!candidates.has(friendly)) {
              candidates.set(friendly, {
                url,
                headers: request.headers(),
                postForm: form,
                friendlyName: friendly,
              });
            }
            if (/Timeline/i.test(friendly)) settleWithBest();
          }
        } catch {
          /* ignore per-request parse errors */
        }
      };

      page.on('request', onRequest);

      try {
        await self.dismissLoginModal(); // in case it reappeared
        // Scroll further and more times: on a Profile the post timeline sits
        // below a tall header + the media "Tiles" grid, so a few short scrolls
        // never reach it. Keep scrolling (settling early if we hit Timeline).
        for (let i = 0; i < 10 && !settled; i++) {
          await page.evaluate(() =>
            window.scrollBy(0, Math.max(1200, window.innerHeight * 1.5))
          );
          await new Promise((r) => setTimeout(r, 1400));
          await self.dismissLoginModal(); // FB may re-show it as you scroll
        }
      } catch {
        /* page may navigate; ignore */
      }

      // Scrolling done — settle with the best candidate we gathered, if any.
      settleWithBest();

      setTimeout(() => {
        if (!settled) {
          page.off('request', onRequest);
          const seen = Array.from(self.friendlyNames);
          // If the only graphql traffic was Facebook's logged-out login prompts,
          // the feed is locked because we're not signed in — no amount of
          // scrolling will produce a pagination request. Say so plainly.
          const loggedOut =
            seen.length > 0 &&
            seen.every((n) => /LoggedOut|Login|QrCode/i.test(n));
          if (loggedOut) {
            reject(
              new Error(
                'Not logged in: Facebook does not paginate a Page feed for ' +
                  'logged-out visitors, so there was no pagination request to ' +
                  'capture. To scrape all posts, tick “Log in first”, sign in ' +
                  'in the Chrome window, then click Continue.\n' +
                  'Friendly names seen: ' +
                  seen.join(', ')
              )
            );
          } else {
            reject(
              new Error(
                'No pagination request captured within timeout.\n' +
                  'Friendly names seen: ' +
                  (seen.join(', ') || '(none)') +
                  '\nTry: scroll more manually, confirm this is a Page ' +
                  '(not a Group/Profile), or increase CAPTURE_TIMEOUT_MS.'
              )
            );
          }
        }
      }, this.config.CAPTURE_TIMEOUT_MS);
    });
  }

  // ---- one pagination request (in-page fetch) ------------------------------
  // `timeWindow` (optional): { afterTime, beforeTime } bounds this request to a
  // date range — used by the parallel year chains. When a window is set and no
  // cursor is given, we start that window fresh (cursor null), not from the
  // template's captured mid-feed cursor.
  async fetchPage(template, cursor, timeWindow) {
    const form = { ...template.postForm };
    let vars = {};
    try {
      vars = JSON.parse(form.variables);
    } catch {
      vars = {};
    }
    if (timeWindow) {
      injectTimeWindow(vars, timeWindow.afterTime, timeWindow.beforeTime);
      if (!cursor) vars.cursor = null; // fresh start for this window
    }
    if (cursor) injectCursor(vars, cursor);
    if (this.config.BATCH_SIZE) setBatchSize(vars, this.config.BATCH_SIZE);
    form.variables = JSON.stringify(vars);
    const body = encodeForm(form);

    return await this.page.evaluate(
      async (url, body, headers) => {
        const res = await fetch(url, {
          method: 'POST',
          headers: {
            'content-type': 'application/x-www-form-urlencoded',
            ...headers,
          },
          body,
          credentials: 'include',
        });
        return await res.text();
      },
      template.url,
      body,
      pickForwardHeaders(template.headers)
    );
  }

  // ---- main run ------------------------------------------------------------
  async run() {
    try {
      this.status('launching');
      this.log('Facebook GraphQL replay scraper starting.');
      this.log('Target:', this.config.PAGE_URL);

      const launchArgs = [
        '--no-sandbox',
        '--disable-blink-features=AutomationControlled',
      ];
      if (this.config.USE_PROXY && this.config.PROXY_SERVER) {
        launchArgs.push(`--proxy-server=${this.config.PROXY_SERVER}`);
        this.log('Using proxy:', this.config.PROXY_SERVER);
      }

      this.browser = await puppeteer.launch({
        headless: this.config.HEADLESS,
        userDataDir: this.config.USER_DATA_DIR,
        defaultViewport: null,
        args: launchArgs,
      });

      // Reuse the first tab and close any extras the persistent profile
      // restored, so only one Chrome tab is in play.
      const openPages = await this.browser.pages();
      this.page = openPages[0] || (await this.browser.newPage());
      for (const extra of openPages.slice(1)) {
        try {
          await extra.close();
        } catch {
          /* ignore */
        }
      }

      if (this.config.USE_PROXY && this.config.PROXY_USERNAME) {
        await this.page.authenticate({
          username: this.config.PROXY_USERNAME,
          password: this.config.PROXY_PASSWORD,
        });
      }

      // Resume state.
      let state = this.loadProgress();
      this.postsById = new Map(state.posts.map((p) => [p.id, p]));

      this.status('navigating');
      this.log('Navigating to target page…');
      await this.page.goto(this.config.PAGE_URL, {
        waitUntil: 'domcontentloaded',
        timeout: 60000,
      });

      // The login popup usually appears a beat after load — give it a moment,
      // then dismiss it so the page is scrollable for the capture.
      await sleep(2000);
      await this.dismissLoginModal();

      // Manual gate — driver resolves via continue().
      await this.waitForGate(
        'If a login/checkpoint is showing, solve or close it in the browser window, then click Continue to begin capture.'
      );
      if (this.stopped) return this._finish();

      // Capture template.
      let template;
      try {
        template = await this.captureTemplate();
      } catch (e) {
        this.status('error');
        this.emit('error', { message: e.message });
        this.log('ERROR capturing pagination request:\n' + e.message);
        this.log('Browser left open for inspection.');
        return;
      }

      // Parallel-by-year: hand off to the concurrent driver and finish.
      if ((this.config.CONCURRENCY || 1) > 1) {
        this.status('scraping');
        await this.runYearWindows(template, state);
        return this._finish();
      }

      let firstResponseDumped = fs.existsSync(this.config.SAMPLE_FILE);
      let cursor = state.cursor;
      let round = 0;
      let consecutiveEmpty = 0;
      // Adaptive backoff: multiplies the base delay when Facebook shows strain
      // (empty/error rounds), resets to 1 on a healthy round. Fast when all is
      // well, cautious automatically when it isn't.
      let backoff = 1;

      this.status('scraping');
      while (!state.done && !this.stopped) {
        round++;
        if (this.config.MAX_ROUNDS && round > this.config.MAX_ROUNDS) {
          this.log(`Reached MAX_ROUNDS (${this.config.MAX_ROUNDS}). Stopping.`);
          break;
        }

        if (await this.isCheckpoint()) {
          await this.waitForGate(
            'Checkpoint/challenge detected. Resolve it in the browser, then click Continue to resume.'
          );
          if (this.stopped) break;
        }

        let responseText;
        try {
          responseText = await this.fetchPage(template, cursor);
        } catch (e) {
          this.log(`Round ${round}: fetch failed (${e.message}). Recovering…`);
          responseText = '';
          backoff = Math.min(backoff * 2, 6); // ease off after a failure
        }

        if (!firstResponseDumped && responseText) {
          fs.writeFileSync(this.config.SAMPLE_FILE, responseText);
          this.log(
            `Dumped raw first response → ${this.config.SAMPLE_FILE} (inspect to tune parser)`
          );
          firstResponseDumped = true;
        }

        const objects = parseStreamedJson(responseText);

        if (objects.length === 0) {
          consecutiveEmpty++;
          backoff = Math.min(backoff * 2, 6); // slow down when things look off
          this.log(
            `Round ${round}: empty/unparseable response (${consecutiveEmpty}).`
          );
          if (consecutiveEmpty >= 2) {
            this.log('Possible token expiry. Re-navigating to refresh session…');
            try {
              await this.page.goto(this.config.PAGE_URL, {
                waitUntil: 'domcontentloaded',
                timeout: 60000,
              });
              if (await this.isCheckpoint()) {
                await this.waitForGate(
                  'Checkpoint after refresh — resolve it, then click Continue.'
                );
                if (this.stopped) break;
              }
              template = await this.captureTemplate();
              consecutiveEmpty = 0;
              this.log('Session refreshed and template re-captured. Resuming.');
            } catch (e) {
              this.log('Refresh failed:', e.message, '- retrying after delay.');
            }
          }
          await sleep(Math.floor(this.randDelay() * backoff));
          continue;
        }
        consecutiveEmpty = 0;
        backoff = 1; // healthy round → back to full speed

        const newPosts = extractPosts(objects);
        const { endCursor, hasNextPage } = extractPageInfo(objects);

        let added = 0;
        for (const p of newPosts) {
          if (!this.postsById.has(p.id)) {
            this.postsById.set(p.id, p);
            added++;
          }
        }

        this.log(
          `Round ${round}: +${added} new posts (total ${this.postsById.size}) | ` +
            `hasNext=${hasNextPage} | cursor=${
              endCursor ? endCursor.slice(0, 20) + '…' : '(none)'
            }`
        );
        this.emit('progress', {
          round,
          added,
          total: this.postsById.size,
          hasNext: hasNextPage,
          cursor: endCursor || cursor,
          friendlyNames: Array.from(this.friendlyNames),
        });

        state = {
          cursor: endCursor || cursor,
          posts: Array.from(this.postsById.values()),
          done: false,
        };
        this.saveProgress(state);

        // ---- Scope stop conditions ------------------------------------
        // By number: enough posts collected.
        if (this.config.TARGET_COUNT && this.postsById.size >= this.config.TARGET_COUNT) {
          this.log(
            `Reached target of ${this.config.TARGET_COUNT} posts. Stopping.`
          );
          state.done = true;
          this.saveProgress(state);
          break;
        }
        // By date: posts have gotten older than the cutoff. Feeds page
        // newest→oldest, so once this round contains a post older than the
        // cutoff we've paged past the window and can stop.
        if (this.config.DATE_CUTOFF) {
          const oldest = newPosts.reduce((min, p) => {
            if (typeof p.timestamp === 'number' && p.timestamp > 0) {
              return min === null ? p.timestamp : Math.min(min, p.timestamp);
            }
            return min;
          }, null);
          if (oldest !== null && oldest < this.config.DATE_CUTOFF) {
            this.log(
              'Reached posts older than the date cutoff. Stopping.'
            );
            state.done = true;
            this.saveProgress(state);
            break;
          }
        }

        // Distinguish the two very different "reached the end" cases so we can
        // tell a genuine Facebook cap apart from a parser that lost the cursor.
        if (hasNextPage === false && added > 0) {
          // Facebook explicitly said there is no next page, yet this round still
          // returned posts → this is Facebook capping the feed, not us.
          this.stopReason =
            `facebook-cap: Facebook returned has_next_page=false after ${this.postsById.size} posts ` +
            `(round ${round}). This is Facebook's limit, not the scraper.`;
          this.log(this.stopReason);
          state.done = true;
          this.saveProgress(state);
          break;
        }
        if (!endCursor) {
          // We got a response but couldn't locate an end_cursor in it. If this
          // round DID return posts, the feed likely continues and our PARSER is
          // the bottleneck — flag it loudly so we don't blame the login.
          this.stopReason =
            added > 0
              ? `parser-gap: response had ${added} posts but NO end_cursor was found — ` +
                `parser missed the cursor (NOT a Facebook cap).`
              : `no-cursor: response had no posts and no cursor after ${this.postsById.size} posts.`;
          this.log(this.stopReason);
          state.done = true;
          this.saveProgress(state);
          break;
        }
        if (!hasNextPage) {
          this.stopReason =
            `end-of-feed: has_next_page=false with no new posts after ${this.postsById.size} posts.`;
          this.log(this.stopReason);
          state.done = true;
          this.saveProgress(state);
          break;
        }
        if (endCursor === cursor) {
          this.stopReason =
            `cursor-stuck: cursor did not advance after ${this.postsById.size} posts ` +
            `(Facebook returned the same cursor — often its way of ending a logged-out feed).`;
          this.log(this.stopReason);
          state.done = true;
          this.saveProgress(state);
          break;
        }

        cursor = endCursor;
        await sleep(Math.floor(this.randDelay() * backoff));
      }

      return this._finish();
    } catch (err) {
      this.status('error');
      this.emit('error', { message: err.message });
      this.log('FATAL:', err.message);
    }
  }

  // ---- parallel-by-year driver ---------------------------------------------
  /**
   * Partition history into per-year windows (via afterTime/beforeTime) and
   * paginate several concurrently. Each window is its own independent cursor
   * chain; all merge into the shared postsById (deduped by post_id).
   */
  async runYearWindows(template, state) {
    // `windows` is the per-window resume map, keyed by window key ('2024' or
    // '2024-06'). Migrate the legacy `years` map (same '2024' keys) if present.
    state.windows = state.windows || state.years || {};
    const currentYear = new Date().getFullYear();
    const years = [];
    for (let y = currentYear; y >= this.config.EARLIEST_YEAR; y--) years.push(y);
    // Never run more chains than there are windows to work — extra workers idle.
    const concurrency = Math.max(
      2,
      Math.min(years.length, this.config.CONCURRENCY | 0)
    );

    // Shared work queue. Seeded with year windows; a dense year pushes its 12
    // month sub-windows onto it at runtime so idle chains pick them up. On
    // resume, a year previously marked `split` is re-seeded as its (unfinished)
    // month windows instead of the year window.
    const queue = [];
    for (const y of years) {
      const rec = state.windows[String(y)];
      if (rec && rec.split) {
        for (let m = 11; m >= 0; m--) {
          const mk = this.monthWindow(y, m).key;
          if (!(state.windows[mk] && state.windows[mk].done)) {
            queue.push(this.monthWindow(y, m));
          }
        }
      } else if (!(rec && rec.done)) {
        queue.push(this.yearWindow(y));
      }
    }

    this.log(
      `Parallel-by-year: ${years.length} year windows (${years[years.length - 1]}–${years[0]}), ` +
        `${concurrency} chains at a time` +
        (this.config.SUBDIVIDE_MONTHS
          ? ', dense years auto-split into months.'
          : '.')
    );

    // Emit the full window plan so the UI can render every year cell up front
    // (queued → active → done). On resume, reflect what's already finished.
    const planWindows = [];
    for (const y of years) {
      const rec = state.windows[String(y)];
      let st = 'queued';
      if (rec && rec.split) st = 'split';
      else if (rec && rec.done) st = 'done';
      planWindows.push({ key: String(y), kind: 'year', year: y, state: st });
      // A split year's months (resume): show whichever are already done.
      if (rec && rec.split) {
        for (let m = 0; m < 12; m++) {
          const mw = this.monthWindow(y, m);
          const mr = state.windows[mw.key];
          planWindows.push({
            key: mw.key,
            kind: 'month',
            year: y,
            month: m,
            state: mr && mr.done ? 'done' : 'queued',
          });
        }
      }
    }
    this.emit('plan', { years, concurrency, windows: planWindows });

    let active = 0; // chains currently paginating (may still enqueue months)
    const worker = async (wid) => {
      await sleep(wid * 300); // stagger starts so chains don't fire in unison
      while (!this.stopped) {
        const win = queue.shift();
        if (!win) {
          if (active === 0) return; // nothing queued and nobody producing more
          await sleep(200); // a busy chain may still push month windows
          continue;
        }
        active++;
        try {
          await this.paginateWindow(template, win, state, queue);
        } catch (e) {
          this.log(`[${win.key}] window error: ${e.message}`);
        } finally {
          active--;
        }
      }
    };
    const runners = [];
    for (let k = 0; k < concurrency; k++) runners.push(worker(k));
    await Promise.all(runners);

    if (!this.stopped) {
      state.done = true;
      this.saveProgress(state);
      this.stopReason =
        this.stopReason ||
        `parallel-by-year: completed all windows with up to ${concurrency} concurrent chains.`;
    }
  }

  /** A whole-year time window (UTC bounds padded ±2 days so neighbors overlap). */
  yearWindow(year) {
    const DAY = 86400;
    return {
      key: String(year),
      kind: 'year',
      year,
      afterTime: Math.floor(Date.UTC(year, 0, 1) / 1000) - 2 * DAY,
      beforeTime: Math.floor(Date.UTC(year + 1, 0, 1) / 1000) - 1 + 2 * DAY,
    };
  }

  /** A single-month time window. `month` is 0-based (0 = January). */
  monthWindow(year, month) {
    const DAY = 86400;
    return {
      key: `${year}-${String(month + 1).padStart(2, '0')}`,
      kind: 'month',
      year,
      month,
      afterTime: Math.floor(Date.UTC(year, month, 1) / 1000) - 2 * DAY,
      beforeTime: Math.floor(Date.UTC(year, month + 1, 1) / 1000) - 1 + 2 * DAY,
    };
  }

  /**
   * How many collected posts actually fall in this window, bucketed by calendar
   * year (and month for month windows). Unlike "new this run", this reflects the
   * real total in the window — so a resume doesn't show a misleading 0 for years
   * that were already fully collected. Buckets are exclusive, so cells sum to the
   * dated-post total.
   */
  windowPostCount(win) {
    let n = 0;
    for (const p of this.postsById.values()) {
      const t = p.timestamp;
      if (typeof t !== 'number' || t <= 0) continue;
      const d = new Date(t * 1000);
      if (d.getUTCFullYear() !== win.year) continue;
      if (win.kind === 'month' && d.getUTCMonth() !== win.month) continue;
      n++;
    }
    return n;
  }

  /**
   * Paginate a single time window (year or month) into the shared postsById.
   * `queue` is the live work queue — a dense year pushes its month windows onto
   * it so idle chains keep busy (adaptive subdivision).
   */
  async paginateWindow(template, win, state, queue) {
    const { key, afterTime, beforeTime } = win;
    const w = { afterTime, beforeTime };
    const rec =
      state.windows[key] || (state.windows[key] = { cursor: null, done: false });
    if (rec.done) return;
    let cursor = rec.cursor || null;
    let round = 0;
    let consecutiveEmpty = 0;
    let backoff = 1;
    let addedTotal = 0;
    let stall = 0; // consecutive rounds with NO new post AND no older timestamp
    let minTs = Infinity; // oldest post timestamp reached in this window so far
    const staleLimit = this.config.STALE_ROUND_LIMIT || 6;

    const canSplit =
      this.config.SUBDIVIDE_MONTHS && win.kind === 'year' && Array.isArray(queue);
    const splitThreshold = this.config.MONTH_SPLIT_THRESHOLD || 30;

    // This window just became a live chain — light up its grid cell.
    this.emit('window', {
      key,
      kind: win.kind,
      year: win.year,
      month: win.month,
      state: 'active',
    });

    while (!this.stopped && !rec.done) {
      await this.awaitResume(); // blocks if another chain paused everyone
      if (this.stopped) break;

      let responseText;
      try {
        responseText = await this.fetchPage(template, cursor, w);
      } catch (e) {
        backoff = Math.min(backoff * 2, 6);
        responseText = '';
      }

      const objects = parseStreamedJson(responseText);
      if (objects.length === 0) {
        consecutiveEmpty++;
        backoff = Math.min(backoff * 2, 6);
        if (consecutiveEmpty >= 3) {
          // Sustained empties across a chain usually mean a checkpoint or
          // rate-limit. Pause ALL chains and let the user resolve it once.
          this.requestPause(
            'A chain hit repeated empty responses — Facebook may be rate-limiting ' +
              'or showing a checkpoint. Solve it in the browser if shown, then click Continue.'
          );
          consecutiveEmpty = 0;
          backoff = 1;
        } else {
          await sleep(Math.floor(this.randDelay() * backoff));
        }
        continue;
      }
      consecutiveEmpty = 0;
      backoff = 1;

      const newPosts = extractPosts(objects);
      const { endCursor, hasNextPage } = extractPageInfo(objects);

      let added = 0;
      let oldest = null;
      for (const p of newPosts) {
        if (!this.postsById.has(p.id)) {
          this.postsById.set(p.id, p);
          added++;
        }
        if (typeof p.timestamp === 'number' && p.timestamp > 0) {
          oldest = oldest === null ? p.timestamp : Math.min(oldest, p.timestamp);
        }
      }
      addedTotal += added;
      round++;
      // A round makes PROGRESS if it either added a new post OR reached an older
      // timestamp than we've seen (the cursor is still advancing deeper into the
      // window — e.g. paging through a duplicate overlap band toward fresh posts).
      // Only when it does NEITHER for several rounds is the window truly stuck.
      let progressed = added > 0;
      if (oldest !== null && oldest < minTs) {
        minTs = oldest;
        progressed = true;
      }
      if (progressed) stall = 0;
      else stall++;

      const windowTotal = this.windowPostCount(win);
      this.log(
        `[${key}] round ${round}: +${added} (window ${windowTotal}, total ${this.postsById.size}) | hasNext=${hasNextPage}`
      );
      this.emit('progress', {
        window: key,
        kind: win.kind,
        year: win.year,
        month: win.month,
        windowCount: addedTotal, // new this run (for logs)
        windowTotal, // real posts in this window (what the grid shows)
        round,
        added,
        total: this.postsById.size,
        hasNext: hasNextPage,
      });

      rec.cursor = endCursor || cursor;
      state.posts = Array.from(this.postsById.values());
      this.saveProgress(state);

      // Global target across all windows.
      if (this.config.TARGET_COUNT && this.postsById.size >= this.config.TARGET_COUNT) {
        this.log(`Reached target of ${this.config.TARGET_COUNT} posts. Stopping all chains.`);
        this.stopped = true;
        break;
      }

      // Exhaustion backstop. Facebook sometimes keeps serving in-range DUPLICATES
      // with has_next_page=true and an advancing cursor, so none of the normal end
      // conditions (has_next_page=false / cursor-stall / oldest<afterTime) ever
      // fire and the chain churns "+0" forever. We stop ONLY when the window has
      // made no progress of EITHER kind — no new post AND no older timestamp — for
      // `staleLimit` rounds. That means Facebook is looping / re-serving the same
      // band, so there is nothing deeper to reach. Crucially, a duplicate OVERLAP
      // band (e.g. a month re-reading its neighbor's ±2-day padding) keeps hitting
      // older timestamps, so it counts as progress and is NOT cut short — no
      // in-window post is skipped.
      if (stall >= staleLimit && hasNextPage !== false) {
        rec.done = true;
        this.log(
          `[${key}] exhausted: no new posts and no older timestamp in ` +
            `${staleLimit} rounds (collected ${addedTotal} this window) — marking done.`
        );
        break;
      }

      // Adaptive subdivision: a year still paging after `splitThreshold` new
      // posts is dense — hand its remaining span to 12 month chains that idle
      // workers pick up. The recent months re-fetch what this chain already
      // grabbed (bounded overlap; dedup covers it). Mark the year `split` so a
      // resume re-seeds the months rather than the year.
      if (canSplit && addedTotal >= splitThreshold && hasNextPage !== false) {
        for (let m = 11; m >= 0; m--) {
          const mw = this.monthWindow(win.year, m);
          queue.push(mw);
          // Newly-queued month cells so the grid shows the year expanding.
          this.emit('window', {
            key: mw.key,
            kind: 'month',
            year: win.year,
            month: m,
            state: 'queued',
          });
        }
        rec.split = true;
        rec.done = true;
        this.emit('window', { key, kind: 'year', year: win.year, state: 'split' });
        this.log(
          `[${key}] dense (${addedTotal} posts so far) → split into 12 monthly chains.`
        );
        break;
      }

      // Window-end conditions.
      if (hasNextPage === false || !endCursor || endCursor === cursor) {
        rec.done = true;
        break;
      }
      // Client-side bound: FB pages newest→oldest; once this round dips below the
      // window start we've left the window (covers FB ignoring afterTime). The
      // next-older window has its own chain.
      if (oldest !== null && oldest < afterTime) {
        rec.done = true;
        break;
      }

      cursor = endCursor;
      await sleep(Math.floor(this.randDelay() * backoff));
    }

    if (rec.done && !rec.split) {
      // Window finished — mark its cell done with the real total in the window.
      this.emit('window', {
        key,
        kind: win.kind,
        year: win.year,
        month: win.month,
        count: this.windowPostCount(win),
        state: 'done',
      });
      this.log(`[${key}] complete (+${addedTotal} new this window).`);
    }
    this.saveProgress(state);
  }

  // ---- scout: find a page's date span without a full scrape ----------------
  /**
   * Cheaply discover a page's active date range. Scans year windows upward from
   * EARLIEST_YEAR (each empty year = 1 request) to find the first year with
   * posts (the page's birth year), then pages that year to its end for the
   * actual oldest post. Returns a summary + a suggested chain count. Read-only:
   * writes nothing to the output/progress files.
   */
  async scout() {
    try {
      this.status('launching');
      this.log('Scout: opening page to find its date range…');

      const launchArgs = [
        '--no-sandbox',
        '--disable-blink-features=AutomationControlled',
      ];
      if (this.config.USE_PROXY && this.config.PROXY_SERVER) {
        launchArgs.push(`--proxy-server=${this.config.PROXY_SERVER}`);
      }
      this.browser = await puppeteer.launch({
        headless: this.config.HEADLESS,
        userDataDir: this.config.USER_DATA_DIR,
        defaultViewport: null,
        args: launchArgs,
      });
      const openPages = await this.browser.pages();
      this.page = openPages[0] || (await this.browser.newPage());
      for (const extra of openPages.slice(1)) {
        try {
          await extra.close();
        } catch {
          /* ignore */
        }
      }
      if (this.config.USE_PROXY && this.config.PROXY_USERNAME) {
        await this.page.authenticate({
          username: this.config.PROXY_USERNAME,
          password: this.config.PROXY_PASSWORD,
        });
      }

      this.status('navigating');
      await this.page.goto(this.config.PAGE_URL, {
        waitUntil: 'domcontentloaded',
        timeout: 60000,
      });
      await sleep(2000);
      await this.dismissLoginModal();
      await this.waitForGate(
        'If a login/checkpoint is showing, solve or close it in the browser, then click Continue to scout.'
      );
      if (this.stopped) return null;

      const template = await this.captureTemplate();
      this.status('scouting');

      const floor = this.config.EARLIEST_YEAR;
      const currentYear = new Date().getFullYear();

      // 1) Scan upward for the first year that has posts.
      let birthYear = null;
      for (let y = floor; y <= currentYear && !this.stopped; y++) {
        const text = await this.fetchPage(template, null, this.yearWindow(y));
        const posts = extractPosts(parseStreamedJson(text));
        this.log(`Scout ${y}: ${posts.length} post(s) on first page`);
        this.emit('progress', { scoutYear: y, found: posts.length });
        if (posts.length > 0) {
          birthYear = y;
          break;
        }
        await sleep(Math.floor(this.randDelay()));
      }

      if (birthYear === null) {
        const result = { pageUrl: this.config.PAGE_URL, birthYear: null };
        this.status('scouted');
        this.emit('scouted', result);
        return result;
      }

      // 2) Page the birth year to its end for the actual oldest post.
      this.log(`Scout: paging ${birthYear} to its end for the oldest post…`);
      const win = this.yearWindow(birthYear);
      let cursor = null;
      let oldest = null;
      let count = 0;
      for (let round = 0; round < 300 && !this.stopped; round++) {
        const objects = parseStreamedJson(await this.fetchPage(template, cursor, win));
        const posts = extractPosts(objects);
        const { endCursor, hasNextPage } = extractPageInfo(objects);
        if (posts.length === 0) break;
        for (const p of posts) {
          count++;
          if (typeof p.timestamp === 'number' && p.timestamp > 0) {
            oldest = oldest === null ? p.timestamp : Math.min(oldest, p.timestamp);
          }
        }
        if (hasNextPage === false || !endCursor || endCursor === cursor) break;
        cursor = endCursor;
        await sleep(Math.floor(this.randDelay()));
      }

      const span = currentYear - birthYear + 1;
      const result = {
        pageUrl: this.config.PAGE_URL,
        birthYear,
        oldestTimestamp: oldest, // unix seconds, or null if undated
        oldestYearCount: count,
        currentYear,
        span,
        suggestedChains: Math.max(2, Math.min(span, 12)),
      };
      this.status('scouted');
      this.emit('scouted', result);
      this.log(
        `Scout done: birth year ${birthYear}, oldest post ${
          oldest ? new Date(oldest * 1000).toISOString().slice(0, 10) : 'unknown'
        }, span ${span} years.`
      );
      return result;
    } catch (e) {
      this.status('error');
      this.emit('error', { message: e.message });
      this.log('Scout error: ' + e.message);
      return null;
    }
  }

  _finish() {
    let finalPosts = Array.from(this.postsById.values());

    // Apply scope trimming to the final set so output matches the request.
    if (this.config.DATE_CUTOFF) {
      // Keep posts newer than the cutoff; keep undated posts (can't judge them).
      finalPosts = finalPosts.filter(
        (p) =>
          typeof p.timestamp !== 'number' ||
          p.timestamp === 0 ||
          p.timestamp >= this.config.DATE_CUTOFF
      );
    }
    if (this.config.TARGET_COUNT && finalPosts.length > this.config.TARGET_COUNT) {
      finalPosts = finalPosts.slice(0, this.config.TARGET_COUNT);
    }

    if (finalPosts.length) this.writeOutputs(finalPosts);
    if (this.stopped) {
      this.status('stopped');
      this.log('Stopped. Collected', String(finalPosts.length), 'posts so far.');
    } else {
      this.status('done');
      this.log('Done. Collected', String(finalPosts.length), 'posts.');
      this.log('Stop reason:', this.stopReason || '(none recorded)');
    }
    this.emit('done', { posts: finalPosts, stopReason: this.stopReason });
    return finalPosts;
  }

  async close() {
    try {
      if (this.browser) await this.browser.close();
    } catch {
      /* ignore */
    }
  }
}

module.exports = {
  Scraper,
  DEFAULT_CONFIG,
  // parser internals exported so they can be unit-tested / tuned:
  walk,
  parseStreamedJson,
  extractPageInfo,
  extractPosts,
  extractExtras,
  findNode,
  injectCursor,
  injectTimeWindow,
  setBatchSize,
  toCsv,
  ALL_FIELDS,
};
