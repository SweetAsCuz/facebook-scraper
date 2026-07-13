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
    if (this._gateResolve) {
      const r = this._gateResolve;
      this._gateResolve = null;
      r();
    }
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
  async fetchPage(template, cursor) {
    const form = { ...template.postForm };
    let vars = {};
    try {
      vars = JSON.parse(form.variables);
    } catch {
      vars = {};
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
  setBatchSize,
  toCsv,
  ALL_FIELDS,
};
