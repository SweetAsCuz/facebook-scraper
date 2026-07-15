# How the Facebook page scraper works (and its hard limits)

This documents what the scraper actually does, why it behaves the way it does,
and what is and isn't possible — captured from live investigation against
`https://www.facebook.com/Badmintonwithu2013` (a Page with 567K followers).

See also:
- **`api-params.md`** — the full, live-captured GraphQL request variables.
- **`raw-response-fields.md`** — every field present in a raw feed response.

---

## 1. It replays the API — it does NOT scrape the DOM

The scraper is **not** reading posts off the rendered page. It:

1. Opens the page in a real Chrome (Puppeteer), logged in via the persistent
   `fb-session/` profile.
2. **Captures** the page's own GraphQL pagination request at the CDP level
   (`page.on('request')` in `captureTemplate()`, `lib/scraperCore.js`) — the URL,
   headers, and full POST body (tokens + query id + variables).
3. **Replays** that request in a loop from inside the page context
   (`page.evaluate(fetch(...))` in `fetchPage()`), changing only the **cursor**
   each round, and parses the JSON response.

So it is already "calling the API directly." The browser is there to provide
auth, the current query template, and a genuine request fingerprint — not to
render posts.

### What Puppeteer is actually for
| Job | Why it needs a browser |
|---|---|
| Auth session | `fb-session/` holds the logged-in cookies |
| Live query capture | FB rotates the `doc_id`/token; capturing keeps it current |
| Request fingerprint | Firing from the page context looks like real Chrome (TLS/header order); raw `node-fetch` is easier to block |
| Checkpoints | A visible window lets you solve challenges |

You *could* go pure-HTTP (no Puppeteer), but you'd hand-roll cookie/`fb_dtsg`
refresh, hardcode a `doc_id` that breaks on FB updates, and get blocked sooner.
Dropping Puppeteer would **not** make it faster (see §4).

---

## 2. The feed query and its parameters

**Query:** `ProfileCometTimelineFeedRefetchQuery` · `doc_id 27631023009915089`
· `POST /api/graphql/`. It sends ~45 variables; ~30 are relay feature-flags
(`__relay_internal__pv__*`) and irrelevant. The meaningful ones:

| variable | meaning | behavior |
|---|---|---|
| `cursor` | pagination pointer | opaque **encrypted** `organic_cursor`; only the previous response reveals the next one |
| `count` | page size requested | **ignored** — FB returns 3 per request for this profile feed regardless |
| `beforeTime` | upper time bound (unix s) | the **year filter** sets this (year 2022 → `1672502399` = 2022-12-31 23:59:59 GMT+8) |
| `afterTime` | lower time bound (unix s) | accepted, but the year UI leaves it `null` |
| `feedLocation` | `"TIMELINE"` | which feed surface |
| `postedBy` | `{group:"OWNER"}` | owner's posts vs tagged/others |
| `omitPinnedPost`, `privacy`, `taggedInOnly` | minor filters | dialog defaults |
| `id` | `100064545286089` | the target page/profile id |

Top-level POST form fields carry auth/session: `fb_dtsg`, `lsd`, `av`, `__user`,
`fb_api_req_friendly_name`, `server_timestamps`, etc.

---

## 3. Why you only get ~3 posts per request

For this Page's logged-in timeline, Facebook returns **exactly 3 stories per
pagination request** and **ignores the `count` we ask for**. This is server-side
throttling of the authenticated feed, not a scraper bug (verified by counting
distinct `post_id`s in a raw response, and by watching the live feed paginate).

- **Logged out:** FB serves the public page as a big pre-rendered batch — ~100
  posts fast — then **hard-caps around ~100**. Fast but shallow.
- **Logged in:** 3/round, but **unbounded depth** (a real run reached 5,668
  posts). Slow but complete.

You cannot have both: fast+shallow (logged out) or slow+deep (logged in).

---

## 4. Why it can't just "go faster" or fire in parallel

### The cursor is serial and encrypted
Each response contains the **only** key to the next page — an encrypted
`organic_cursor`. You cannot construct request N+1 without response N, so a
single feed chain **cannot be parallelized**. (Decoded structure:
`organic_cursor` [encrypted blob] / `ad_cursor` / `global_position` / `offset` /
`last_ad_position`; the ints are validated against the encrypted blob.)

### The only real speed levers
1. **Drop the proxy** (~2×). Measured ~4s/round, of which ~3.3s is the proxy
   round-trip. Trade-off: FB sees your real IP.
2. **Fast mode** — shortens the delay *between* requests (not the count).
3. **Time-window partitioning (the real unlock)** — because `afterTime` +
   `beforeTime` bound a date range, you can run **independent, non-overlapping
   chains concurrently** (e.g. one per year or month). Each is still 3/round
   internally, but N run at once → ~N× throughput.
   - The year-filter UI only sets `beforeTime`, so its chains overlap. Setting
     **both** bounds yourself gives clean partitions.
   - Keep concurrency modest (~3–4): many simultaneous streams from one account
     raise the checkpoint risk.

Dropping Puppeteer does **not** help — the bottleneck is FB's per-request time +
the serial cursor, not browser overhead.

---

## 5. Speed modes

| Mode | Delay between requests | Batch requested |
|---|---|---|
| Safe | 1.5–3s | 20 |
| Balanced | 0.9–1.8s | 25 |
| Fast | 0.4–0.9s | 30 |

The batch column is **ineffective on this page** (FB ignores it → always 3), so
here the modes differ **only in delay**. On a classic Page that honors the batch
size (returning 20–30/round) the batch column would matter.

---

## 6. Sessions, resume, and fields

- **Login session:** the `fb-session/` Chrome profile persists your login across
  runs — log in once in the window.
- **Resume:** after every round the scraper writes `progress.json`
  (`{ pageUrl, cursor, posts, done }`). A new run **resumes** an unfinished run
  of the **same page** from its cursor; it **ignores** a `done:true` file or a
  cursor from a *different* page (both start fresh). So restarting continues
  where it left off without re-scraping.
- **Fields captured (10):** `id, text, timestamp, url, author, authorUrl,
  reactions, comments, shares, image`. These are always saved to `posts.json` /
  `progress.json`. The UI field checkboxes only affect the **table display and
  CSV export** — never what's scraped. The raw response holds far more (~345
  keys; see `raw-response-fields.md`) if you want to extract more (all images,
  video URLs, reaction breakdown, top comments, reshare origin, etc.).
