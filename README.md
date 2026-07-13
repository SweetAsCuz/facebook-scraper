# Facebook Page Post Scraper (GraphQL replay)

Scrapes posts from a **public Facebook page** by capturing Facebook's own
`/api/graphql/` pagination request and replaying it in a loop — changing only
the cursor each round. No slow scrolling.

## How it works
1. Launches headed Chrome (Puppeteer) with a persistent profile (`fb-session/`).
2. Navigates to `PAGE_URL`, then **pauses for you** to solve any login/checkpoint.
3. Triggers one scroll to capture a real graphql pagination request as a
   template (with the session's `doc_id`, `fb_dtsg`, `lsd`, `variables`).
4. Replays that request from **inside the page** (`fetch` with
   `credentials:'include'`), editing only the cursor each round.
5. Parses each streamed response with a recursive walker (no hardcoded paths).

## Two ways to use it

### A) Web UI (recommended) — runs locally on your machine
```bash
pnpm install         # backend deps
pnpm build           # installs + builds the React frontend
pnpm start           # serves the UI at http://localhost:5174
```
Open **http://localhost:5174**, paste a page URL, pick **how much to scrape**,
click **Scrape**, then download the results as JSON or CSV. A live post count
shows progress and a **Stop** button ends early (keeping what's collected). A
Chrome window opens while it works — if a login/checkpoint appears, solve it
there.

**Scope options** (no need to guess a page count):
- **All posts** — everything until Facebook has no more.
- **By number** — stop after N posts (50 / 100 / 500 / custom).
- **By date** — only posts newer than a cutoff (last 7 / 30 / 90 days, or a date).
- **By pages** — stop after N pagination requests (advanced/fine control).

**Fields to collect** (toggle which columns to show/export; Text is always on):
Date, Author, Reactions, Comments, Shares, Link, Image. These are pulled from
Facebook's raw response per post (e.g. reaction/comment/share totals, the
author, and the first image).

**Log in first** (checkbox): logged out, Facebook caps page-feed pagination at
roughly **100 posts** — after that it stops returning a next page, so an
"All posts" run ends early by design. Tick **Log in first** to log into Facebook
in the Chrome window, then click **Continue**; a logged-in session pages far
deeper. (Your login persists in `fb-session/` for next time.)

**Speed** (Safe / Balanced / Fast): trades speed against block risk. Each preset
sets the delay between requests and the page size (posts per request). Bigger
page size = fewer requests for the same data. On top of the preset, an adaptive
backoff automatically slows down after empty/error rounds and resumes full speed
when healthy. Pagination is cursor-chained so requests can't be parallelised —
the real levers are page size and delay.

Developing the UI? Run both with hot-reload instead:
```bash
pnpm dev             # backend :5174 + Vite frontend :5173 (open :5173)
```

### B) Command line
```bash
pnpm cli             # scrape → posts.json + posts.csv
```

On first run it dumps the first raw response to `sample.json` so you can inspect
the real structure and tune the parser functions (grouped in the PARSER SECTION
of `scraper.js`).

## Config
Edit the `CONFIG` block at the top of `scraper.js`:

| Key | Meaning |
|-----|---------|
| `PAGE_URL` | Target public page URL |
| `USER_DATA_DIR` | Chrome profile dir (login persistence) |
| `DELAY_MIN_MS` / `DELAY_MAX_MS` | Randomized delay between requests (keep it) |
| `MAX_ROUNDS` | Safety cap (0 = unlimited) |
| `HEADLESS` | Keep `false` to solve checkpoints by hand |
| `USE_PROXY` / `PROXY_*` | Session-sticky proxy (e.g. DataImpulse) |

## Proxy (keep your IP off Facebook)
Bring your own **session-sticky residential** proxy (DataImpulse, IPRoyal, etc.
— avoid free/datacenter/rotating proxies; they're flagged and unsafe). Then:

1. `cp .env.example .env`
2. Fill `PROXY_SERVER` (host:port) and `PROXY_USERNAME` / `PROXY_PASSWORD`.
3. Restart (`pnpm dev`) and tick **Route through proxy** in the UI.

Traffic then exits via the proxy's IP, so a scraping ban lands on that IP, not
your home IP. Note: a proxy protects your IP, not your **account** — if you're
logged in, keep the pace polite (Safe/Balanced). Credentials stay in `.env`
(gitignored); they never touch the browser UI.

## Resume & crash safety
Progress (cursor + posts) is written to `progress.json` **every round**. Re-run
to resume from the saved cursor. Delete `progress.json` to start fresh.

## Failure modes handled
- **No pagination request captured** → logs every graphql `friendly_name` seen
  and tells you to scroll more / verify the page type.
- **Checkpoint mid-run** → pauses for you to resolve, then resumes.
- **Token expiry** → re-navigates to refresh the session, re-captures the
  template, resumes from the last cursor.

## Proxy note
Use a **session-sticky** endpoint and do **not** rotate IP mid-run, or Facebook
will invalidate the session token. Fill `PROXY_SERVER` / `PROXY_USERNAME` /
`PROXY_PASSWORD` and set `USE_PROXY = true`.

## Legal
Only scrape public data you're permitted to access, and respect Facebook's
Terms and applicable laws.
