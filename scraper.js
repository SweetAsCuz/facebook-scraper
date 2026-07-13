/**
 * Facebook Page Post Scraper — CLI wrapper
 * ========================================
 *
 * Thin command-line driver around lib/scraperCore.js. All the real logic lives
 * in the Scraper class; here we just wire its events to the console and resolve
 * the manual gate by waiting for ENTER on stdin.
 *
 * The web UI (server/server.js) drives the same core, resolving the gate from a
 * "Continue" button instead.
 *
 * Usage:
 *   node scraper.js          # scrape → posts.json (+ posts.csv)
 *   node scraper.js --csv    # (CSV is always written now; flag kept for compat)
 */

'use strict';

const readline = require('readline');
const { Scraper } = require('./lib/scraperCore');

// ============================================================================
// CONFIG — edit these (mirrors DEFAULT_CONFIG in lib/scraperCore.js)
// ============================================================================
const CONFIG = {
  PAGE_URL: 'https://www.facebook.com/nasa',
  USER_DATA_DIR: './fb-session',
  OUTPUT_JSON: './posts.json',
  OUTPUT_CSV: './posts.csv',
  PROGRESS_FILE: './progress.json',
  SAMPLE_FILE: './sample.json',
  DELAY_MIN_MS: 1500,
  DELAY_MAX_MS: 3000,
  MAX_ROUNDS: 0,
  HEADLESS: false,
  CAPTURE_TIMEOUT_MS: 20000,

  // ---- Proxy (optional) ------------------------------------------------
  // Session-sticky endpoint only (e.g. DataImpulse). Do NOT rotate mid-run.
  USE_PROXY: false,
  PROXY_SERVER: '', // e.g. 'gw.dataimpulse.com:823'
  PROXY_USERNAME: '', // e.g. 'user__cr.us;sessid.abc123'
  PROXY_PASSWORD: '',
};

function waitForEnter(promptText) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });
    rl.question(promptText, () => {
      rl.close();
      resolve();
    });
  });
}

async function main() {
  const scraper = new Scraper(CONFIG);

  // Pipe events to the console.
  scraper.on('log', (msg) => {
    const t = new Date().toISOString().replace('T', ' ').replace('Z', '');
    console.log(`[${t}] ${msg}`);
  });

  // Resolve the manual gate from stdin (this is the "press ENTER" behaviour).
  scraper.on('gate', async ({ reason }) => {
    await waitForEnter(`\n>>> ${reason}\n>>> Press ENTER to continue… `);
    scraper.continue();
  });

  await scraper.run();
  console.log('\nLeaving the browser open. Press Ctrl+C to exit.');
  // Browser intentionally left open so the session/profile stays warm.
}

main().catch((err) => {
  console.error('FATAL:', err);
  process.exit(1);
});
