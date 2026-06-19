// Integration check: serve public/ and drive the demo in real Chrome, confirming the actual
// Web Worker round-trip (pool + worker + transit over postMessage) and a speedup.
//
//   npx shadow-cljs release example-main example-worker
//   node scripts/browser-check.mjs
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { chromium } from 'playwright-core';

const ROOT = path.resolve('public');
const TYPES = { '.html': 'text/html', '.js': 'application/javascript', '.css': 'text/css', '.map': 'application/json' };

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/') p = '/index.html';
  const fp = path.join(ROOT, p);
  fs.readFile(fp, (err, data) => {
    if (err) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, { 'content-type': TYPES[path.extname(fp)] || 'application/octet-stream' });
    res.end(data);
  });
});

await new Promise(r => server.listen(0, r));
const port = server.address().port;

const browser = await chromium.launch({ channel: 'chrome' });
const page = await browser.newPage();
const logs = [];
page.on('console', m => logs.push(m.text()));
page.on('pageerror', e => logs.push('PAGEERROR: ' + e.message));

let code = 1;
try {
  await page.goto(`http://localhost:${port}/`);
  await page.click('#run');
  await page.waitForFunction(() => {
    const t = document.getElementById('out').textContent;
    return t.includes('single-thread:') && t.includes('parallel (');
  }, { timeout: 60000 });

  const out = await page.evaluate(() => document.getElementById('out').textContent);
  console.log('=== #out ===\n' + out.trim());
  if (logs.length) console.log('=== console ===\n' + logs.join('\n'));

  const single = Number(/single-thread:\s*(\d+)\s*ms\s*\(sum=(\d+)\)/.exec(out)?.[1]);
  const singleSum = /single-thread:.*\(sum=(\d+)\)/.exec(out)?.[1];
  const parallel = Number(/parallel \(n=\d+\):\s*(\d+)\s*ms\s*\(sum=(\d+)\)/.exec(out)?.[1]);
  const parallelSum = /parallel \(n=\d+\):.*\(sum=(\d+)\)/.exec(out)?.[1];

  const sumsMatch = singleSum && singleSum === parallelSum;
  console.log(`\nsums match: ${sumsMatch} (single=${singleSum}, parallel=${parallelSum})`);
  console.log(`timing: single=${single}ms parallel=${parallel}ms  speedup=${(single / parallel).toFixed(2)}x`);

  if (!sumsMatch) { console.error('FAIL: worker results do not match single-threaded results'); }
  else if (!(parallel < single)) { console.error('FAIL: no speedup from worker pool'); }
  else { console.log('PASS'); code = 0; }
} catch (e) {
  console.error('ERROR:', e.message);
  if (logs.length) console.log('=== console ===\n' + logs.join('\n'));
} finally {
  await browser.close();
  server.close();
  process.exit(code);
}
