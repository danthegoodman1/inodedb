// Renders each chart in charts.html to <chart>-light.png and <chart>-dark.png
// at 2x, for the README, through headless Chrome's DevTools protocol. Needs
// Node 22 or later, Chrome (or CHROME=<path>), and network access for the
// fonts. Edit the data in charts.html, then run `node docs/images/render.mjs`.
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const charts = ["ops", "latency", "devices"];
// Page color around the card: charts.html's capture mode sets GitHub's.
const MARGIN = 24;

const chrome = process.env.CHROME ?? (process.platform === "darwin"
  ? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
  : "google-chrome");
const profile = mkdtempSync(join(tmpdir(), "inodedb-render-"));
const browser = spawn(chrome, [
  "--headless=new", "--remote-debugging-port=0", `--user-data-dir=${profile}`,
  "--hide-scrollbars", "--no-first-run", "about:blank",
], { stdio: ["ignore", "ignore", "pipe"] });

const endpoint = await new Promise((resolve, reject) => {
  let seen = "";
  browser.stderr.on("data", chunk => {
    seen += chunk;
    const match = seen.match(/DevTools listening on (ws:\/\/\S+)/);
    if (match) resolve(match[1]);
  });
  browser.on("exit", code => reject(new Error(`Chrome exited with ${code}: ${seen}`)));
});

const socket = new WebSocket(endpoint);
await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject; });
let next = 0;
const pending = new Map();
const waiters = [];
socket.onmessage = ({ data }) => {
  const message = JSON.parse(data);
  if (message.id != null) {
    const { resolve, reject } = pending.get(message.id);
    pending.delete(message.id);
    message.error ? reject(new Error(message.error.message)) : resolve(message.result);
  } else {
    for (const waiter of waiters.splice(0)) waiter(message);
  }
};
const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
  const id = ++next;
  pending.set(id, { resolve, reject });
  socket.send(JSON.stringify({ id, method, params, sessionId }));
});
const event = (method, sessionId) => new Promise(resolve => {
  const wait = message => message.method === method && message.sessionId === sessionId
    ? resolve(message) : waiters.push(wait);
  waiters.push(wait);
});

try {
  const { targetId } = await send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await send("Target.attachToTarget", { targetId, flatten: true });
  await send("Page.enable", {}, sessionId);
  await send("Emulation.setDeviceMetricsOverride",
    { width: 1104, height: 1600, deviceScaleFactor: 2, mobile: false }, sessionId);
  for (const chart of charts) {
    for (const theme of ["light", "dark"]) {
      const loaded = event("Page.loadEventFired", sessionId);
      const url = `${pathToFileURL(join(here, "charts.html")).href}#${chart}-${theme}`;
      // A new hash on the same page loads nothing, so start from a blank one.
      await send("Page.navigate", { url: "about:blank" }, sessionId);
      await send("Page.navigate", { url }, sessionId);
      await loaded;
      const { result } = await send("Runtime.evaluate", {
        expression: `document.fonts.ready.then(() => JSON.stringify(document.getElementById(${JSON.stringify(chart)}).getBoundingClientRect()))`,
        awaitPromise: true, returnByValue: true,
      }, sessionId);
      const box = JSON.parse(result.value);
      const clip = {
        x: box.x - MARGIN, y: box.y - MARGIN,
        width: box.width + 2 * MARGIN, height: box.height + 2 * MARGIN, scale: 1,
      };
      const { data } = await send("Page.captureScreenshot",
        { format: "png", clip, captureBeyondViewport: true }, sessionId);
      const file = join(here, `${chart}-${theme}.png`);
      writeFileSync(file, Buffer.from(data, "base64"));
      console.log(`${chart}-${theme}.png ${Math.round(clip.width * 2)}x${Math.round(clip.height * 2)}`);
    }
  }
} finally {
  socket.close();
  const exited = new Promise(resolve => browser.once("exit", resolve));
  browser.kill();
  await exited;
  rmSync(profile, { recursive: true, force: true });
}
