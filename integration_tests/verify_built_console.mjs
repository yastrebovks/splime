// Exercise the actual release gate against a built Console staging tree.
// Usage: node integration_tests/verify_built_console.mjs /path/to/artifacts/console
import assert from "node:assert/strict";
import { webcrypto } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve, sep } from "node:path";

const root = resolve(process.argv[2]);
const manifest = JSON.parse(await readFile(resolve(root, "static-integrity.json"), "utf8"));
const code = await readFile(resolve(root, "releaseGate.js"));
const { bootConsoleRelease } = await import(`data:text/javascript;base64,${code.toString("base64")}`);
const storage = new Map();
const location = { protocol: "https:", href: "https://console.test/app/" };
const events = [];
const window = {
  location: { ...location, replace: () => assert.fail("built Console must boot without a repair reload") },
  sessionStorage: {
    getItem: (key) => storage.get(key) ?? null,
    setItem: (key, value) => storage.set(key, value),
    removeItem: (key) => storage.delete(key),
  },
};
const result = await bootConsoleRelease({
  releaseId: manifest.release_id,
  shellReleaseId: manifest.release_id,
  location,
  window,
  crypto: webcrypto,
  documentNonce: "artifact-verification",
  fetchFn: async (url) => {
    const path = new URL(url, location.href).pathname.replace(/^\/app\//, "");
    const file = resolve(root, path);
    assert.ok(file.startsWith(root + sep));
    events.push(path);
    return new Response(await readFile(file), { status: 200 });
  },
  installStyles: async () => events.push("styles-started"),
  importApp: async () => events.push("app-started"),
});
assert.equal(result, true);
assert.deepEqual(events.slice(-2), ["styles-started", "app-started"]);
assert.equal(window.__SPLIME_CONSOLE_EXACT_BUILD_ID__, manifest.build_id);
for (const path of Object.keys(manifest.assets)) assert.ok(events.includes(path.slice(2)), path);
console.log(JSON.stringify({ release_id: manifest.release_id, build_id: manifest.build_id, verified_assets: Object.keys(manifest.assets).length, booted: true }));
