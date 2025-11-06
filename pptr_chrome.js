// C:\cookie-lab\pptr_chrome.js
const fs = require("fs");
const path = require("path");
const puppeteer = require("puppeteer-core");

(async () => {
  // 1) POINT TO YOUR UNPACKED EXTENSION FOLDER (must contain manifest.json)
  //    If your folder name differs, adjust here:
  const EXT_DIR = "C:\\cookie-lab\\extensions\\chromium\\Honey 18.2.1";

  // 2) Use a dedicated profile (kept so Chrome doesn't nuke the extension after launch)
  const USER_DATA_DIR = "C:\\cookie-lab\\pptr_profile";

  // 3) Pick a Chrome executable. If stable misbehaves with extensions, try Canary or Chromium:
  const CANDIDATES = [
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",                         // Stable
    "C:\\Users\\karti\\AppData\\Local\\Google\\Chrome SxS\\Application\\chrome.exe",     // Canary (if installed)
    "C:\\Program Files\\Chromium\\chrome.exe",                                           // Chromium (if installed)
  ];
  const executablePath = CANDIDATES.find(fs.existsSync);
  if (!executablePath) {
    console.error("[fatal] No Chrome/Chromium binary found in candidates. Edit CANDIDATES in pptr_chrome.js.");
    process.exit(1);
  }
  console.log(`[auto] chrome binary: ${executablePath}`);

  // --- Sanity checks ---
  const manifestPath = path.join(EXT_DIR, "manifest.json");
  if (!fs.existsSync(EXT_DIR) || !fs.existsSync(manifestPath)) {
    console.error(`[fatal] Extension folder not found or missing manifest.json:\n  ${EXT_DIR}`);
    process.exit(1);
  }

  // Normalize for Chrome CLI on Windows (forward slashes are safest)
  const EXT_DIR_CLI = EXT_DIR.replace(/\\/g, "/");
  const USER_DATA_CLI = USER_DATA_DIR.replace(/\\/g, "/");

  // ——— LAUNCH ———
  const launchArgs = [
    `--user-data-dir=${USER_DATA_CLI}`,
    `--load-extension=${EXT_DIR_CLI}`,        // ✅ Chrome stable honors this
    "--disable-notifications",
    "--no-first-run",
    "--no-default-browser-check",
    "--enable-logging=stderr",
    "--v=1",
  ];

  console.log(`[info] Loading unpacked extension from: ${EXT_DIR} (manifest v${JSON.parse(fs.readFileSync(manifestPath, "utf8")).manifest_version})`);
  const browser = await puppeteer.launch({
    headless: false,             // extensions require headful
    executablePath,
    args: launchArgs,
    defaultViewport: null,
  });

  // Give Chrome a moment to finish extension load
  // Then try to find the service worker target belonging to the extension
  // (MV3 extensions run as service workers; MV2 would be a "background_page")
  const swTarget = await browser.waitForTarget(
    t =>
      t.type() === "service_worker" &&
      /chrome-extension:\/\//.test(t.url()),
    { timeout: 8000 }
  ).catch(() => null);

  if (swTarget) {
    const url = swTarget.url();
    // chrome-extension://<EXT_ID>/_generated_background_page.html or /service_worker.js etc
    const m = url.match(/^chrome-extension:\/\/([a-p]{32})\//);
    const extId = m ? m[1] : "(unknown-id)";
    console.log(`[ok] Extension service worker detected. Extension ID: ${extId}`);
  } else {
    console.warn("[warn] No extension service worker detected yet. It still may have loaded UI-side.");
    console.warn("       If you see a yellow banner about 'extensions, apps, and user scripts' being disabled,");
    console.warn("       your Chrome build may be ignoring extension flags. Try Canary/Chromium candidate above.");
  }

  // Open the merchant page
  const page = (await browser.pages())[0] || (await browser.newPage());
  await page.goto("https://www.bestbuy.com", { waitUntil: "domcontentloaded" });

  console.log("Chrome launched. If the extension icon appears in the toolbar, it's loaded.");
  console.log("Press Ctrl+C here to quit.");
})();
