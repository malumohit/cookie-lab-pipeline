// puppeteer_chromium_runner.mjs
// Usage (called by Python):
//   node puppeteer_chromium_runner.mjs --url "<aff_link>" --ext "<unpacked_extension_dir>"
//     --redirectWindow 6 --binary "C:\\Path\\to\\chrome.exe" --privacyFlags "--disable-third-party-cookies,--incognito"
//     --jobId "<id>" --browserName "chrome" --privacyName "default"
//
// Prints a single JSON to STDOUT with:
// { browserVersion, popupSeen, newTabs: [{title,url}], beforeCookies: [...], afterCookies: [...], redirectUrl, refreshed }

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";
import puppeteer from "puppeteer";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const m = a.match(/^--([^=]+)=(.*)$/);
    if (m) return [m[1], m[2]];
    if (a.startsWith("--")) return [a.replace(/^--/, ""), "true"];
    return ["_", a];
  })
);

function parseCSVFlags(s) {
  if (!s) return [];
  return String(s)
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean);
}

function mkTmpProfile(prefix = "pupp-chrome-") {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  return dir;
}

function rlQuestion(rl, q) {
  return new Promise((res) => rl.question(q, (a) => res(String(a || "").trim())));
}

(async () => {
  const url = args.url || "about:blank";
  const extDir = args.ext || ""; // unpacked extension folder (must contain manifest.json)
  const binary = args.binary && args.binary !== "None" ? args.binary : undefined;
  const redirectWindow = Math.max(0, Number(args.redirectWindow || 6));
  const privacyFlags = parseCSVFlags(args.privacyFlags);
  const browserName = (args.browserName || "chrome").toLowerCase();

  const userDataDir = mkTmpProfile(`${browserName}_profile_`);
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

  let browser;
  try {
    const launchArgs = [
      "--no-first-run",
      "--no-default-browser-check",
      "--disable-notifications",
      `--user-data-dir=${userDataDir}`,
    ];

    // Load unpacked extension (critical)
    if (extDir) {
      launchArgs.push(`--load-extension=${extDir}`);
    }

    // Privacy flags (may include --disable-third-party-cookies, --incognito, etc.)
    for (const f of privacyFlags) {
      // Chrome will run; note some flags (e.g. --disable-extensions-except) are ignored
      launchArgs.push(f);
    }

    browser = await puppeteer.launch({
      headless: false,    // extensions require headful
      args: launchArgs,
      userDataDir,
      executablePath: binary, // if undefined, Puppeteer uses its bundled Chromium; better to pass Chrome.exe
    });

    const pages = await browser.pages();
    const page = pages[0] || (await browser.newPage());
    const versionInfo = await browser.version(); // e.g., "HeadlessChrome/123.0.XXXX"
    const browserVersion = versionInfo.split("/")[1] || versionInfo;

    // Go to link
    await page.goto(url, { waitUntil: "domcontentloaded" });

    // BEFORE snapshot
    const beforeCookies = await page.cookies();

    // Prompt: navigate to checkout
    let ans;
    for (;;) {
      ans = (await rlQuestion(
        rl,
        "Are you at CHECKOUT now? [y]es / [s]kip / [n]o: "
      )).toLowerCase();
      if (["y", "yes", "s", "skip", "n", "no"].includes(ans)) break;
    }

    let popupSeen = "";
    if (ans === "y" || ans === "yes") {
      for (;;) {
        const q = (await rlQuestion(
          rl,
          "Do you see the extension popup right now? [y]es / [n]o: "
        )).toLowerCase();
        if (["y", "yes"].includes(q)) {
          popupSeen = "Yes";
          break;
        }
        if (["n", "no"].includes(q)) {
          popupSeen = "No";
          break;
        }
      }
    } else if (ans === "s" || ans === "skip") {
      popupSeen = "Skipped";
    }

    // If skip, AFTER=BEFORE
    let afterCookies = beforeCookies;
    let redirectUrl = "";
    let refreshed = false;
    const newTabs = [];

    if (ans === "s" || ans === "skip") {
      // nothing
    } else {
      console.log("\n=== ACTION ===");
      console.log(
        popupSeen === "Yes"
          ? "Great — click the popup now to apply/activate."
          : "No popup? Click the extension’s toolbar button to apply/activate."
      );
      await rlQuestion(rl, "When you've clicked it, press ENTER here.");

      // Watch time-window for redirect/refresh/new targets
      const startTargets = new Set((await browser.targets()).map((t) => t._targetId));
      const preUrl = page.url();

      // Quick heuristic for refresh: track nav events
      let navChanged = false;
      const onFrNav = (frame) => {
        try {
          if (frame === page.mainFrame()) navChanged = true;
        } catch {}
      };
      page.on("framenavigated", onFrNav);

      const t0 = Date.now();
      while (Date.now() - t0 < redirectWindow * 1000) {
        await new Promise((r) => setTimeout(r, 200));
      }
      page.off("framenavigated", onFrNav);

      const postUrl = page.url();
      if (postUrl && postUrl !== preUrl) {
        redirectUrl = postUrl;
      } else if (navChanged && postUrl === preUrl) {
        refreshed = true;
      }

      // New tabs/windows
      const endTargets = (await browser.targets()).filter((t) => !startTargets.has(t._targetId));
      for (const t of endTargets) {
        try {
          const p = await t.page();
          if (p) newTabs.push({ title: await p.title(), url: p.url() });
        } catch {}
      }

      // AFTER snapshot
      afterCookies = await page.cookies();
    }

    rl.close();

    // Output JSON to stdout
    const out = {
      browserVersion,
      popupSeen,
      newTabs,
      afterCookies,
      beforeCookies,
      redirectUrl,
      refreshed,
    };
    process.stdout.write(JSON.stringify(out));
  } catch (e) {
    try { rl.close(); } catch {}
    // Print JSON error shape so Python wrapper can surface it
    process.stdout.write(JSON.stringify({ error: String(e && e.stack ? e.stack : e) }));
  } finally {
    // do NOT delete userDataDir here; Python wrapper cleans it
  }
})();
