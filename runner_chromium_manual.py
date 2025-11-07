# runner_chromium_manual.py — Chromium runner (Chrome/Edge/Brave/Opera)
# Fresh temp profile per run; redirect/refresh/new-tab watch; writes only the `campaign` cookie
# for Landing / Before / After. DEFAULT: normal mode. If privacy flags include --incognito/--inprivate,
# runs Incognito. NOTE: Chromium still loads the extension at launch (Chrome limitation).
# We mitigate by adding a Landing snapshot (pre-checkout) and recording hosts.
#
# COMPAT PATCHES:
# - Add plain 'Website' in Cookie Field Comparison (uses Before host as canonical).
# - Restore legacy Clean_Data fields: 'Coupon Applied?', 'Cookies Added (count)', 'Cookies Changed (count)'.
# - Restore 'Merchant' in Clean_Data (uses Before host as canonical).

import os
import time
import json
import hashlib
import tempfile
import shutil
import subprocess
from urllib.parse import urlparse
from pathlib import Path
from datetime import datetime

os.environ["CHROME_LOG_FILE"] = os.devnull  # reduce Chromium logging noise

from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchWindowException,
    SessionNotCreatedException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.edge.service import Service as EdgeService

_WDM_CHROME_AVAILABLE = False
_WDM_EDGE_AVAILABLE = False
try:
    from webdriver_manager.chrome import ChromeDriverManager
    _WDM_CHROME_AVAILABLE = True
except Exception:
    pass
try:
    from webdriver_manager.microsoft import EdgeChromiumDriverManager
    _WDM_EDGE_AVAILABLE = True
except Exception:
    pass

from excel_writer import append_clean_data_row, append_diagnostics, append_cookie_comparison

# ===== Single-target cookie: 'campaign' (case-insensitive exact name) =====
TARGET_NAME = "campaign"

def _is_campaign(raw_name: str) -> bool:
    return isinstance(raw_name, str) and raw_name.lower() == TARGET_NAME

def _h(v: str) -> str:
    return hashlib.sha256((v or "").encode("utf-8")).hexdigest()[:16]

def _cookie_frame_full(c: dict) -> dict:
    # Normalize a Selenium cookie dict for stable comparisons / hashing
    return {
        "name": c.get("name"),
        "value": c.get("value") or "",
        "value_hash": _h(c.get("value")),
        "domain": c.get("domain"),
        "path": c.get("path"),
        "expiry": c.get("expiry"),
        "httpOnly": c.get("httpOnly"),
        "secure": c.get("secure"),
        "sameSite": c.get("sameSite"),
    }

def _get_campaign_value(cookies):
    """
    Return first 'campaign' cookie value if present on the current domain snapshot;
    else return empty string. (Selenium exposes cookies only for the CURRENT host.)
    """
    for c in cookies:
        if _is_campaign(c.get("name")):
            return c.get("value") or ""
    return ""

# ===== Nav helpers =====
def _get_nav_marker(driver):
    try:
        return driver.execute_script("return performance.timeOrigin || Date.now();")
    except Exception:
        return None

def _observe_redirect_refresh_and_tabs(driver, pre_url, pre_nav_ts, pre_handles, window_sec=6.0):
    """
    After the user clicks the extension/popup, watch briefly for:
      - a URL change (redirect)
      - a refresh (navigation timestamp changed while URL stayed)
      - newly opened tabs/windows (collect title+URL)
    """
    t0 = time.time()
    seen_handles = set(pre_handles)
    new_tabs, redirect_url, refreshed = [], "", False
    while (time.time() - t0) < window_sec:
        try:
            handles = set(driver.window_handles)
        except Exception:
            handles = set()
        for h in list(handles - seen_handles):
            try:
                driver.switch_to.window(h)
                new_tabs.append({"title": driver.title or "", "url": driver.current_url or ""})
            except Exception:
                new_tabs.append({"title": "", "url": ""})
            finally:
                seen_handles.add(h)
        try:
            driver.switch_to.window(list(pre_handles)[0])
        except Exception:
            pass
        try:
            curr_url = driver.current_url or ""
        except Exception:
            curr_url = ""
        nav_ts = _get_nav_marker(driver)
        if curr_url and pre_url and curr_url != pre_url and not redirect_url:
            redirect_url = curr_url
        if nav_ts is not None and pre_nav_ts is not None and nav_ts != pre_nav_ts:
            if (not redirect_url) and (curr_url == pre_url):
                refreshed = True
        time.sleep(0.2)
    if not redirect_url and new_tabs:
        redirect_url = new_tabs[0].get("url", "") or ""
    try:
        driver.switch_to.window(list(pre_handles)[0])
    except Exception:
        pass
    return redirect_url, refreshed, new_tabs

# ===== Driver =====
def _apply_common_browser_flags(opts):
    opts.add_argument("--log-level=3")
    opts.add_argument("--disable-logging")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")

def _seed_incognito_permission(profile_dir: Path, ext_id: str | None):
    """
    Pre-allow an extension in Incognito by writing Default/Preferences JSON.
    This only matters if --incognito/--inprivate is used. Harmless otherwise.
    """
    if not ext_id:
        return
    default_dir = Path(profile_dir) / "Default"
    default_dir.mkdir(parents=True, exist_ok=True)
    prefs_path = default_dir / "Preferences"
    prefs = {}
    if prefs_path.exists():
        try:
            prefs = json.loads(prefs_path.read_text(encoding="utf-8") or "{}")
        except Exception:
            prefs = {}
    prefs.setdefault("extensions", {}).setdefault("settings", {}).setdefault(ext_id, {})
    prefs["extensions"]["settings"][ext_id]["incognito"] = 1  # 1 = allowed in Incognito
    prefs["extensions"]["settings"][ext_id]["state"] = 1      # enabled
    prefs_path.write_text(json.dumps(prefs, ensure_ascii=False), encoding="utf-8")

def _attach_extension(opts, extension_path: str | None):
    """
    Chrome/Edge/Brave/Opera can only load extensions at startup via Options.
    """
    if not extension_path:
        return
    p = Path(extension_path)
    if p.is_file() and p.suffix.lower() == ".crx":
        try:
            opts.add_extension(str(p))
            return
        except Exception:
            pass
    # Unpacked dir or CRX fallback via --load-extension
    opts.add_argument(f"--load-extension={str(p)}")

def _flags_request_incognito(flags) -> bool:
    f = " ".join(flags or [])
    return ("--incognito" in f.lower()) or ("--inprivate" in f.lower())

def _make_driver(browser_binary: str | None,
                 privacy_flags,
                 privacy_prefs,
                 browser_name: str = "chrome",
                 extension_path: str | None = None,
                 chromium_extension_id: str | None = None):
    """
    Create a temp profile per run so state is isolated. We always pass --user-data-dir to the temp dir.
    """
    b = (browser_name or "chrome").lower()

    if b == "edge":
        opts = EdgeOptions()
        profile_dir = Path(tempfile.mkdtemp(prefix="edge_profile_"))
        opts.add_argument(f"--user-data-dir={str(profile_dir)}")
        if browser_binary:
            try:
                opts.binary_location = browser_binary
            except Exception:
                pass
        _apply_common_browser_flags(opts)
        for f in (privacy_flags or []):
            opts.add_argument(f)
        if privacy_prefs:
            opts.add_experimental_option("prefs", privacy_prefs)
        try:
            if _flags_request_incognito(privacy_flags):
                _seed_incognito_permission(profile_dir, chromium_extension_id)
        except Exception:
            pass
        _attach_extension(opts, extension_path)
        try:
            driver = webdriver.Edge(options=opts, service=EdgeService(log_output=subprocess.DEVNULL))
            driver._temp_profile_dir = profile_dir
            return driver
        except (SessionNotCreatedException, WebDriverException, Exception):
            if not _WDM_EDGE_AVAILABLE:
                raise
            service = EdgeService(EdgeChromiumDriverManager().install(), log_output=subprocess.DEVNULL)
            driver = webdriver.Edge(options=opts, service=service)
            driver._temp_profile_dir = profile_dir
            return driver

    # Chrome / Brave / Opera via ChromeDriver
    opts = ChromeOptions()
    profile_dir = Path(tempfile.mkdtemp(prefix=f"{b}_profile_"))
    opts.add_argument(f"--user-data-dir={str(profile_dir)}")
    if browser_binary:
        opts.binary_location = browser_binary
    _apply_common_browser_flags(opts)
    for f in (privacy_flags or []):
        opts.add_argument(f)
    if privacy_prefs:
        opts.add_experimental_option("prefs", privacy_prefs)

    try:
        if _flags_request_incognito(privacy_flags):
            _seed_incognito_permission(profile_dir, chromium_extension_id)
    except Exception:
        pass
    _attach_extension(opts, extension_path)

    try:
        driver = webdriver.Chrome(options=opts, service=ChromeService(log_output=subprocess.DEVNULL))
        driver._temp_profile_dir = profile_dir
        return driver
    except (SessionNotCreatedException, WebDriverException, Exception):
        if not _WDM_CHROME_AVAILABLE:
            raise
        service = ChromeService(ChromeDriverManager().install(), log_output=subprocess.DEVNULL)
        driver = webdriver.Chrome(options=opts, service=service)
        driver._temp_profile_dir = profile_dir
        return driver

# ===== Main (called by pipeline) =====
def run_one(job: dict, src_workbook: Path, out_workbook: Path):
    """
    DEFAULT: normal window.
    If matrix privacy flags include --incognito/--inprivate, the browser launches Incognito.
    NOTE: Landing is captured immediately after the first navigation; Before is at CHECKOUT;
          After is post action (popup/toolbar) with a short watch window.
    """
    driver = _make_driver(
        job.get("browser_binary"),
        job.get("privacy_flags") or [],
        job.get("privacy_prefs") or {},
        job.get("browser") or "chrome",
        extension_path=job.get("extension_path"),
        chromium_extension_id=job.get("chromium_extension_id"),  # may be absent/None
    )
    temp_profile = getattr(driver, "_temp_profile_dir", None)

    try:
        url = job["affiliate_link"]

        # Navigate (retry once if window disappeared)
        for attempt in (1, 2):
            try:
                driver.get(url)
                break
            except NoSuchWindowException:
                if attempt == 1:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = _make_driver(
                        job.get("browser_binary"),
                        job.get("privacy_flags") or [],
                        job.get("privacy_prefs") or {},
                        job.get("browser") or "chrome",
                        extension_path=job.get("extension_path"),
                        chromium_extension_id=job.get("chromium_extension_id"),
                    )
                    temp_profile = getattr(driver, "_temp_profile_dir", None)
                    continue
                else:
                    raise

        # ---- LANDING SNAPSHOT (taken before any manual navigation to checkout) ----
        landing_host = urlparse(driver.current_url or url).netloc
        landing_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]

        print("\n=== MANUAL NAVIGATION ===")
        print("The browser is open at the link. The extension (Chromium) is already loaded.")
        print("Navigate yourself to the CHECKOUT page.")
        print("When you are at CHECKOUT: type 'y' + Enter to continue (or 's' to skip).")

        before_coupon_cookies = None
        popup_seen = ""  # your answer recorded below
        caps = driver.capabilities or {}
        browser_ver = caps.get("browserVersion") or caps.get("version") or ""

        # Wait for checkout confirmation
        while True:
            try:
                ans = input("Are you at CHECKOUT now? [y]es / [s]kip / [n]o: ").strip().lower()
            except Exception:
                ans = ""

            if ans in ("y", "yes"):
                before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                before_host = urlparse(driver.current_url or url).netloc

                while True:
                    try:
                        q = input("Do you see the extension popup right now? [y]es / [n]o: ").strip().lower()
                    except Exception:
                        q = ""
                    if q in ("y", "yes"):
                        popup_seen = "Yes"
                        break
                    if q in ("n", "no"):
                        popup_seen = "No"
                        break
                    print("Please type 'y' or 'n'.")
                break

            elif ans in ("s", "skip"):
                # If skipping, Before == After; still write the row.
                try:
                    before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                except Exception as e:
                    print(f"Warning: could not read cookies before skip ({e}). Proceeding empty.")
                    before_coupon_cookies = []
                before_host = urlparse(driver.current_url or url).netloc
                after_coupon_cookies = before_coupon_cookies
                after_host = before_host
                new_tabs = []
                redirect_url = ""
                refreshed = False
                popup_seen = "Skipped"
                _write_rows(
                    job, src_workbook, out_workbook, driver, browser_ver,
                    landing_host, before_host, after_host,
                    landing_cookies, before_coupon_cookies, after_coupon_cookies,
                    new_tabs, redirect_url, refreshed, popup_seen
                )
                return

            else:
                print("OK, still waiting. (Tip: press 's' to skip.)")
                time.sleep(4)

        # === ACTION ===
        print("\n=== ACTION ===")
        print("If a popup is visible, click it now; otherwise click the extension's toolbar button.")
        print("Press ENTER here right after you do that.")
        pre_handles = set(driver.window_handles)
        pre_url = driver.current_url or ""
        pre_nav_ts = _get_nav_marker(driver)
        try:
            input()
        except Exception:
            pass

        redirect_url, refreshed, new_tabs = _observe_redirect_refresh_and_tabs(
            driver, pre_url, pre_nav_ts, pre_handles, window_sec=float(job.get("redirect_window_sec", 6.0))
        )
        after_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
        after_host = urlparse(driver.current_url or url).netloc

        _write_rows(
            job, src_workbook, out_workbook, driver, browser_ver,
            landing_host, before_host, after_host,
            landing_cookies, before_coupon_cookies, after_coupon_cookies,
            new_tabs, redirect_url, refreshed, popup_seen
        )

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        if temp_profile:
            try:
                shutil.rmtree(temp_profile, ignore_errors=True)
            except Exception:
                pass

# ===== Output builders =====
def _write_rows(job, src_workbook, out_workbook, driver, browser_ver,
                landing_host, before_host, after_host,
                landing_cookies, before_cookies, after_cookies,
                new_tabs, redirect_url_final, refreshed, popup_seen):

    # Extract only 'campaign' values at each snapshot
    def _val(cset):
        for c in cset:
            if _is_campaign(c.get("name")):
                return c.get("value") or ""
        return ""

    landing_campaign = _val(landing_cookies)
    before_campaign  = _val(before_cookies)
    after_campaign   = _val(after_cookies)

    prefix = f"{job.get('extension_ordinal',0)}." if job.get("extension_ordinal") else ""

    # Wide row: include metadata + only three campaign columns
    # COMPAT: add plain 'Website' using BEFORE host.
    wide = {
        "Plugin": job.get("extension_name",""),
        "Browser": job.get("browser","Chromium"),
        "Browser Privacy Level": job.get("privacy_name",""),
        "Browser Version": browser_ver,
        "Website": before_host,  # compat
        "Website (Landing)": landing_host,
        "Website (Before)": before_host,
        "Website (After)": after_host,
        "Affiliate Link": job.get("affiliate_link",""),
        "campaign (Landing)": prefix + landing_campaign if landing_campaign else "",
        "campaign (Before)":  prefix + before_campaign  if before_campaign  else "",
        "campaign (After)":   prefix + after_campaign   if after_campaign   else "",
    }

    # Clean_Data summary
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    new_tab_urls   = "; ".join([t.get("url","")   for t in new_tabs if t.get("url")])
    new_tab_titles = "; ".join([t.get("title","") for t in new_tabs if t.get("title")])

    # COMPAT: restore 'Merchant' and legacy counters/flag (set to safe defaults).
    clean_row = {
        "Timestamp": ts,
        "Test ID": job.get("job_id",""),
        "Browser": job.get("browser","Chromium"),
        "Browser Privacy Level": job.get("privacy_name",""),
        "Browser Version": browser_ver,
        "Extension": job.get("extension_name",""),
        "Extension Version": job.get("extension_version",""),
        "Merchant": before_host,                    # compat
        "Merchant (Landing)": landing_host,
        "Merchant (Before)": before_host,
        "Merchant (After)": after_host,
        "Affiliate Link": job.get("affiliate_link",""),
        "Coupon Applied?": "",                      # compat
        "Cookies Added (count)": "0",               # compat (campaign-only mode)
        "Cookies Changed (count)": "0",             # compat (campaign-only mode)
        "Extension Popup Seen?": popup_seen,
        "Redirect URL": redirect_url_final,
        "Refreshed?": "Yes" if refreshed else "No",
        "New Pages Opened": str(len(new_tabs)),
        "New Tab URLs": new_tab_urls,
        "New Tab Titles": new_tab_titles,
        "Status": "SUCCESS",
        "Failure Reason": "",
        "Notes": "Only-campaign-capture",
        "Redirect Window (s)": str(job.get("redirect_window_sec", 6.0)),
    }

    # Diagnostics: only log campaign changes (ADDED / REMOVED / CHANGED) + host context
    diag_rows = []
    def _hash(v): return _h(v) if v is not None else ""
    b_hash = _hash(before_campaign)
    a_hash = _hash(after_campaign)

    if before_campaign and not after_campaign:
        change = "REMOVED"
    elif after_campaign and not before_campaign:
        change = "ADDED"
    elif before_campaign and after_campaign and (b_hash != a_hash):
        change = "CHANGED"
    else:
        change = "UNCHANGED"

    if change != "UNCHANGED":
        diag_rows.append({
            "Test ID": clean_row["Test ID"],
            "Browser": clean_row["Browser"],
            "Browser Version": clean_row["Browser Version"],
            "Extension": clean_row["Extension"],
            "Extension Version": clean_row["Extension Version"],
            "Merchant": before_host,  # compat primary
            "Merchant (Before)": before_host,
            "Merchant (After)": after_host,
            "Affiliate Link": job.get("affiliate_link",""),
            "Cookie Name": "campaign",
            "Change": change,
            "Before Hash": b_hash or "",
            "After Hash": a_hash or "",
            "Observed At": ts,
            "Snapshot Before Host": before_host,   # context
            "Snapshot After Host": after_host,     # context
        })

    append_cookie_comparison(out_workbook, wide)
    append_clean_data_row(src_workbook, out_workbook, clean_row)
    append_diagnostics(out_workbook, diag_rows)
    print("✔ Wrote: Clean_Data + Diagnostics + Cookie Field Comparison (Chromium, campaign-only).")
