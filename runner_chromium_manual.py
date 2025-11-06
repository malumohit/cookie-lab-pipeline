# runner_chromium_manual.py — Chromium runner (Chrome/Edge/Brave/Opera)
# Fresh temp profile per run; redirect/refresh/new-tab watch; cookie diffs; writes to Excel.
# DEFAULT: normal mode. If privacy flags include --incognito/--inprivate, runs Incognito.
# Auto-loads extension; (optionally) pre-allows it for Incognito if an ID is supplied.

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

# ===== TARGET COOKIES =====
TARGET_ORDER = [
    "NV_MC_LC","NV_MC_FC","NV_ECM_TK_LC",
    "__attentive_utm_param_campaign","__attentive_utm_param_source","__attentive_utm_param_medium",
    "__attentive_utm_param_term","__attentive_utm_param_content",
    "campaign","campaign_id","campaign_date","campaign_source","campaign_medium","campaign_name",
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "affid","aff_id","affiliate","affiliate_id","affiliate_source","affsource","aff_source","affname",
    "aff_sub","aff_sub2","aff_sub3","aff_sub4","aff_sub5","subid","sub_id",
    "awinaffid","awcid","awcr","aw_referrer","aw_click_id",
    "cjevent","cjData",
    "irclickid","irgwc","irpid","iradid","iradname",
    "sscid","scid",
    "prms","prm_expid","prm_click",
    "gclid","gclsrc","dclid","fbclid","msclkid","ttclid","twclid","yclid",
    "_ga","_ga_*","_gid","_gat","_gat_*","_gcl_au","_gcl_aw","_gcl_dc",
    "_fbp","_fbc","_uetsid","_uetvid","_tt_enable_cookie","_ttp","_pin_unauth","_rdt_uuid",
    "AMCV_","s_cc","s_sq","mbox","mboxEdgeCluster",
    "ref","referrer","source","campaignCode","promo","promocode","coupon","coupon_code",
    "session_id","sessionid","sid",
]

_CANON_EXACT = {name.lower(): name for name in TARGET_ORDER if not name.endswith("*") and not name.endswith("_")}
_PREFIXES = []
for name in TARGET_ORDER:
    if name.endswith("*"):
        _PREFIXES.append(name[:-1].lower())
    elif name.endswith("_"):
        _PREFIXES.append(name.lower())

def _is_target_name(raw_name: str) -> str | None:
    if not raw_name:
        return None
    ln = raw_name.lower()
    if ln in _CANON_EXACT:
        return _CANON_EXACT[ln]
    for p in _PREFIXES:
        if ln.startswith(p):
            return raw_name
    return None

# ===== Cookie helpers =====
def _h(v: str) -> str:
    return hashlib.sha256((v or "").encode("utf-8")).hexdigest()[:16]

def _cookie_frame_full(c: dict) -> dict:
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

def _snapshot_targets(cookies):
    out = {}
    for c in cookies:
        cname = c.get("name") or ""
        canon = _is_target_name(cname)
        if canon:
            val = c.get("value") or ""
            out[canon] = {"value": val, "hash": _h(val)}
    return out

def _sanitize_cookie_name(name: str) -> str:
    if name is None:
        return "Cookie:UNKNOWN"
    safe = name.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return safe if safe.startswith("Cookie:") else f"Cookie:{safe}"

def _before_key(name: str) -> str: return _sanitize_cookie_name(name) + " (Before)"
def _after_key(name: str)  -> str: return _sanitize_cookie_name(name) + " (After)"

# ===== Nav helpers =====
def _get_nav_marker(driver):
    try:
        return driver.execute_script("return performance.timeOrigin || Date.now();")
    except Exception:
        return None

def _observe_redirect_refresh_and_tabs(driver, pre_url, pre_nav_ts, pre_handles, window_sec=6.0):
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
    # Pre-allow extension in Incognito by writing Default/Preferences JSON (only useful if Incognito is used)
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
    if not extension_path:
        return
    p = Path(extension_path)
    if p.is_file() and p.suffix.lower() == ".crx":
        try:
            opts.add_extension(str(p))
            return
        except Exception:
            pass
    opts.add_argument(f"--load-extension={str(p)}")  # unpacked dir or CRX fallback

def _flags_request_incognito(flags) -> bool:
    f = " ".join(flags or [])
    return ("--incognito" in f.lower()) or ("--inprivate" in f.lower())

def _make_driver(browser_binary: str | None,
                 privacy_flags,
                 privacy_prefs,
                 browser_name: str = "chrome",
                 extension_path: str | None = None,
                 chromium_extension_id: str | None = None):
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
        # Pre-allow incognito only matters if flags include it; harmless otherwise.
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

        print("\n=== MANUAL NAVIGATION ===")
        print("The browser is open at the link. The extension is already loaded.")
        print("Browse yourself to the CHECKOUT page.")
        print("When you are at CHECKOUT: type 'y' + Enter to continue (or 's' to skip).")

        before_coupon_cookies = None
        popup_seen = ""  # your answer recorded below
        caps = driver.capabilities or {}
        browser_ver = caps.get("browserVersion") or caps.get("version") or ""
        domain = urlparse(driver.current_url or url).netloc

        # Wait for checkout confirmation
        while True:
            try:
                ans = input("Are you at CHECKOUT now? [y]es / [s]kip / [n]o: ").strip().lower()
            except Exception:
                ans = ""

            if ans in ("y", "yes"):
                before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                domain = urlparse(driver.current_url or url).netloc

                # Ask if the extension popup is visible now
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
                try:
                    before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                except Exception as e:
                    print(f"Warning: could not read cookies before skip ({e}). Proceeding empty.")
                    before_coupon_cookies = []
                try:
                    domain = urlparse(driver.current_url or url).netloc
                except Exception:
                    domain = url
                print("Skipping coupon step for this run as requested.")
                after_coupon_cookies = before_coupon_cookies
                new_tabs = []
                redirect_url = ""
                refreshed = False
                popup_seen = "Skipped"
                _write_rows(job, src_workbook, out_workbook, driver, browser_ver, domain,
                            before_coupon_cookies, after_coupon_cookies, new_tabs,
                            redirect_url, refreshed, popup_seen)
                return

            else:
                print("OK, still waiting. (Tip: press 's' to skip.)")
                time.sleep(4)

        # === Your action step ===
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

        _write_rows(job, src_workbook, out_workbook, driver, browser_ver, domain,
                    before_coupon_cookies, after_coupon_cookies, new_tabs,
                    redirect_url, refreshed, popup_seen)

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
def _write_rows(job, src_workbook, out_workbook, driver, browser_ver, domain,
                before_cookies, after_cookies, new_tabs, redirect_url_final, refreshed, popup_seen):
    prefix = f"{job.get('extension_ordinal',0)}." if job.get("extension_ordinal") else ""

    new_tab_urls   = "; ".join([t.get("url","")   for t in new_tabs if t.get("url")])
    new_tab_titles = "; ".join([t.get("title","") for t in new_tabs if t.get("title")])

    before_targets = _snapshot_targets(before_cookies)
    after_targets  = _snapshot_targets(after_cookies)

    def val_before(name): return (prefix + (before_targets.get(name, {}).get("value","") or "")) if name in before_targets else ""
    def val_after(name):  return (prefix + (after_targets.get(name,  {}).get("value","") or "")) if name in after_targets  else ""

    wide = {
        "Plugin": job.get("extension_name",""),
        "Browser": job.get("browser","Chromium"),
        "Browser Privacy Level": job.get("privacy_name",""),
        "Browser Version": browser_ver,
        "Website": domain,
        "Affiliate Link": job.get("affiliate_link",""),
    }

    for ck in sorted(before_targets.keys() | after_targets.keys(), key=lambda x: x.lower()):
        wide[f"{ck} (Before)"] = val_before(ck)
        wide[f"{ck} (After)"]  = val_after(ck)

    def key(c): return (c["name"], c["domain"], c["path"])
    bmap = {key(c): c for c in before_cookies}
    amap = {key(c): c for c in after_cookies}

    changed_names = set()
    for k in amap.keys() - bmap.keys(): changed_names.add(amap[k]["name"])
    for k in bmap.keys() - amap.keys(): changed_names.add(bmap[k]["name"])
    for k in amap.keys() & bmap.keys():
        if amap[k]["value_hash"] != bmap[k]["value_hash"]:
            changed_names.add(amap[k]["name"])

    for name in sorted(changed_names, key=lambda x: (x or "").lower()):
        if _is_target_name(name):
            continue
        bvals = [c["value"] for c in before_cookies if c["name"] == name]
        avals = [c["value"] for c in after_cookies  if c["name"] == name]
        wide[_before_key(name)] = (prefix + bvals[0]) if bvals else ""
        wide[_after_key(name)]  = (prefix + avals[0]) if avals else ""

    added = [amap[k] for k in amap.keys() - bmap.keys()]
    changed = []
    for k in amap.keys() & bmap.keys():
        if amap[k]["value_hash"] != bmap[k]["value_hash"]:
            changed.append({"before": bmap[k], "after": amap[k]})

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    clean_row = {
        "Timestamp": ts,
        "Test ID": job.get("job_id",""),
        "Browser": job.get("browser","Chromium"),
        "Browser Privacy Level": job.get("privacy_name",""),
        "Browser Version": browser_ver,
        "Extension": job.get("extension_name",""),
        "Extension Version": job.get("extension_version",""),
        "Merchant": domain,
        "Affiliate Link": job.get("affiliate_link",""),
        "Coupon Applied?": "",
        "Extension Popup Seen?": popup_seen,
        "New Pages Opened": str(len(new_tabs)),
        "Cookies Added (count)": str(len(added)),
        "Cookies Changed (count)": str(len(changed)),
        "Redirect URL": redirect_url_final,
        "Refreshed?": "Yes" if refreshed else "No",
        "New Tab URLs": new_tab_urls,
        "New Tab Titles": new_tab_titles,
        "HAR Path": "",
        "Screenshots": "",
        "Status": "SUCCESS",
        "Failure Reason": "",
        "Notes": f"CookieComparisonRow=1; Tabs={len(new_tabs)}",
        "Redirect Window (s)": str(job.get("redirect_window_sec", 6.0)),
    }

    append_cookie_comparison(out_workbook, wide)
    append_clean_data_row(src_workbook, out_workbook, clean_row)

    diag_rows = []
    for ck in sorted(before_targets.keys() | after_targets.keys(), key=lambda x: x.lower()):
        b_hash = before_targets.get(ck, {}).get("hash")
        a_hash = after_targets.get(ck, {}).get("hash")
        change = "UNCHANGED"
        if ck in before_targets and ck not in after_targets: change = "REMOVED"
        elif ck in after_targets and ck not in before_targets: change = "ADDED"
        elif (b_hash is not None) and (a_hash is not None) and b_hash != a_hash: change = "CHANGED"
        if change != "UNCHANGED":
            diag_rows.append({
                "Test ID": clean_row["Test ID"],
                "Browser": clean_row["Browser"],
                "Browser Version": clean_row["Browser Version"],
                "Extension": clean_row["Extension"],
                "Extension Version": clean_row["Extension Version"],
                "Merchant": domain,
                "Affiliate Link": job.get("affiliate_link",""),
                "Cookie Name": ck,
                "Change": change,
                "Before Hash": b_hash or "",
                "After Hash": a_hash or "",
                "Observed At": ts
            })
    for tab in new_tabs:
        diag_rows.append({
            "Test ID": clean_row["Test ID"],
            "Browser": clean_row["Browser"],
            "Browser Version": clean_row["Browser Version"],
            "Extension": clean_row["Extension"],
            "Extension Version": clean_row["Extension Version"],
            "Merchant": domain,
            "Affiliate Link": job.get("affiliate_link",""),
            "Cookie Name": "(new_tab)",
            "Change": tab.get("title",""),
            "Before Hash": "",
            "After Hash": tab.get("url",""),
            "Observed At": ts
        })
    append_diagnostics(out_workbook, diag_rows)

    print("✔ Wrote: Clean_Data + Diagnostics + Cookie Field Comparison (Chromium).")
