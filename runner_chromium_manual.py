# runner_chromium_manual.py — manual-browse runner (Chromium family)
import time, hashlib, tempfile, shutil
from urllib.parse import urlparse, parse_qs, urlsplit
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common.exceptions import NoSuchWindowException

from excel_writer import append_clean_data_row, append_diagnostics, append_cookie_comparison

TARGET_ORDER = [
    "NV_MC_LC",
    "NV_MC_FC",
    "__attentive_utm_param_campaign",
    "__attentive_utm_param_source",
    "NV_ECM_TK_LC",
]
TARGET_SET = set(TARGET_ORDER)

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
        n = c["name"]
        if n in TARGET_SET:
            out[n] = {"value": c["value"], "hash": c["value_hash"]}
    return out

def _get_nav_marker(driver):
    try:
        return driver.execute_script("return (performance.timeOrigin||performance.timing.navigationStart)||Date.now();")
    except Exception:
        return None

def _observe_redirect_refresh_and_tabs(driver, pre_url, pre_nav_ts, pre_handles, window_sec=6.0):
    t0 = time.time()
    seen_handles = set(pre_handles)
    new_tabs = []
    redirect_url = ""
    refreshed = False

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
        redirect_url = new_tabs[0].get("url", "")

    try:
        driver.switch_to.window(list(pre_handles)[0])
    except Exception:
        pass

    return redirect_url, refreshed, new_tabs

def _sanitize_header(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return "Cookie:__COOKIE_UNNAMED__"
    return f"Cookie:{name.replace('\n',' ').replace('\r',' ')}"

def _extract_utm_campaign(url: str) -> str:
    try:
        q = parse_qs(urlsplit(url).query)
        return q.get("utm_campaign", [""])[0]
    except Exception:
        return ""

def _make_driver(browser_binary: str | None, ext_path: str | None, flags: list[str] | None, profile_dir: Path):
    opts = ChromeOptions()
    if browser_binary:
        opts.binary_location = browser_binary
    # clean profile per run
    opts.add_argument(f"--user-data-dir={str(profile_dir)}")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-notifications")
    for flag in (flags or []):
        opts.add_argument(flag)
    # extension load (packed/unpacked)
    if ext_path:
        p = Path(ext_path)
        if p.is_dir():
            opts.add_argument(f"--load-extension={p}")
        else:
            opts.add_argument(f"--disable-extensions-except={p}")
            opts.add_argument(f"--load-extension={p}")
    return webdriver.Chrome(options=opts)

def run_one(job: dict, src_workbook: Path, out_workbook: Path):
    ext_ordinal = job.get("extension_ordinal", 0)
    prefix = f"{ext_ordinal}." if ext_ordinal else ""
    privacy_name = job.get("privacy_name", "default")

    profile_dir = Path(tempfile.mkdtemp(prefix=f"{job.get('browser','chromium')}_profile_"))
    driver = _make_driver(job.get("browser_binary"), job.get("extension_path"), job.get("privacy_flags"), profile_dir)
    try:
        url = job["affiliate_link"]
        for attempt in (1, 2):
            try:
                driver.get(url); break
            except NoSuchWindowException:
                if attempt == 1:
                    try: driver.quit()
                    except Exception: pass
                    driver = _make_driver(job.get("browser_binary"), job.get("extension_path"), job.get("privacy_flags"), profile_dir)
                    continue
                raise

        print("\n=== MANUAL NAVIGATION ===")
        print("Navigate to CHECKOUT. When there, type 'y'. 's' to skip coupon step.")
        before_coupon_cookies = None
        ts_before = ""
        popup_answer = "Unknown"
        caps = driver.capabilities or {}
        browser_ver = caps.get("browserVersion") or caps.get("version") or ""
        domain = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc

        while True:
            try:
                ans = input("Are you at CHECKOUT now? [y]es / [s]kip / [n]o: ").strip().lower()
            except Exception:
                ans = ""

            if ans in ("y", "yes"):
                try:
                    popup_ans = input("Do you see the extension popup? [y/n]: ").strip().lower()
                    popup_answer = "Yes" if popup_ans in ("y","yes") else "No"
                except Exception:
                    popup_answer = "Unknown"

                ts_before = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                domain = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc
                break

            elif ans in ("s", "skip"):
                ts_before = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                try:
                    before_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]
                except Exception:
                    before_coupon_cookies = []
                domain = urlparse(driver.current_url or job.get("affiliate_link", "")).netloc

                ts_after = ts_before
                after_coupon_cookies = before_coupon_cookies
                new_tabs = []
                redirect_url = ""
                refreshed = False
                goto_comparison_and_write(
                    job, src_workbook, out_workbook, driver, browser_ver, domain,
                    before_coupon_cookies, after_coupon_cookies, new_tabs, prefix,
                    redirect_url, refreshed, ts_before, ts_after, popup_answer, privacy_name
                )
                return
            else:
                print("OK, waiting… (or press 's' to skip)")
                time.sleep(5)

        print("\n=== ACTION ===")
        print("Click the extension’s Apply/Activate. Press ENTER here after you click.")
        pre_handles = set(driver.window_handles)
        pre_url = driver.current_url or ""
        pre_nav_ts = _get_nav_marker(driver)
        try: input()
        except Exception: pass

        redirect_url, refreshed, new_tabs = _observe_redirect_refresh_and_tabs(
            driver, pre_url, pre_nav_ts, pre_handles, window_sec=float(job.get("redirect_window_sec", 6.0))
        )

        ts_after = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        after_coupon_cookies = [_cookie_frame_full(c) for c in driver.get_cookies()]

        goto_comparison_and_write(
            job, src_workbook, out_workbook, driver, browser_ver, domain,
            before_coupon_cookies, after_coupon_cookies, new_tabs, prefix,
            redirect_url, refreshed, ts_before, ts_after, popup_answer, privacy_name
        )

    finally:
        try: driver.quit()
        except Exception: pass
        try: shutil.rmtree(profile_dir, ignore_errors=True)
        except Exception: pass

def goto_comparison_and_write(job, src_workbook, out_workbook,
                              driver, browser_ver, domain,
                              before_cookies, after_cookies,
                              new_tabs, prefix,
                              redirect_url_final, refreshed,
                              ts_before, ts_after, popup_answer, privacy_name):
    new_tab_urls = "; ".join([t.get("url","") for t in new_tabs if t.get("url")])
    new_tab_titles = "; ".join([t.get("title","") for t in new_tabs if t.get("title")])

    before_targets = _snapshot_targets(before_cookies)
    after_targets  = _snapshot_targets(after_cookies)

    def val_before(name): return (prefix + (before_targets.get(name, {}).get("value", "") or "")) if before_targets.get(name) else ""
    def val_after(name):  return (prefix + (after_targets.get(name, {}).get("value", "") or "")) if after_targets.get(name) else ""

    campaign_before = before_targets.get("__attentive_utm_param_campaign", {}).get("value") or _extract_utm_campaign(job.get("affiliate_link",""))
    campaign_after  = after_targets.get("__attentive_utm_param_campaign", {}).get("value") or _extract_utm_campaign(driver.current_url or "")

    wide = {
        "Plugin": job.get("extension_name", ""),
        "Browser": job.get("browser","Chromium"),
        "Browser Version": browser_ver,
        "Website": domain,
        "Affiliate Link": job.get("affiliate_link", ""),
        "Privacy Level": privacy_name,
        **{f"{ck} (Before)": val_before(ck) for ck in TARGET_ORDER},
        **{f"{ck} (After)" : val_after(ck)  for ck in TARGET_ORDER},
        "Campaign (Before)": campaign_before,
        "Campaign Date (Before)": ts_before,
        "Campaign (After)": campaign_after,
        "Campaign Date (After)": ts_after,
    }

    def key(c): return (c["name"], c["domain"], c["path"])
    bmap = {key(c): c for c in before_cookies}
    amap = {key(c): c for c in after_cookies}

    changed_names = set()
    for k in amap.keys() - bmap.keys(): changed_names.add(amap[k]["name"])
    for k in bmap.keys() - amap.keys(): changed_names.add(bmap[k]["name"])
    for k in amap.keys() & bmap.keys():
        if amap[k]["value_hash"] != bmap[k]["value_hash"]:
            changed_names.add(amap[k]["name"])

    for name in sorted(changed_names):
        if name in TARGET_SET: continue
        hdr_b = f"{_sanitize_header(name)} (Before)"
        hdr_a = f"{_sanitize_header(name)} (After)"
        bvals = [c["value"] for c in before_cookies if c["name"] == name]
        avals = [c["value"] for c in after_cookies  if c["name"] == name]
        wide[hdr_b] = (prefix + bvals[0]) if bvals else ""
        wide[hdr_a] = (prefix + avals[0]) if avals else ""

    added = [amap[k] for k in amap.keys() - bmap.keys()]
    changed = []
    for k in amap.keys() & bmap.keys():
        if amap[k]["value_hash"] != bmap[k]["value_hash"]:
            changed.append({"before": bmap[k], "after": amap[k]})

    ts = ts_after
    clean_row = {
        "Timestamp": ts,
        "Test ID": job.get("job_id", ""),
        "Browser": job.get("browser","Chromium"),
        "Browser Version": browser_ver,
        "Extension": job.get("extension_name", ""),
        "Extension Version": job.get("extension_version", ""),
        "Merchant": domain,
        "Affiliate Link": job.get("affiliate_link", ""),
        "Privacy Level": privacy_name,
        "Popup Shown?": popup_answer,
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
    for ck in TARGET_ORDER:
        b = next((c for c in before_cookies if c["name"] == ck), None)
        a = next((c for c in after_cookies  if c["name"] == ck), None)
        b_hash = b and b.get("value_hash")
        a_hash = a and a.get("value_hash")
        change = "UNCHANGED"
        if b and not a: change = "REMOVED"
        elif a and not b: change = "ADDED"
        elif b and a and b_hash != a_hash: change = "CHANGED"
        if change != "UNCHANGED":
            diag_rows.append({
                "Test ID": clean_row["Test ID"],
                "Browser": clean_row["Browser"],
                "Browser Version": clean_row["Browser Version"],
                "Extension": clean_row["Extension"],
                "Extension Version": clean_row["Extension Version"],
                "Merchant": domain,
                "Affiliate Link": job.get("affiliate_link", ""),
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
            "Affiliate Link": job.get("affiliate_link", ""),
            "Cookie Name": "(new_tab)",
            "Change": tab.get("title",""),
            "Before Hash": "",
            "After Hash": tab.get("url",""),
            "Observed At": ts
        })
    append_diagnostics(out_workbook, diag_rows)
    print("✔ Wrote: Clean_Data + Diagnostics + Cookie Field Comparison (Chromium, temp profile, privacy).")
