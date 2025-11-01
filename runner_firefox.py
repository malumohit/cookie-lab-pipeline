# runner_firefox.py (adds wide output per your format)
import time, hashlib
from urllib.parse import urlparse, unquote
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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
        "value": c.get("value"),               # RAW value
        "value_hash": _h(c.get("value")),
        "domain": c.get("domain"),
        "path": c.get("path"),
        "expiry": c.get("expiry"),
        "httpOnly": c.get("httpOnly"),
        "secure": c.get("secure"),
        "sameSite": c.get("sameSite"),
    }

def _find_click(driver, selectors, timeout=12, pause=2.0):
    for kind, sel in selectors:
        try:
            if kind == "xpath":
                el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((By.XPATH, sel)))
            else:
                el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            el.click()
            time.sleep(pause)
            return True
        except Exception:
            continue
    return False

def _kill_overlays(driver):
    try:
        driver.switch_to.active_element
        driver.execute_script("document.dispatchEvent(new KeyboardEvent('keydown', {'key':'Escape'}));")
    except Exception:
        pass
    close_x = [
        ("css", "button[aria-label='Close'],button[aria-label='close'],button[title*='Close']"),
        ("xpath", "//button[contains(@class,'close') or contains(@class,'modal__close')]"),
        ("xpath", "//div[contains(@class,'modal')]//button[contains(., '×') or contains(., 'Close')]"),
    ]
    _find_click(driver, close_x, timeout=2, pause=0.2)

def _snapshot_target_values(cookies):
    """Return dict: name -> {'value','hash'} for TARGET cookies only."""
    out = {}
    for c in cookies:
        n = c["name"]
        if n in TARGET_SET:
            out[n] = {"value": c.get("value") or "", "hash": c["value_hash"]}
    return out

def _is_checkout(driver) -> bool:
    """Heuristic: detect if we're on a checkout/payment page."""
    try:
        url = (driver.current_url or "").lower()
        if any(k in url for k in ["checkout", "payment", "placeorder", "place-order", "securecheckout"]):
            return True
    except Exception:
        pass
    try:
        # quick text sniff (not too heavy)
        txt = (driver.title or "").lower()
        if any(k in txt for k in ["checkout", "payment"]):
            return True
    except Exception:
        pass
    # common buttons/labels
    try:
        elems = driver.find_elements(By.XPATH, "//button|//a|//h1|//h2")
        sample = " ".join((e.text or "").lower() for e in elems[:30])
        if any(k in sample for k in ["checkout", "secure checkout", "payment", "place order", "billing", "shipping address"]):
            return True
    except Exception:
        pass
    return False

def _wait_for_checkout_with_prompt(driver, max_idle_sec: int = 120):
    """
    Poll for checkout automatically; also ask the user to confirm.
    Returns True if we should proceed to the extension step, False to skip.
    """
    start = time.time()
    last_msg = 0
    while True:
        # auto-detect
        if _is_checkout(driver):
            print("Detected checkout heuristically.")
            # ask for confirmation anyway (sites differ)
        # throttle console spam
        now = time.time()
        if now - last_msg > 6:
            print("Are you at the CHECKOUT page now?  [y]es / [n]o (keep waiting) / [s]kip coupon")
            last_msg = now
        try:
            ans = input().strip().lower()
        except Exception:
            ans = ""
        if ans in ("y", "yes"):
            return True
        if ans in ("s", "skip"):
            return False
        # no/empty → keep waiting a bit and re-check
        time.sleep(5)
        if (time.time() - start) > max_idle_sec:
            print("Timed out waiting for checkout. Proceeding to coupon step anyway; you can still press [Enter] there.")
            return True


def run_one(job: dict, src_workbook: Path, out_workbook: Path):
    ext_ordinal = job.get("extension_ordinal", 0)  # for the "1.", "2.", "3." prefix
    prefix = f"{ext_ordinal}." if ext_ordinal else ""  # if 0, no prefix

    opts = Options()
    driver = webdriver.Firefox(options=opts)
    try:
        driver.install_addon(job["extension_path"], temporary=True)

        # BEFORE cookies (full)
        cookies_before_full = [_cookie_frame_full(c) for c in driver.get_cookies()]
        before_targets = _snapshot_target_values(cookies_before_full)

        # Navigate
        driver.get(job["affiliate_link"])
        WebDriverWait(driver, 25).until(lambda d: d.execute_script("return document.readyState") == "complete")
        domain = urlparse(driver.current_url).netloc
        browser_ver = driver.capabilities.get("browserVersion", "")

        # A->C->CO flow (human can assist in the foreground)
        _kill_overlays(driver)
        _find_click(driver, [
            ("xpath", "//button[contains(translate(., 'ADCRT', 'adcrt'),'add to cart')]"),
            ("css", "#ProductBuy button"),
            ("css", "button.btn-primary"),
        ], timeout=15)
        _kill_overlays(driver)

        _find_click(driver, [
            ("xpath", "//a[contains(translate(., 'VIEW CART', 'view cart'),'view cart')]"),
            ("css", "a[href*='ShoppingCart'], a[href*='Cart']"),
        ], timeout=12)
        _kill_overlays(driver)

        _find_click(driver, [
            ("xpath", "//a[contains(translate(., 'CHECKOUT', 'checkout'),'checkout')]"),
            ("xpath", "//button[contains(translate(., 'CHECKOUT', 'checkout'),'checkout')]"),
            ("css", "button[title*='Checkout'], a[title*='Checkout']"),
        ], timeout=15)
        _kill_overlays(driver)

        # === Confirm checkout (no more long, silent waits) ===
        proceed_to_coupon = _wait_for_checkout_with_prompt(driver, max_idle_sec=180)
        if not proceed_to_coupon:
            print("Skipping coupon step for this run as requested.")
            new_tabs = []
        else:
            # === Human-assisted extension step (now that you're on checkout) ===
            print("\n=== ACTION ===")
            print("Click your extension's Apply/Activate popup now.")
            print("Press ENTER here after you’ve clicked it.")
            pre_handles = set(driver.window_handles)
            try:
                input()
            except Exception:
                pass
            time.sleep(5)  # allow background tabs to open
            post_handles = set(driver.window_handles)
            new_handles = list(post_handles - pre_handles)
            new_tabs = []
            for h in new_handles:
                try:
                    driver.switch_to.window(h)
                    new_tabs.append({"title": driver.title or "", "url": driver.current_url or ""})
                except Exception:
                    new_tabs.append({"title": "", "url": ""})
            # switch back if possible
            try:
                orig = list(pre_handles)[0]
                driver.switch_to.window(orig)
            except Exception:
                pass

        # AFTER cookies (full)
        cookies_after_full = [_cookie_frame_full(c) for c in driver.get_cookies()]
        after_targets = _snapshot_target_values(cookies_after_full)

        # Build wide comparison row (raw values; plus prefix like "1." if provided)
        def val_before(name):
            v = before_targets.get(name, {}).get("value", "")
            return (prefix + v) if v else v

        def val_after(name):
            v = after_targets.get(name, {}).get("value", "")
            return (prefix + v) if v else v

        # also decoded versions (optional—set include_decoded=True to add)
        include_decoded = False
        wide = {
            "Plugin": job.get("extension_name", ""),
            "Browser": "Firefox",
            "Browser Version": browser_ver,
            "Website": domain,
            "Affiliate Link": job.get("affiliate_link", ""),
        }
        for ck in TARGET_ORDER:
            wide[f"{ck} (Before)"] = val_before(ck)
            wide[f"{ck} (After)"]  = val_after(ck)
            if include_decoded:
                wide[f"{ck} (Before, Decoded)"] = unquote(before_targets.get(ck, {}).get("value", "") or "")
                wide[f"{ck} (After, Decoded)"]  = unquote(after_targets.get(ck, {}).get("value", "") or "")

        # Also keep your existing Clean_Data row + Diagnostics for hashes, tabs
        # Counts & hashes (we still compute for Diagnostics)
        def key(c): return (c["name"], c["domain"], c["path"])
        bmap = {key(c): c for c in cookies_before_full}
        amap = {key(c): c for c in cookies_after_full}
        added = [amap[k] for k in amap.keys() - bmap.keys()]
        changed = []
        for k in amap.keys() & bmap.keys():
            if amap[k]["value_hash"] != bmap[k]["value_hash"]:
                changed.append({"before": bmap[k], "after": amap[k]})

        ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        clean_row = {
            "Timestamp": ts,
            "Test ID": job.get("job_id", ""),
            "Browser": "Firefox",
            "Browser Version": browser_ver,
            "Extension": job.get("extension_name", ""),
            "Extension Version": job.get("extension_version", ""),
            "Merchant": domain,
            "Affiliate Link": job.get("affiliate_link", ""),
            "Coupon Applied?": "",  # unknown (human)
            "New Pages Opened": str(len(new_tabs)),
            "Cookies Added (count)": str(len(added)),
            "Cookies Changed (count)": str(len(changed)),
            "HAR Path": "",
            "Screenshots": "",
            "Status": "SUCCESS",
            "Failure Reason": "",
            "Notes": f"CookieComparisonRow=1; Tabs={len(new_tabs)}"
        }

        append_cookie_comparison(out_workbook, wide)
        append_clean_data_row(src_workbook, out_workbook, clean_row)

        # Diagnostics rows for target cookie hashes + new tabs
        diag_rows = []
        for ck in TARGET_ORDER:
            b = before_targets.get(ck, {})
            a = after_targets.get(ck, {})
            change = "UNCHANGED"
            if b and not a: change = "REMOVED"
            elif a and not b: change = "ADDED"
            elif b and a and b.get("hash") != a.get("hash"): change = "CHANGED"
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
                    "Before Hash": b.get("hash",""),
                    "After Hash": a.get("hash",""),
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

        print("✔ Cookie Field Comparison row appended.")
    finally:
        try: driver.quit()
        except Exception: pass
