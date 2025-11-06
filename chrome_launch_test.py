import subprocess, os, time, sys

CHROME = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
PROFILE = r"C:\cookie-lab\pptr_profile"
EXT_DIR = r"C:\cookie-lab\extensions\chromium\Honey 18.2.1"
URL = "https://www.bestbuy.com"

if not os.path.exists(CHROME):
    print("[fatal] Chrome not found:", CHROME); sys.exit(1)
if not os.path.exists(os.path.join(EXT_DIR, "manifest.json")):
    print("[fatal] manifest.json missing in", EXT_DIR); sys.exit(1)

args = [
    CHROME,
    f"--user-data-dir={PROFILE}",
    f"--load-extension={EXT_DIR}",
    "--disable-notifications",
    "--no-first-run",
    "--no-default-browser-check",
    URL
]
print("[launch]", " ".join(args))
# Start detached so Chrome stays open after Python exits
subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
print("Launched Chrome. Check the toolbar for the extension icon.")
