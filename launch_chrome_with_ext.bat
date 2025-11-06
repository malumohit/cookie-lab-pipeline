@echo off
set EXT_DIR=C:\cookie-lab\extensions\chromium\honey old
set PROFILE=C:\cookie-lab\pptr_profile
start "" "C:\Program Files\Google\Chrome\Application\chrome.exe" --user-data-dir="%PROFILE%" --load-extension="%EXT_DIR%" --disable-notifications --no-first-run --no-default-browser-check "https://www.bestbuy.com"
