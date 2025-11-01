# Cookie-Lab — Extension Cookie Interaction Test Suite

This repository contains the automation pipeline and runners used to test how coupon/affiliate browser extensions interact with affiliate cookies on merchant sites.

## Project layout

- `pipeline.py`                — main orchestrator (supports resume/start flags)
- `runner_firefox_manual.py`   — manual Firefox runner (install `.xpi`, manual navigation to checkout)
- `runner_chromium_manual.py`  — manual Chromium-family runner (Chrome/Edge/Brave/Opera)
- `excel_writer.py`            — atomic Excel writer helpers
- `matrix.yaml`                — configuration: browsers, extensions, links, file paths
- `extensions/`                — (not checked in by default) extension files / folders
- `requirements.txt`           — Python dependencies
- `README.md`                  — project docs (this file)

## Quick start (inside VM)

1. Clone the repo:
   ```powershell
   git clone https://github.com/<your-username>/<repo-name>.git
   cd <repo-name>
Create and activate a virtualenv (PowerShell):

   ```powershell

   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
Place your extension packages (XPI for Firefox, CRX or unpacked folder for Chromium) into extensions/firefox/ and extensions/chromium/ and update matrix.yaml to point to those paths.

Run the pipeline (example — manual Firefox runner):

   ```powershell
   python pipeline.py
To resume from a specific extension or link:

   ```powershell
   python pipeline.py --start-extension "Perkspot" --start-link 1


## Matrix.yaml keys

master_workbook — path to the input workbook (template).

output_workbook — path where agent_output.xlsx will be written.

browsers — list of browsers; for Chromium ones you can specify binary.

extensions — each extension can have firefox_path and/or chromium_path.

links — array of affiliate/product links to test.

Notes & recommendations
Keep agent_output.xlsx closed while the pipeline runs (Windows locks it).

For reproducible runs use a VM snapshot and either:

launch each browser with a fresh --user-data-dir (fast), or

restore the VM snapshot between browser runs (fully isolated).

If you will store extension binaries or large Excel files in the repo, use Git LFS.

Dependencies
See requirements.txt for exact Python packages. Typical packages:

selenium

pandas

pyyaml

openpyxl
