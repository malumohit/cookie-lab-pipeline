# 🧪 Cookie-Lab — Extension Cookie Interaction Test Suite

This repository contains the automation pipeline and runners used to test how coupon and affiliate browser extensions interact with affiliate cookies on merchant sites.

---

## 📁 Project Layout

| File / Folder | Description |
|----------------|--------------|
| `pipeline.py` | Main orchestrator (supports `--resume` / `--start` flags) |
| `runner_firefox_manual.py` | Manual Firefox runner (install `.xpi`, navigate manually to checkout) |
| `runner_chromium_manual.py` | Manual Chromium-family runner (Chrome / Edge / Brave / Opera) |
| `excel_writer.py` | Atomic Excel writer helpers |
| `matrix.yaml` | Configuration: browsers, extensions, links, file paths |
| `extensions/` | (Not checked in) Contains extension files / folders |
| `requirements.txt` | Python dependencies |
| `README.md` | Project documentation (this file) |

---

## 🚀 Quick Start (Inside VM)

### 1. Clone the repository
```powershell
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
2. Create and activate a virtual environment
powershell
Copy code
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
3. Place extension packages
Put your extension packages into:

bash
Copy code
extensions/firefox/
extensions/chromium/
Update matrix.yaml to point to the correct paths.

4. Run the pipeline
Example — manual Firefox runner:

powershell
Copy code
python pipeline.py
To resume from a specific extension or link:

powershell
Copy code
python pipeline.py --start-extension "Perkspot" --start-link 1
⚙️ matrix.yaml Keys
Key	Description
master_workbook	Path to the input workbook (template)
output_workbook	Path where agent_output.xlsx will be written
browsers	List of browsers (Chromium ones can specify binary paths)
extensions	Each extension can have firefox_path and/or chromium_path
links	Array of affiliate / product links to test

💡 Notes & Recommendations
Keep agent_output.xlsx closed while the pipeline runs (Windows locks the file).

For reproducible runs, use a VM snapshot and either:

Launch each browser with a fresh --user-data-dir (faster), or

Restore the VM snapshot between browser runs (fully isolated).

If storing large files (extension binaries, Excel workbooks, etc.), use Git LFS.

🧩 Dependencies
See requirements.txt for details.
Typical packages include:

nginx
Copy code
selenium
pandas
pyyaml
openpyxl
🧭 License
This project is for research and testing purposes only.
Use responsibly and comply with all relevant website terms and data policies.

yaml
Copy code

---

Would you like me to include **badges** (e.g., Python version, license, build status) or a **project architecture diagram** section at the top? It can make your README look even more professional on GitHub.






