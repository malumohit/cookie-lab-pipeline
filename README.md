# 🧪 Cookie Lab Pipeline

A browser automation pipeline for testing how coupon and affiliate browser extensions interact with affiliate cookies on merchant sites.

---

## 📁 Project Structure

| File/Folder | Description |
|-------------|-------------|
| `pipeline.py` | Main orchestrator (supports `--resume` / `--start` flags) |
| `runner_firefox_manual.py` | Manual Firefox runner (install `.xpi`, navigate manually to checkout) |
| `runner_chromium_manual.py` | Manual Chromium-family runner (Chrome/Edge/Brave/Opera) |
| `excel_writer.py` | Atomic Excel writer helpers |
| `matrix.yaml` | Configuration: browsers, extensions, links, file paths |
| `extensions/` | *(Not checked in)* Contains extension files/folders |
| `requirements.txt` | Python dependencies |

---

## 🚀 Quick Start (Inside VM)

### 1. Clone the repository
```powershell
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
```

### 2. Create and activate a virtual environment
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Place extension packages

Put your extension packages into:
```
extensions/firefox/
extensions/chromium/
```

Update `matrix.yaml` to point to the correct paths.

### 4. Run the pipeline

**Example — manual Firefox runner:**
```powershell
python pipeline.py
```

**To resume from a specific extension or link:**
```powershell
python pipeline.py --start-extension "Perkspot" --start-link 1
```

---

## ⚙️ matrix.yaml Configuration

| Key | Description |
|-----|-------------|
| `master_workbook` | Path to the input workbook (template) |
| `output_workbook` | Path where `agent_output.xlsx` will be written |
| `browsers` | List of browsers (Chromium ones can specify binary paths) |
| `extensions` | Each extension can have `firefox_path` and/or `chromium_path` |
| `links` | Array of affiliate/product links to test |

---

## 💡 Notes & Recommendations

- **Keep `agent_output.xlsx` closed** while the pipeline runs (Windows locks the file)
- For reproducible runs, use a VM snapshot and either:
  - Launch each browser with a fresh `--user-data-dir` (faster), or
  - Restore the VM snapshot between browser runs (fully isolated)
- If storing large files (extension binaries, Excel workbooks, etc.), use [Git LFS](https://git-lfs.github.com/)

---

## 🧩 Dependencies

See `requirements.txt` for details. Typical packages include:

- `selenium`
- `pandas`
- `pyyaml`
- `openpyxl`

---

## 📜 License

This project is for research and testing purposes only.  
Use responsibly and comply with all relevant website terms and data policies.
