import yaml
from pathlib import Path
from runner_firefox import run_one

if __name__ == "__main__":
    job = yaml.safe_load(Path("one_job.yaml").read_text())

    # Set your workbook paths (master in, output out)
    src = Path(r"C:\cookie-lab\Mohit Cookie Stuffing File .xlsx")
    out = Path(r"C:\cookie-lab\agent_output.xlsx")

    run_one(job, src, out)
