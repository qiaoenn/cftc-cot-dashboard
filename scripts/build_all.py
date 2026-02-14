# scripts/build_all.py
import subprocess
import sys

def run(cmd: str):
    print(f"\n>>> {cmd}")
    r = subprocess.run(cmd, shell=True)
    if r.returncode != 0:
        sys.exit(r.returncode)

if __name__ == "__main__":
    run("PYTHONPATH=. python scripts/run_fetch_test.py")
    run("PYTHONPATH=. python scripts/run_fetch_dis_test.py")
    run("PYTHONPATH=. python scripts/run_transform_test.py")
    run("PYTHONPATH=. python scripts/run_transform_dis_test.py")
    run("PYTHONPATH=. python scripts/build_combined_tidy.py")
    run("PYTHONPATH=. python scripts/run_metrics.py")
    print("\n✅ All build steps completed.")
