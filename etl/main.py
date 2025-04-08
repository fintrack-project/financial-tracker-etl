import importlib
import sys

def run_job(job_name):
    """
    Dynamically import and run a specific ETL job.
    """
    try:
        print(f"Attempting to import: etl.jobs.{job_name}")
        job_module = importlib.import_module(f"etl.jobs.{job_name}")
        print(f"Successfully imported: etl.jobs.{job_name}")
        job_module.run()
    except ModuleNotFoundError:
        print(f"Error: Job '{job_name}' not found.")
    except Exception as e:
        print(f"Error while running job '{job_name}': {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <job_name>")
    else:
        job_name = sys.argv[1]
        run_job(job_name)