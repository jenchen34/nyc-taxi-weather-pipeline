import luigi
import subprocess
import os
from pathlib import Path

# ----------------------------
# Config from environment vars
# ----------------------------
GCP_PROJECT = os.getenv("GCP_PROJECT")
GCP_REGION = os.getenv("GCP_REGION")
GCP_CLUSTER = os.getenv("GCP_CLUSTER")
GCS_SPARK_SCRIPT = os.getenv("GCS_SPARK_SCRIPT")
SNOWFLAKE_SQL = os.getenv("SNOWFLAKE_SQL", "Snowflake/snowflake_load.sql")

FLAG_DIR = Path("flags")
FLAG_DIR.mkdir(exist_ok=True)

class RunSparkJob(luigi.Task):
    def output(self):
        return luigi.LocalTarget(FLAG_DIR / "spark_completed.marker")

    def run(self):
        print("\n" + "="*50)
        print(f"🚀 [STAGE 1] Submitting Spark Job: {GCS_SPARK_SCRIPT}")
        print("="*50)

        cmd = [
            "gcloud", "dataproc", "jobs", "submit", "pyspark",
            GCS_SPARK_SCRIPT,
            "--cluster", GCP_CLUSTER,
            "--region", GCP_REGION,
            "--project", GCP_PROJECT
        ]

        try:
            subprocess.run(cmd, check=True)
            with self.output().open("w") as f:
                f.write(f"Spark Job completed on {GCP_PROJECT}")
            print("\n✅ Spark Task Finished Successfully!")
        except subprocess.CalledProcessError as e:
            print("\n❌ Spark Job Failed. Please check GCP Logs.")
            raise e

class LoadToSnowflake(luigi.Task):
    def requires(self):
        return RunSparkJob()

    def output(self):
        return luigi.LocalTarget(FLAG_DIR / "snowflake_completed.marker")

    def run(self):
        print("\n" + "="*50)
        print(f"❄️ [STAGE 2] Loading Snowflake via {SNOWFLAKE_SQL}")
        print("="*50)

        cmd = f"/usr/local/bin/snowsql -f {SNOWFLAKE_SQL}"

        try:
            subprocess.run(cmd, shell=True, check=True)
            with self.output().open("w") as f:
                f.write(f"Snowflake loaded.")
            print("\n✅ Snowflake Task Finished Successfully!")
        except subprocess.CalledProcessError as e:
            print("\n❌ Snowflake Loading Failed.")
            raise e

if __name__ == "__main__":
    luigi.run(["LoadToSnowflake", "--local-scheduler"])
