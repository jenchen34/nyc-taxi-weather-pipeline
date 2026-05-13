import os
import shutil
import subprocess
from pathlib import Path

import luigi
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
FLAG_DIR = BASE_DIR / "flags"
FLAG_DIR.mkdir(exist_ok=True)

load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_SPARK_SCRIPT = PROJECT_ROOT / "Spark" / "spark-job.py"
DEFAULT_SNOWFLAKE_SQL = PROJECT_ROOT / "Snowflake" / "snowflake_load.sql"


def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value


def normalize_gcs_bucket_for_snowflake(bucket: str) -> str:
    bucket = bucket.strip().rstrip("/")
    if bucket.startswith("gs://"):
        return bucket[len("gs://"):]
    return bucket


def require_command(command: str) -> str:
    preferred_paths = {
        "gcloud": PROJECT_ROOT / "tools" / "google-cloud-sdk" / "bin" / "gcloud",
        "snowsql": Path("/Applications/SnowSQL.app/Contents/MacOS/snowsql"),
    }

    preferred = preferred_paths.get(command)
    if preferred and preferred.exists():
        return str(preferred)

    path = shutil.which(command)
    if not path:
        raise FileNotFoundError(
            f"Required command not found in PATH: {command}. "
            f"Install it before running the pipeline."
        )
    return path


def resolve_path(env_name: str, default_path: Path) -> Path:
    configured_path = os.getenv(env_name)
    if configured_path:
        return Path(configured_path).expanduser().resolve()
    return default_path.resolve()


def run_checked(cmd: list[str], env: dict[str, str] | None = None) -> None:
    subprocess.run(cmd, check=True, env=env)


class RunSparkJob(luigi.Task):
    """Stage 1: Submit the local Spark job to Dataproc using GCS-hosted input data."""

    def output(self):
        return luigi.LocalTarget(FLAG_DIR / "spark_completed.marker")

    def run(self):
        gcloud = require_command("gcloud")
        gcp_project = require_env("GCP_PROJECT")
        gcp_region = require_env("GCP_REGION")
        gcp_cluster = require_env("GCP_CLUSTER")
        gcs_bucket = require_env("GCS_BUCKET")
        raw_taxi_path = os.getenv("RAW_TAXI_PATH", "NYC-taxi")
        raw_weather_path = os.getenv("RAW_WEATHER_PATH", "Weather.csv")
        fact_output_name = os.getenv("FACT_OUTPUT_NAME", "fact_taxi_demand")
        dim_time_output_name = os.getenv("DIM_TIME_OUTPUT_NAME", "dim_time")
        dim_weather_output_name = os.getenv("DIM_WEATHER_OUTPUT_NAME", "dim_weather")
        dim_location_output_name = os.getenv("DIM_LOCATION_OUTPUT_NAME", "dim_location")

        spark_script = resolve_path("SPARK_SCRIPT", DEFAULT_SPARK_SCRIPT)
        if not spark_script.exists():
            raise FileNotFoundError(f"Spark script not found: {spark_script}")

        print("\n" + "=" * 50)
        print(f"[STAGE 1] Submitting Spark job: {spark_script}")
        print("=" * 50)

        cmd = [
            gcloud,
            "dataproc",
            "jobs",
            "submit",
            "pyspark",
            str(spark_script),
            "--cluster",
            gcp_cluster,
            "--region",
            gcp_region,
            "--project",
            gcp_project,
            "--",
            "--gcs-bucket",
            gcs_bucket,
            "--raw-taxi-path",
            raw_taxi_path,
            "--raw-weather-path",
            raw_weather_path,
            "--fact-output-name",
            fact_output_name,
            "--dim-time-output-name",
            dim_time_output_name,
            "--dim-weather-output-name",
            dim_weather_output_name,
            "--dim-location-output-name",
            dim_location_output_name,
        ]

        try:
            run_checked(cmd)
            with self.output().open("w") as f:
                f.write(f"Spark job completed successfully on project {gcp_project}.\n")
            print("\nSpark task finished successfully.")
        except subprocess.CalledProcessError as e:
            print("\nSpark job failed. Check Dataproc logs in GCP.")
            raise e


class LoadToSnowflake(luigi.Task):
    """Stage 2: Load processed outputs into Snowflake."""

    def requires(self):
        return RunSparkJob()

    def output(self):
        return luigi.LocalTarget(FLAG_DIR / "snowflake_completed.marker")

    def run(self):
        snowsql = require_command("snowsql")
        snowflake_account = require_env("SNOWFLAKE_ACCOUNT")
        snowflake_user = require_env("SNOWFLAKE_USER")
        snowflake_password = require_env("SNOWFLAKE_PASSWORD")
        snowflake_warehouse = require_env("SNOWFLAKE_WAREHOUSE")
        snowflake_database = require_env("SNOWFLAKE_DATABASE")
        snowflake_schema = require_env("SNOWFLAKE_SCHEMA")
        snowflake_role = require_env("SNOWFLAKE_ROLE")
        gcs_bucket = require_env("GCS_BUCKET")

        snowflake_sql = resolve_path("SNOWFLAKE_SQL", DEFAULT_SNOWFLAKE_SQL)

        print("\n" + "=" * 50)
        print(f"[STAGE 2] Loading Snowflake via {snowflake_sql}")
        print("=" * 50)

        if not snowflake_sql.exists():
            raise FileNotFoundError(f"Snowflake SQL file not found: {snowflake_sql}")

        bucket_no_prefix = normalize_gcs_bucket_for_snowflake(gcs_bucket)

        sql_text = snowflake_sql.read_text()
        sql_text = sql_text.replace("__GCS_BUCKET_NO_PREFIX__", bucket_no_prefix)

        rendered_sql_path = FLAG_DIR / "snowflake_load_rendered.sql"
        rendered_sql_path.write_text(sql_text)

        cmd = [
            snowsql,
            "-a",
            snowflake_account,
            "-u",
            snowflake_user,
            "-r",
            snowflake_role,
            "-w",
            snowflake_warehouse,
            "-d",
            snowflake_database,
            "-s",
            snowflake_schema,
            "-f",
            str(rendered_sql_path),
        ]

        try:
            snowsql_env = os.environ.copy()
            snowsql_env["SNOWSQL_PWD"] = snowflake_password
            run_checked(cmd, env=snowsql_env)
            with self.output().open("w") as f:
                f.write("Snowflake load completed successfully.\n")
            print("\nSnowflake task finished successfully.")
        except subprocess.CalledProcessError as e:
            print("\nSnowflake loading failed. Check SnowSQL output.")
            raise e


class FullPipeline(luigi.Task):
    """Final wrapper task for the full pipeline."""

    def requires(self):
        return LoadToSnowflake()

    def output(self):
        return luigi.LocalTarget(FLAG_DIR / "pipeline_completed.marker")

    def run(self):
        with self.output().open("w") as f:
            f.write("Full pipeline completed successfully.\n")
        print("\nFull pipeline completed successfully.")


if __name__ == "__main__":
    luigi.run()
