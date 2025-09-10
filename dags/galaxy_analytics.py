"""
## Galaxy Analytics DAG

This DAG analyzes galaxy data after the ETL pipeline has completed.
It runs automatically when the galaxy_data Asset is updated by the ETL DAG.
"""

from airflow.sdk import Asset, dag, task
from pendulum import datetime
import logging
import os

# Use the Airflow task logger to log information to the task logs
t_log = logging.getLogger("airflow.task")

# Define variables used in the DAG
_DUCKDB_INSTANCE_NAME = os.getenv("DUCKDB_INSTANCE_NAME", "include/astronomy.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "galaxy_data")

# Define the same Asset that the ETL DAG produces
galaxy_data = Asset("galaxy_data")


@dag(
    start_date=datetime(2025, 4, 1),
    schedule=[galaxy_data],  # Run when galaxy_data Asset is updated
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 1,
    },
    tags=["example", "analytics"],
)
def galaxy_analytics():
    @task()
    def analyze_galaxies(
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ) -> None:
        """
        Analyze the galaxy data by creating an analytics
        table (in the logs!) with the count of galaxies
        by type.

        In production, you would write this data to a
        persistent table somewhere and perhaps use it
        in an analytics dashboard.
        """
        import duckdb
        from tabulate import tabulate

        t_log.info("Starting galaxy analysis...")

        cursor = duckdb.connect(duckdb_instance_name)

        query = f"""
           SELECT type_of_galaxy, COUNT(*) AS count
           FROM {table_name}
           GROUP BY type_of_galaxy
           ORDER BY count DESC
        """

        galaxy_analysis_df = cursor.sql(query).df()

        t_log.info("Galaxy Analysis Results:")
        t_log.info(tabulate(galaxy_analysis_df, headers="keys", tablefmt="pretty"))

        cursor.close()

        t_log.info("Galaxy analysis completed successfully!")

        return galaxy_analysis_df

    # Call the analysis task
    analyze_galaxies()


galaxy_analytics()
