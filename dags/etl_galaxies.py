"""
## Galaxies ETL DAG

This example demonstrates an ETL pipeline using Airflow.
The pipeline mocks data extraction for data about galaxies using a modularized
function, filters the data based on the distance from the Milky Way, and loads the
filtered data into a DuckDB database.
"""

from airflow.sdk import Asset, Param, dag, task
from airflow.models.param import Param
import duckdb
import logging
import pandas as pd
from pendulum import datetime, duration
import os
from include.custom_functions.galaxy_functions import get_galaxy_data

# Use the Airflow task logger to log information to the task logs (or use print()).
t_log = logging.getLogger("airflow.task")


# Define variables used in a DAG as environment variables in .env for your whole Airflow instance
# to standardize your DAGs.
_DUCKDB_INSTANCE_NAME = os.getenv("DUCKDB_INSTANCE_NAME", "include/astronomy.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "galaxy_data")
_NUM_GALAXIES_TOTAL = int(os.getenv("NUM_GALAXIES_TOTAL", 10))
_CLOSENESS_THRESHOLD_LY_DEFAULT = os.getenv("CLOSENESS_THRESHOLD_LY_DEFAULT", 500000)
_CLOSENESS_THRESHOLD_LY_PARAMETER_NAME = "closeness_threshold_light_years"

# Define the Asset that represents the galaxy data
galaxy_data = Asset("galaxy_data")

# Instantiate a DAG with the @dag decorator and set DAG parameters


@dag(
    start_date=datetime(2025, 4, 1),
    schedule="0 0 * * *",  # Run daily at midnight (00:00)
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 1,
        "retry_delay": duration(seconds=30),
    },
    tags=["example", "ETL"],
    params={
        _CLOSENESS_THRESHOLD_LY_PARAMETER_NAME: Param(
            _CLOSENESS_THRESHOLD_LY_DEFAULT,
            type="number",
            title="Galaxy Closeness Threshold",
            description="Set how close galaxies need ot be to the milkyway in order to be loaded to DuckDB.",
        )
    },
)
def etl_galaxies():
    @task(retries=3)
    def create_galaxy_table_in_duckdb(
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ) -> None:
        """
        Create a table in DuckDB to store galaxy data.
        This task simulates a setup step in an ETL pipeline.
        Args:
            duckdb_instance_name: The name of the DuckDB instance.
            table_name: The name of the table to be created.
        """

        t_log.info("Creating galaxy table in DuckDB.")

        os.makedirs(os.path.dirname(duckdb_instance_name), exist_ok=True)

        cursor = duckdb.connect(duckdb_instance_name)

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                name STRING PRIMARY KEY,
                distance_from_milkyway INT,
                distance_from_solarsystem INT,
                type_of_galaxy STRING,
                characteristics STRING
            )"""
        )
        cursor.close()

        t_log.info(f"Table {table_name} created in DuckDB.")

    @task
    def extract_galaxy_data(num_galaxies: int = _NUM_GALAXIES_TOTAL) -> pd.DataFrame:
        """
        Retrieve data about galaxies.
        This task simulates an extraction step in an ETL pipeline.
        Args:
            num_galaxies (int): The number of galaxies for which data should be returned.
            Default is 10. Maximum is 20.
        Returns:
            pd.DataFrame: A DataFrame containing data about galaxies.
        """

        galaxy_df = get_galaxy_data(num_galaxies)

        return galaxy_df

    @task(queue="transformation-queue")
    def transform_galaxy_data(galaxy_df: pd.DataFrame, **context):
        """
        Filter the galaxy data based on the distance from the Milky Way.
        This task simulates a transformation step in an ETL pipeline.
        Args:
            closeness_threshold_light_years (int): The threshold for filtering
            galaxies based on distance.
            Default is 500,000 light years.
        Returns:
            pd.DataFrame: A DataFrame containing filtered galaxy data.
        """

        closeness_threshold_light_years = context["params"][
            _CLOSENESS_THRESHOLD_LY_PARAMETER_NAME
        ]

        t_log.info(
            f"Filtering for galaxies closer than {closeness_threshold_light_years} light years."
        )

        filtered_galaxy_df = galaxy_df[
            galaxy_df["distance_from_milkyway"] < closeness_threshold_light_years
        ]

        return filtered_galaxy_df

    @task()
    def load_galaxy_data(
        filtered_galaxy_df: pd.DataFrame,
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ):
        """
        Load the filtered galaxy data into a DuckDB database.
        This task simulates a loading step in an ETL pipeline.
        Args:
            filtered_galaxy_df (pd.DataFrame): The filtered galaxy data to be loaded.
            duckdb_instance_name (str): The name of the DuckDB instance.
            table_name (str): The name of the table to load the data into.
        """

        t_log.info("Loading galaxy data into DuckDB.")
        cursor = duckdb.connect(duckdb_instance_name)
        cursor.sql(
            f"INSERT OR IGNORE INTO {table_name} BY NAME SELECT * FROM filtered_galaxy_df;"
        )
        t_log.info("Galaxy data loaded into DuckDB.")

    @task(outlets=[galaxy_data])
    def print_loaded_galaxies(
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ):
        """
        Get the galaxies stored in the DuckDB database that were filtered
        based on closeness to the Milky Way and print them to the logs.
        Args:
            duck_db_conn_id (str): The connection ID for the duckdb database
            where the table is stored.
        Returns:
            pd.DataFrame: A DataFrame containing the galaxies closer than
            500,000 light years from the Milky Way.
        """
        from tabulate import tabulate

        cursor = duckdb.connect(duckdb_instance_name)
        near_galaxies_df = cursor.sql(f"SELECT * FROM {table_name};").df()
        near_galaxies_df = near_galaxies_df.sort_values(
            by="distance_from_milkyway", ascending=True
        )
        t_log.info(tabulate(near_galaxies_df, headers="keys", tablefmt="pretty"))

    # Call tasks + set dependencies
    create_galaxy_table_in_duckdb_obj = create_galaxy_table_in_duckdb()
    extract_galaxy_data_obj = extract_galaxy_data()
    transform_galaxy_data_obj = transform_galaxy_data(extract_galaxy_data_obj)
    load_galaxy_data_obj = load_galaxy_data(transform_galaxy_data_obj)

    # Set dependencies: table creation and data transformation can run in parallel,
    # but loading requires both the table to exist and the data to be transformed
    create_galaxy_table_in_duckdb_obj >> load_galaxy_data_obj
    transform_galaxy_data_obj >> load_galaxy_data_obj >> print_loaded_galaxies()


etl_galaxies()
