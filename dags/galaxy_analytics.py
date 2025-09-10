import logging
import os


t_log = logging.getLogger("airflow.task")


_DUCKDB_INSTANCE_NAME = os.getenv("DUCKDB_INSTANCE_NAME", "include/astronomy.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "galaxy_data")


# TODO: Fill in the asset function here!
# @asset()
# def galaxy_data_asset():
#     pass
