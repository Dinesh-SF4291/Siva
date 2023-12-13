import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.common import pendulum

from sql_database import sql_database, sql_table

pipeline = dlt.pipeline(
     pipeline_name="sync",  # Use a custom name if desired
     destination="mssql",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
     dataset_name="sync_data"  # Use a custom name if desired
)

source = sql_database()
info = pipeline.run(source, write_disposition="replace")
print(info)