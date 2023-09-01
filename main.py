from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import IntegerType
import json
from snowflake.snowpark import Session


connection_parameters = {
  "account": "yudedse-av63812",
  "user": "Aklesh",
  "password": "Ran@_123",
  "role": "ACCOUNTADMIN",
  "warehouse": "COMPUTE_WH",
  "database": "DEMO",
  "schema": "PUBLIC",
  "stage": "STG"}

session = Session.builder.configs(connection_parameters).create()


@udf(name='sfpy', input_types=[IntegerType(), IntegerType()], return_type=IntegerType(), stage_location='@STG', is_permanent=True, replace=True)
def sfpy(a: int, b: int) -> int:
    return a-b
 