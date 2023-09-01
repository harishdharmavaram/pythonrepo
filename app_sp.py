from snowflake.snowpark import Session
from snowflake.snowpark.types import IntegerType, StringType, StructType
import pandas as pd
from session_dev import connect

snowpark_session = connect()
snowpark_session.add_packages('snowflake-snowpark-python')


def SNOWPARK_HOLDING_DATA_SHOW(snowpark_session, tableName):
    dataframe = snowpark_session.table(tableName)
    return dataframe


snowpark_session.sproc.register(
    func=SNOWPARK_HOLDING_DATA_SHOW
    , return_type=StructType()
    , input_types=[StringType()]
    , is_permanent=True
    , name='SNOWPARK_HOLDING_DATA_SHOW'
    , replace=True
    , stage_location='@STG'
)


#calling
df = snowpark_session.sql('''CALL SNOWPARK_HOLDING_DATA_SHOW('MY_TABLE')''')

print(df.to_pandas())
