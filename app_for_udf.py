from snowflake.snowpark import Session
from snowflake.snowpark.stored_procedure import StoredProcedureRegistration
from snowflake.snowpark.types import IntegerType

Session.add_packages('snowflake-snowpark-python')


def multiply_by_three(Session):
    return 20*3


Session.sproc.register(
    func=multiply_by_three
    , return_type=IntegerType()
    , is_permanent=True
    , name='SNOWPARK_MULTIPLY_INTEGER_BY_THREE'
    , replace=True
    , stage_location='@STG'
)




