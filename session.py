import json
from snowflake.snowpark import Session


def connect():
    with open("secret.json") as secrets_file:
        secrets = json.load(secrets_file)

    snowflake_username = secrets["snowflake"]["snowflake_user"]
    snowflake_password = secrets["snowflake"]["snowflake_password"]
    snowflake_role = secrets["snowflake"]["snowflake_role"]
    snowflake_warehouse = secrets["snowflake"]["snowflake_warehouse"]
    snowflake_database = secrets["snowflake"]["snowflake_database"]
    snowflake_schema = secrets["snowflake"]["snowflake_schema"]
    snowflake_account = secrets["snowflake"]["snowflake_account"]

    connection_parameters = {
      "account": snowflake_account,
      "user": snowflake_username,
      "password": snowflake_password,
      "role": snowflake_role,
      "warehouse": snowflake_warehouse,
      "database": snowflake_database,
      "schema": snowflake_schema
    }

    session = Session.builder.configs(connection_parameters).create()
    if session:
        print('connected........')
    return session
