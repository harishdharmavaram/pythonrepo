import unittest
import snowflake.connector
from snowflake_connector import SnowflakeConnector
import json

class SnowflakeConnection(unittest.TestCase):
    # Set up the Snowflake connection here, return the connection object
    with open("secret_dev.json") as secrets_file:
        secrets = json.load(secrets_file)

    user = secrets["snowflake"]["snowflake_user"]
    password = secrets["snowflake"]["snowflake_password"]
    warehouse = secrets["snowflake"]["snowflake_warehouse"]
    database = secrets["snowflake"]["snowflake_database"]
    schema = secrets["snowflake"]["snowflake_schema"]
    account = secrets["snowflake"]["snowflake_account"]

    connection = SnowflakeConnector(user, password, account, warehouse, database, schema)
    connection.connect()

    def connect(self):
        self.connection = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )

    def is_connected(self):
        return self.connection is not None
    # Teardown: Close the connection after testing
    # connection.close()


class TestSnowflakeConnection(unittest.TestCase):

    def test_snowflake_connection(self):
        connection = SnowflakeConnection()
        self.assertTrue(connection.is_connected())

