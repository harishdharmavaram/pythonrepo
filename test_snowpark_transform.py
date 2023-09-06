import unittest
import json
import snowflake.connector
from snowflake_connector import SnowflakeConnector
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, avg, asc, desc

with open("secret.json") as secrets_file:
    secrets = json.load(secrets_file)

user = secrets["snowflake"]["snowflake_user"]
password = secrets["snowflake"]["snowflake_password"]
warehouse = secrets["snowflake"]["snowflake_warehouse"]
database = secrets["snowflake"]["snowflake_database"]
schema = secrets["snowflake"]["snowflake_schema"]
account = secrets["snowflake"]["snowflake_account"]

connection_parameters = {
        "account": account,
        "user": user,
        "password": password,
        "warehouse": warehouse,
        "database": database,
        "schema": schema
    }
session = Session.builder.configs(connection_parameters).create()


class TestNoRows(unittest.TestCase):
    def sp_TopNRows_Test(self, tableName, sort_cl, no_of_rows):
        df_tbl_read = session.table(tableName)
        df_filter = df_tbl_read.limit(no_of_rows).sort(desc(col(sort_cl)))
        return df_filter.count()

    def test_no_of_rows(self):
        is_count = False
        no_of_rows = self.sp_TopNRows_Test('FGP_CLOBAL_PRICES', 'PRICE_DATE', 10)
        if no_of_rows == 10:
            is_count = True
        self.assertTrue(is_count)

    def test_for_wrong_no_of_rows(self):
        is_count = False
        no_of_rows = self.sp_TopNRows_Test('FGP_CLOBAL_PRICES', 'PRICE_DATE', 9)
        if no_of_rows == 10:
            is_count = True
        self.assertFalse(is_count)
