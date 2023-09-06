import snowflake.connector


class SnowflakeConnector:
    def __init__(self, user, password, account, warehouse, database, schema):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema

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

    def execute_query(self, query):
        if not self.is_connected():
            raise Exception("Not connected to Snowflake")

        with self.connection.cursor(snowflake.connector.DictCursor) as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return result

    def close(self):
        if self.connection:
            self.connection.close()