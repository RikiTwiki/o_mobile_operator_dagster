from dagster import ConfigurableResource
import psycopg2

class DatabaseResource(ConfigurableResource):
    host: str
    port: int
    database: str
    user: str
    password: str

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
