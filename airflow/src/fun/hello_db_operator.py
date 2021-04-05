from airflow.hooks.mysql_hook import MySqlHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class HelloDBOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            name: str,
            mysql_conn_id: str,
            database: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.name = name
        self.mysql_conn_id = mysql_conn_id
        self.database = database

    def execute(self, context):
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id,
                         schema=self.database)
        sql = "select first_name from authors"
        result = hook.get_first(sql)
        message = "Hello {}".format(result['first_name'])
        print(message)
        return message
