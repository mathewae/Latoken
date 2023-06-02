import sqlalchemy
import pandahouse
import pandas
import utils


class DatabaseConnection:
    def __init__(self, db_source: str, db_name: str, username_param: str, password_param: str):
        self._socket_addresses = {
            'clickhouse': {
                'default': '95.217.178.73:8123'
            },
            'postgresql': {
                'postgres': '135.181.61.116:5432',
                'terminal': '116.203.183.219:48934',
                'mmsettings': '116.203.183.219:48934',
            }
        }
        self.connection = self._get_connection(db_source, db_name, username_param, password_param)

    def _get_connection(self, db_source: str, db_name: str, username_param: str, password_param: str):
        socket_address = self._socket_addresses[db_source][db_name]
        username = utils.Params.get_param(
            username_param,
            f'Username for "{db_name}" database ({db_source}) is not provided'
        )
        password = utils.Params.get_param(
            password_param,
            f'Password for "{db_name}" database ({db_source}) is not provided'
        )
        if db_source == 'clickhouse':
            return {
                'host': f'http://{socket_address}',
                'user': username,
                'password': password
            }
        return sqlalchemy.create_engine(f'{db_source}://{username}:{password}@{socket_address}/{db_name}')


def postgres_query_executor_decorator(query_builder: callable):
    def inner(*args):
        engine = DatabaseConnection('postgresql', 'postgres', 'user_postgres', 'pass_postgres').connection
        return pandas.read_sql(query_builder(*args), engine)

    return inner


def clickhouse_query_executor_decorator(query_func: callable):
    def inner(*args):
        config = DatabaseConnection('clickhouse', 'default', 'user_click', 'pass_click').connection
        return pandas.read_table(
            pandahouse.execute(
                query_func(*args) + ' FORMAT TabSeparatedWithNames',
                connection=config,
                stream=True
            )
        )

    return inner
