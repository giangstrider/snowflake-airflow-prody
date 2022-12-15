import pandas as pd

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import pd_writer
from general.config import get_logger

from airflow.exceptions import AirflowException

AIRFLOW_SNOWFLAKE_CONN_ID = 'SNOWFLAKE'
logger = get_logger(__name__)


class SnowflakeDatabase:
    def __init__(self,
            snowflake_conn_id=AIRFLOW_SNOWFLAKE_CONN_ID,
            database=None,
            schema=None,
            session_parameters=None
        ) -> None:
        """Initilisation of class variable which used across this and sub-class of it
            Parameters
            ----------
            snowflake_conn_id : str
                Snowflake connection ID in Airflow
            Database & Schema: str
                If your DAG only work with single database and schema at a time, you could init it here and don't need to fully_qualified_table all the time
                Otherwise, use fully_qualified_table at all time
        """
        self.snowflake_conn_id = snowflake_conn_id
        self.snowflake_hook = None
        self.engine = None
        self.connection = None

        self.database = database
        self.schema = schema
        self.session_parameters = session_parameters

        if not self.database and self.schema:
            raise AirflowException("Schema was provided but database was not provided")


    def get_hook(self, custom_database: str = None, custom_schema: str = None) -> SnowflakeHook:
        """
        Get SnowflakeHook (Airflow built-in) which has pre-config the connection to Platform.
        This is the base foundation for every Platform's connection.
        """
        if not custom_database and custom_schema:
            raise AirflowException("Schema was provided but database was not provided")
        if custom_database and not custom_schema:
            return SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id, database=custom_database, session_parameters=self.session_parameters)
        if custom_database and custom_schema:
            return SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id, database=custom_database, schema=custom_schema, session_parameters=self.session_parameters)

        if not self.snowflake_hook:
            if self.database and not self.schema:
                self.snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id, database=self.database, session_parameters=self.session_parameters)
            elif self.database and self.schema:
                self.snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id, database=self.database, schema=self.schema, session_parameters=self.session_parameters)

        return self.snowflake_hook


    def get_engine(self, snowflake_hook: SnowflakeHook = None):
        """
        Get SQL Alchemy engine usually used for pure Python library to work with Snowflake
        """
        if not self.engine:
            if not snowflake_hook:
                self.engine = self.get_hook().get_sqlalchemy_engine()
            else:
                self.engine = snowflake_hook.get_sqlalchemy_engine()
        return self.engine
    

    def get_connection(self):
        """
        Get Snowflake Connection that help to execute raw SQL only
        """
        if not self.connection:
            connection = self.get_engine().connect()
        return connection


    def execute_query(self, query: str):
        """
        Execute raw SQL query
        """
        result = self.get_hook().run(query)
        logger.info(f"Query {query} result: {result}")


    def get_type_of_table_name(self, table_name: str) -> str:
        """
        Receive table name and return True if table name included database and schema prefix
        """
        split_name = table_name.split(".")
        if len(split_name) == 3:
            return "fully_qualified"
        elif len(split_name) == 2:
            return "half_qualified"
        else:
            return "not_qualified"


    def check_provided_config_versus_table_name(self, table_name: str, database=None, schema=None, required_type:str = None):
        """
        Given table name and configuration, check whether it is enough information to submit to Snowflake or not
        """
        table_name_type = self.get_type_of_table_name(table_name)
        if required_type and table_name_type != required_type:
            raise AirflowException(f"Table name {table_name} is {table_name_type} and not {required_type} to use this function")

        if table_name_type == "fully_qualified":
            return
        
        if not database:
            database = self.database
        if not schema:
            schema = self.schema

        if table_name_type == "half_qualified" and database:
            return

        if table_name_type == "not_qualified" and database and schema:
            return
        
        raise AirflowException(f"Table name {table_name} is not qualified since database/schema was not provided")


    def get_data(self, query: str) -> list:
        """
        Retrieve data from Platform into pure Python data structure
        :param query: Require fully qualified table if you're referring to another database/schema zone
        """
        try:
            result = self.get_connection().execute(query)
            logger.info(f"Result of {query} : {str(result)}")
            return result
        finally:
            self.get_connection().close()
            self.get_engine().dispose()


    def get_data_dataframe(self, query: str, chunksize=1000) -> pd.DataFrame:
        """
        Retrieve data from Platform into Pandas Dataframe
        """
        rows = []
        try:
            for chunk in pd.read_sql_query(query, self.get_engine(), chunksize=chunksize):
                rows.append(chunk)
            
            return pd.concat(rows, axis=0)
        finally:
            self.get_engine().dispose()


    def insert_data_from_dataframe(self, df: pd.DataFrame, table_name: str, mode='append', delete_id=None, update_schema=False):
        """
        Insert data to Platform from Pandas Dataframe
        :param table_name: if fully qualified table name is provided, it will be used instead settings from object's intialisation.
        :param mode: append|truncate|replace
        """

        if df.empty:
            logger.warning(f"DF is empty so function insert_data_from_dataframe inserting data to {table_name} return None")
            return None

        self.check_provided_config_versus_table_name(table_name)

        if delete_id:
            id_text = "', '".join(df[delete_id].astype(str).drop_duplicates().values)
            delete_query = f"""

                delete from {table_name}
                where {delete_id} in ('{id_text}')
            """
            self.execute_query(delete_query)

        if update_schema:
            self.update_schema(df.columns.to_list(), table_name)

        if mode == 'truncate':
            self.truncate_data(table_name)
            mode = 'append'
        
        split_name = table_name.split(".")
        if self.get_type_of_table_name(table_name) == "fully_qualified":
            database, schema, table = table_name.split(".")
            engine = self.get_engine(snowflake_hook=self.get_hook(custom_database=database, custom_schema=schema))
        elif self.get_type_of_table_name(table_name) == "half_qualified":
            database = self.database
            schema = split_name[0]
            table = split_name[1]
            engine = self.get_engine(snowflake_hook=self.get_hook(custom_database=database, custom_schema=schema))
        else:
            database = self.database
            schema = self.schema
            table = table_name
            engine = self.get_engine()

        df.columns = df.columns.str.upper()

        result = df.to_sql(table.lower(), con=engine, index=False, if_exists=mode, method=pd_writer)
        logger.info("Result of insert_data_from_dataframe: {}", str(result))


    def truncate_data(self, table_name: str):
        self.check_provided_config_versus_table_name(table_name)
        self.execute_query(f"truncate table if exists {table_name}")


    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        self.check_provided_config_versus_table_name(table_name)
        df = self.get_data_dataframe(f"desc table {table_name}")
        df['name'] = df['name'].str.lower()
        df['type'] = df['type'].str.lower()
        return df

    def update_schema(self, updating_schema: list, table_name: str):
        current_schema = self.get_table_schema(table_name)['name'].tolist()
        
        new_cols = [f"{col} TEXT" for col in updating_schema if not col.lower() in current_schema]

        if new_cols:
            update_ddl = f"ALTER TABLE {table_name} ADD {','.join(new_cols)}"
            self.execute_query(update_ddl)
            logger.info(f"Columns {','.join(new_cols)} are added to table {table_name}")
            return
        
        logger.info(f"No new columns were added")

    def grant_ownership(self, table_name: str, role_name: str, grant_object="table"):
        self.check_provided_config_versus_table_name(table_name)
        self.execute_query(f"grant ownership on {grant_object} {table_name} to role {role_name}")


    def export_data_to_s3(self, external_stage: str, file_path: str, data_query: str):
        """
        Require external stage to be set up before hand with provided bucket and key path
        Parameters
            ----------
            storage_integration : str
                External stage with configed file format and data format option
            file_path: str
                S3 path for data to export
            data_query: str
                The query return the data to export
        """
        self.execute_query(f"copy into @{external_stage}/{file_path} from ({data_query})")
