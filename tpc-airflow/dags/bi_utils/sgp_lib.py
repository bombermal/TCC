def string_banco(db_type: str, host: str, port: str, db: str, user: str, passw: str):
        """
        Returns a dictionary with the properties required to connect to a database using JDBC.
        
        Args:
                db_type (str): The type of database to connect to.
                host (str): The hostname or IP address of the database server.
                port (str): The port number to use for the database connection.
                db (str): The name of the database to connect to.
                user (str): The username to use for the database connection.
                passw (str): The password to use for the database connection.
        
        Returns:
                dict: A dictionary containing the properties required to connect to the database using JDBC.
        """
        source_string = f"jdbc:postgresql://{host}:{port}/{db}"
        if db_type == 'oracle':
                source_string = f"jdbc:oracle:thin:@{host}:{port}:{db}"
        source_properties = {
                "url": f"{source_string}",
                "user": f"{user}",
                "password": f"{passw}"
        }

        return source_string, source_properties

def query_leitura(spark, source_properties: dict, query: str):
        """
        Reads data from a JDBC source using the provided source properties and query.
        
        Args:
                source_properties (dict): A dictionary containing the properties required to connect to the JDBC source.
                query (str): The SQL query to execute on the JDBC source.
        
        Returns:
                DataFrame: A DataFrame containing the data read from the JDBC source.
        """
        return spark.read.format("jdbc").options(**source_properties, query=query).load()

def query_escrita(tbl, target_string: str, target_properties: dict, schema: str, table: str, mode: str = "append"):
        """
        Writes data to a PostgreSQL database using the provided DataFrame and target properties.
        
        Args:
                tbl (DataFrame): The DataFrame to write to the PostgreSQL database.
                target_string (str): The connection string for the PostgreSQL database.
                target_properties (dict): A dictionary containing the properties required to connect to the PostgreSQL database.
                schema (str): The name of the schema to write to in the PostgreSQL database.
                table (str): The name of the table to write to in the PostgreSQL database.
                mode (str): The write mode to use when writing to the PostgreSQL database. Defaults to "append".
        """
        tbl.write.jdbc(
                url=target_string,
                table=f"{schema}.{table}",
                mode=mode,
                properties=target_properties)
        
def f_query(cols: list, sch: str, tbl: str, extra: str = ''):
        """
        Constructs a SQL query string based on the given parameters.

        Args:
                cols (list): A list of column names to select.
                sch (str): The schema name.
                tbl (str): The table name.
                extra (str, optional): Any additional SQL query string to append. Defaults to ''.

        Returns:
                str: The constructed SQL query string.
        """
        if extra != '':
                extra = ' '+extra

        return f"SELECT {', '.join(cols)} FROM {sch}.{tbl}{extra}"