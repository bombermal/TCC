# Imports
import os
import typer
import pandas as pd
import datetime 
from string import Template
# import xml.etree.ElementTree as ET
from typing_extensions import Annotated
from sqlalchemy import create_engine

# Default Variables 
machine_ip = os.popen('hostname -I').read()
if ('10.16' in machine_ip) | ('192.168' in machine_ip):
    default_source_path = 'D:/Ivan/OneDrive/Projetos/Códigos ( Profissional )/Material criado/TCC/Load_base_DB/Synthetic_data'
else:
    default_source_path = '/opt/Synthetic_data'

default_conn_params = {
    "drivername": "postgresql",
    "username": "postgres",
    "password": "postgres",
    "host": "192.168.1.30",
    "port": "5432",
    "database": "tpc"
}

app = typer.Typer(help="A utility for loading TPC-DI generated files into Postgres.")

@app.command(help="Get current date and time.")
def now(
        title: Annotated[str, "Title of the task."] = "Source",
        start_end: Annotated[str, "Start or End of the task."] = "started"
        ):
    """
    Returns the current date and time as a formatted string.

    Args:
        title (str): Title of the task. Default is "Source".
        start_end (str): Start or End of the task. Default is "started".

    Returns:
        str: A formatted string containing the current date and time.
    """
    return f"{title} load {start_end}...{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

@app.command(help="Create string used to define file locations")
def files_source(
                source_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
                batch_path: Annotated[str, "Batch directory path."] = "Batch1",
                ):
    """
    Returns a string that defines the location of the files to be loaded based on the source path and batch path.
    
    Args:
        source_path (str): Path to the directory containing the files to be loaded.
        batch_path (str): Batch directory path.
    
    Returns:
         string that defines the location of the files to be loaded.
    """
    return source_path + "/" + batch_path

@app.command(help="Create connection object.")
def create_connection(
                    params: Annotated[str, "Connection parameters."] = default_conn_params
                    ):
    """
    Create a connection object using the given connection parameters.
    
    Args:
        params (str): Connection parameters in the format of a string.
        
    Returns:
        engine: A SQLAlchemy engine object representing the database connection.
    """
    conn_string = Template("${drivername}://${username}:${password}@${host}:${port}/${database}").substitute(params)
    engine = create_engine(conn_string, echo=False)
    
    return engine

@app.command(help="Generic load Pandas Dataframe to Database.")
def generic_write(
        title: Annotated[str, "Title of the task."],
        df_aux: Annotated[str, "Dataframe."],
        engine: Annotated[str, "Connection object."],
        schema: Annotated[str, "Schema name."],
        ):
    """
    Write a dataframe to a SQL table.

    Args:
        title (str): Title of the task.
        df_aux (pandas.DataFrame): Dataframe to be written to SQL.
        engine (sqlalchemy.engine.base.Engine): Connection object.
        schema (str): Schema name.

    Returns:
        None
    """
    with engine.begin() as connection:
        df_aux.to_sql(name=title.lower(), con=connection, schema=schema, if_exists="replace", index=False)

    print(now(title=title, start_end='finished'))
    
@app.command(help="Load Cash Transation table.")
def cash_trans(
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
        engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
        schema: Annotated[str, "Schema name."] = "tpc"
        ):
    """
    Load Cash Transaction table from a text file to a database table.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.

    Returns:
        None
    """
    # Variables
    file_name = "CashTransaction"
    ext = "txt"
    sep = '|'
    cols = ["CT_CA_ID", "CT_DTS", "CT_AMT", "CT_NAME"]
    # Read file
    print(now(title=file_name))
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Fix columns
    df_aux.CT_DTS = pd.to_datetime(df_aux.CT_DTS, format="%Y-%m-%d %H:%M:%S")
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)

@app.command(help="Load Daily market table.")
def daily_market(
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
            engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
            schema: Annotated[str, "Schema name."] = "tpc"
            ):
    """
    This function loads the daily market data from a text file into a database table.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.

    Returns:
        None
    """
    # Variables
    file_name = "DailyMarket"
    ext = "txt"
    sep = '|'
    cols = ["DM_DATE", "DM_S_SYMB", "DM_CLOSE", "DM_HIGH", "DM_LOW", "DM_VOL"]
    # Read file
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Fix columns
    df_aux.DM_DATE = pd.to_datetime(df_aux.DM_DATE, format="%Y-%m-%d")
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)
      
@app.command(help="Load Date table.")
def tb_date(
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
            engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
            schema: Annotated[str, "Schema name."] = "tpc"
            ):
    """
    Load data from the Date file into the database.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.

    Returns:
        None
    """
    # Variables
    file_name = "Date"
    ext = "txt"
    sep = '|'
    cols = ["SK_DateID", "DateValue", "DateDesc", "Calendar_YearID", "CalendarYearDesc", "CalendarQtrID", "CalendarQtrDesc",
            "CalendarMonthID", "CalendarMonthDesc", "CalendarWeekID", "CalendarWeekDesc", "DayOfWeekNum", "DayOfWeekDesc",
            "FiscalYearID", "FiscalYearDesc", "FiscalQtrID", "FiscalQtrDesc", "HolidayFlag"]
    # Read File
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Fix columns
    df_aux.DateValue = pd.to_datetime(df_aux.DateValue, format="%Y-%m-%d")
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)
    
@app.command(help="Load Finwire CMP, SEC e FIN table.")
def finwire(
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
        engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
        schema: Annotated[str, "Schema name."] = "tpc"
        ) -> None:
    """
    Loads Finwire CMP, SEC and FIN tables from a given directory into a database.

    Args:
        file_path: Path to the directory containing the files to be loaded.
        engine: Connection object.
        schema: Schema name.
    Returns:
        None
    Raises:
        None
    """
    
    # Finwire - Find all files names and sort them
    fin_files = os.listdir(f"{file_path}")
    fin_files = filter(lambda y: "audit" not in y, filter(lambda x: "FINWIRE" in x, fin_files))
    fin_files = sorted(fin_files, reverse=False)
    
    # Read File
    df_temp = pd.DataFrame(columns=["temp_col"])
    for file in fin_files:
        df_temp = pd.concat( [df_temp, pd.read_csv(f"{file_path}/{file}", header=None, names=["temp_col"])], axis=0)
    df_temp.reset_index(drop=True, inplace=True)
    df_temp["type"] = df_temp["temp_col"].apply(lambda x: x[15:18])
    
    # Columns Names
    cmp_cols = ["PTS", "RecType", "CompanyName", "CIK", "Status", "IndustryID", "SPrating", "FoundingDate", "AddrLine1",
                "AddrLine2", "PostalCode", "City", "StateProvince", "Country", "CEOname", "Description"]
    sec_cols = ["PTS", "RecType", "Symbol", "IssueType", "Status", "Name", "ExID", "Shout", "FirstTradeDate",
                "FirstTradeExchg", "Dividend", "CoNameOrCIK"]
    fin_cols = ["PTS", "RecType", "Year", "Quarter", "QtrStartDate", "PostingDate", "Revenue", "Earnings", "EPS",
                "DilutedEPS", "Margin", "Inventory", "Assets", "Liabilities", "Shout", "DilutedShOut", "CoNameOrCIK"]

    # Use boolean masks to split the dataframe
    cmp_mask = df_temp.type == "CMP"
    sec_mask = df_temp.type == "SEC"
    fin_mask = df_temp.type == "FIN"

    # Store each dataframe in a different variable
    # Convert dataframe to fwf
    cols_widths = [15, 3, 60, 10, 4, 2, 4, 8, 80, 80, 12, 25, 20, 24, 46, 150]
    accum = 0
    accum_index = []
    for idx in cols_widths:
        v2 = accum+idx
        accum_index.append((accum, v2))
        accum = v2
        
    df_finwire_cmp = pd.DataFrame(
        df_temp.loc[cmp_mask, "temp_col"].copy().map(
            lambda row: [ row[x:y].strip() for x, y in accum_index ])
        .tolist(), columns=cmp_cols)

    cols_widths = [15, 3, 15, 6, 4, 70, 6, 13, 8, 8, 12, 60]
    accum = 0
    accum_index = []
    for idx in cols_widths:
        v2 = accum+idx
        accum_index.append((accum, v2))
        accum = v2
        
    df_finwire_sec = pd.DataFrame(
        df_temp.loc[sec_mask, "temp_col"].copy().map(
            lambda row: [ row[x:y].strip() for x, y in accum_index ])
        .tolist(), columns=sec_cols)

    cols_widths = [15, 3, 4, 1, 8, 8, 17, 17, 12, 12, 12, 17, 17, 17, 13, 13, 60]
    accum = 0
    accum_index = []
    for idx in cols_widths:
        v2 = accum+idx
        accum_index.append((accum, v2))
        accum = v2
        
    df_finwire_fin = pd.DataFrame(
        df_temp.loc[fin_mask, "temp_col"].copy().map(
            lambda row: [ row[x:y].strip() for x, y in accum_index ])
        .tolist(), columns=fin_cols)
    
    # Write do DB
    generic_write("Finwire_cmp", df_finwire_cmp, engine, schema)
    generic_write("Finwire_sec", df_finwire_sec, engine, schema)
    generic_write("Finwire_fin", df_finwire_fin, engine, schema)
   
@app.command(help="Load HoldingHistory table.")
def holding_hist(
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
            engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
            schema: Annotated[str, "Schema name."] = "tpc"
            ):
    """
    Load HoldingHistory data from a text file into a database table.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.
    """
    # Variables
    file_name = "HoldingHistory"
    ext = "txt"
    sep = '|'
    cols = ["HH_H_T_ID", "HH_BEFORE_QTY", "HH_AFTER_QTY"]
    # Read file
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)
  
@app.command(help="Load HoldingHistory table.")
def hr(
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
            engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
            schema: Annotated[str, "Schema name."] = "tpc"
            ):
    """
    This function loads the HR data from a CSV file into a database table.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.
    """
    # Variables
    file_name = "HR"
    ext = "csv"
    sep = ','
    cols = ["HH_H_T_ID", "HH_BEFORE_QTY", "HH_AFTER_QTY"]
    # Read file
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)

@app.command(help="Load Industry table.")    
def industry(
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
            engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
            schema: Annotated[str, "Schema name."] = "tpc"
            ):   
    """
    Load Industry table.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.
    """
    # Variables
    file_name = "Industry"
    ext = "txt"
    sep = '|'
    cols = ["IN_ID", "IN_NAME", "IN_SC_ID"]
    # Read file
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)
    
@app.command(help="Load Prospect table.")    
def prospect(
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
            engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
            schema: Annotated[str, "Schema name."] = "tpc"
            ):   
    """
    Load the Prospect table from a CSV file into a database.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.
    """
    # Variables
    file_name = "Prospect"
    ext = "csv"
    sep = ','
    cols = ["AgencyID", "LastName", "FirstName", "MiddleInitial", "Gender", "AddressLine1", "AddressLine2", "PostalCode",
        "City", "State", "Country", "Phone", "Income", "NumberCars", "NumberChildren", "MaritalStatus", "Age", "CreditRating",
        "OwnOrRentFlag", "Employer", "NumberCreditCards", "NetWorth"]
    # Read file
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)

@app.command(help="Load StatusType table.")    
def statustype(
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
            engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
            schema: Annotated[str, "Schema name."] = "tpc"
            ):
    """
    Loads the StatusType data from a text file into a database table.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.
    """
    # Variables
    file_name = "StatusType"
    ext = "txt"
    sep = '|'
    cols = ["ST_ID", "ST_NAME"]
    # Read file
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)
    
@app.command(help="Load TaxRate table.")    
def taxrate(
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
            engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
            schema: Annotated[str, "Schema name."] = "tpc"
            ):   
    """
    Load TaxRate table.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.
    """
    # Variables
    file_name = "TaxRate"
    ext = "txt"
    sep = '|'
    cols = ["TX_ID", "TX_NAME", "TX_RATE"]
    # Read file
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)

@app.command(help="Load Time table.")    
def time(
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
            engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
            schema: Annotated[str, "Schema name."] = "tpc"
            ):   
    """
    Load Time table.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.
    """
    # Variables
    file_name = "Time"
    ext = "txt"
    sep = '|'
    cols = ["SK_TimeID", "TimeValue", "HourID", "HourDesc", "MinuteID", "MinuteDesc",
        "SecondID", "SecondDesc", "MarketHoursFlag", "OfficeHoursFlag"]
    # Read file
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)
        
@app.command(help="Load TradeHistory table.")    
def tradehistory(
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
            engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
            schema: Annotated[str, "Schema name."] = "tpc"
            ):   
    """
    Load TradeHistory table from a text file into a database table.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.
    """
    # Variables
    file_name = "TradeHistory"
    ext = "txt"
    sep = '|'
    cols = ["TH_T_ID", "TH_DTS", "TH_ST_ID"]
    # Read file
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)
    
@app.command(help="Load Trade table.")    
def trade(
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
            engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
            schema: Annotated[str, "Schema name."] = "tpc"
            ):   
    """
    Load Trade table.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.
    """
    # Variables
    file_name = "Trade"
    ext = "txt"
    sep = '|'
    cols = ["T_ID", "T_DTS", "T_ST_ID", "T_TT_ID", "T_IS_CASH", "T_S_SYMB", "T_QTY", "T_BID_PRICE", "T_CA_ID", "T_EXEC_NAME",
        "T_TRADE_PRICE", "T_CHRG", "T_COMM", "T_TAX"]
    # Read file
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)

@app.command(help="Load TradeType table.")
def tradetype(
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
        engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
        schema: Annotated[str, "Schema name."] = "tpc"
        ):
    """Loads TradeType table from a text file to a database.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.
    """
    # Variables
    file_name = "TradeType"
    ext = "txt"
    sep = '|'
    cols = ["TT_ID", "TT_NAME", "TT_IS_SELL", "TT_IS_MRKT"]

    # Read file
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}", sep=sep, header=None, names=cols)

    # Write to DB
    generic_write(file_name, df_aux, engine, schema)

@app.command(help="Load WatchHistory table.")    
def watchhistory(
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path + "/Batch1",
            engine: Annotated[str, "Connection object."] = create_connection(params=default_conn_params),
            schema: Annotated[str, "Schema name."] = "tpc"
            ):   
    """
    Load WatchHistory table.

    Args:
        file_path (str): Path to the directory containing the files to be loaded.
        engine (str): Connection object.
        schema (str): Schema name.
    """
    # Variables
    file_name = "WatchHistory"
    ext = "txt"
    sep = '|'
    cols = ["W_C_ID", "W_S_SYMB", "W_DTS", "W_ACTION"]
    # Read file
    df_aux = pd.read_csv(f"{file_path}/{file_name}.{ext}",sep=sep, header=None, names=cols)
    # Write do DB
    generic_write(file_name, df_aux, engine, schema)
              
@app.command(help="Run all load functions.")
def load_all(
        source_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
        batch_path: Annotated[str, "Batch directory path."] = "Batch1",
        database: Annotated[str, "Database name."] = default_conn_params["database"],
        schema: Annotated[str, "Schema name."] = "tpc",
        drivername: Annotated[str, "Driver name."] = default_conn_params["drivername"],
        username: Annotated[str, "Username."] = default_conn_params["username"],
        password: Annotated[str, "Password."] = default_conn_params["password"],
        host: Annotated[str, "Host."] = default_conn_params["host"],
        port: Annotated[str, "Port."] = default_conn_params["port"]
        ):
    """
    Runs all load functions.
    
    Args:
        source_path (str): Path to the directory containing the files to be loaded.
        batch_path (str): Batch directory path.
        database (str): Database name.
        schema (str): Schema name.
        drivername (str): Driver name.
        username (str): Username.
        password (str): Password.
        host (str): Host.
        port (str): Port.
    """
    con_params = {
            "drivername": drivername,
            "username": username,
            "password": password,
            "host": host,
            "port": port,
            "database": database
        }
    
    files_path = files_source(source_path, batch_path)
    engine = create_connection(params=con_params)
    
    cash_trans(file_path=files_path, engine=engine, schema=schema)
    daily_market(file_path=files_path, engine=engine, schema=schema)
    tb_date(file_path=files_path, engine=engine, schema=schema)
    finwire(file_path=files_path, engine=engine, schema=schema)
    holding_hist(file_path=files_path, engine=engine, schema=schema)
    hr(file_path=files_path, engine=engine, schema=schema)
    industry(file_path=files_path, engine=engine, schema=schema)
    prospect(file_path=files_path, engine=engine, schema=schema)
    statustype(file_path=files_path, engine=engine, schema=schema)
    taxrate(file_path=files_path, engine=engine, schema=schema)
    time(file_path=files_path, engine=engine, schema=schema)
    tradehistory(file_path=files_path, engine=engine, schema=schema)
    trade(file_path=files_path, engine=engine, schema=schema)
    tradetype(file_path=files_path, engine=engine, schema=schema)
    watchhistory(file_path=files_path, engine=engine, schema=schema)
    
    print(now(start_end="finished"))
    
if __name__ == "__main__":
    app()