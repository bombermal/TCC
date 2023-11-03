# Imports
import os
import typer
import socket
import datetime 
import dask.dataframe as dd
#from string import Template
# import xml.etree.ElementTree as ET
from typing_extensions import Annotated
from psycopg2 import connect
#from sqlalchemy import create_engine

#import pandas as pd

# Default Variables
OS_TYPE = os.name
if OS_TYPE == 'nt':  # for Windows
    HOSTNAME = socket.gethostname()
    MACHINE_IP = socket.gethostbyname(HOSTNAME)
else:  # for Unix-based systems
    HOSTNAME = os.uname()[1]
    MACHINE_IP = os.popen('hostname -I').read()
     
if ('10.16' in MACHINE_IP) | ('192.168' in MACHINE_IP):
    default_source_path = 'D:/Ivan/OneDrive/Projetos/Códigos ( Profissional )/Material criado/TCC/Load_base_DB/Synthetic_data'
else:
    default_source_path = '/opt/Synthetic_data'

default_conn_params = {
    "drivername": "postgresql",
    "username": "postgres",
    "password": "postgres",
    "host": "192.168.1.30",
    "port": "5432",
    "database": "tpc",
    "schema": "tpc"
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
    Create a connection object to a PostgreSQL database.

    Args:
        params (str): Connection parameters.

    Returns:
        tuple: A tuple containing the connection object and cursor object.
    """
    conn = connect(database=params['database'], user=params['username'],
                        password=params['password'], host=params['host'], port=params['port'])
    cur = conn.cursor()
    cur.execute(f'SET search_path TO {params['schema']}')
    return conn, cur

@app.command(help="Generic load Pandas Dataframe to Database.")
def generic_write(
        cur:  Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        file_name: Annotated[str, "Title of the table."],
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."],
        ext: Annotated[str, "File extension."] = "txt",
        sep: Annotated[str, "Separator used in the file."] = '|',
        cols: Annotated[str, "Column names."] = None,
        truncate: Annotated[str, "Truncate table."] = True
        ):
    """
    This function loads a file into a database table.
    
    Args:
        cur (Cursor): Cursor object.
        conn (Connection): Connection object.
        file_name (str): Title of the table.
        file_path (str): Path to the directory containing the files to be loaded.
        ext (str, optional): File extension. Defaults to "txt".
        sep (str, optional): Separator used in the file. Defaults to '|'.
        cols (str, optional): Column names. Defaults to None.
        truncate (bool, optional): Truncate table. Defaults to True.
    """
    if truncate:
        truncate_table(cur=cur, conn=conn, title=file_name)
    
    path = f"{file_path}/{file_name}.{ext}"
    
    with open(path, 'r') as f:
        # next(f)  # Skip the header row.
        cur.copy_from(f, file_name.lower(), sep=sep, columns=map(str.lower, cols))

    conn.commit()
    
    print(now(title=file_name, start_end='finished'))

@app.command(help="Truncate table.")
def truncate_table(
        cur:  Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        title: Annotated[str, "Title of the table."],
        ):
    """
    Truncates a table in the database.

    Args:
        cur (Cursor): Cursor object.
        conn (Connection): Connection object.
        title (str): Title of the table.
    """
    cur.execute(f"TRUNCATE TABLE {title.lower()}")
    conn.commit()

@app.command(help="Load CashTransaction table.")
def cash_trans(
    cur: Annotated[str, "Cursor object."],
    conn: Annotated[str, "Connection object."],
    file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
):
    """
    Load the CashTransaction table from files located in a directory.

    Args:
        cur (str): Cursor object.
        conn (str): Connection object.
        file_path (str, optional): Path to the directory containing the files to be loaded. Defaults to default_source_path.
    """
    file_name = "CashTransaction"
    cols = ["CT_CA_ID", "CT_DTS", "CT_AMT", "CT_NAME"]
    
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, cols=cols)

@app.command(help="Load DailyMarket table.")
def daily_market(
    cur: Annotated[str, "Cursor object."],
    conn: Annotated[str, "Connection object."],
    file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
):
    """
    Load data from files in the specified directory into the DailyMarket table.
    
    Args:
        cur: Cursor object.
        conn: Connection object.
        file_path: Path to the directory containing the files to be loaded.
    """
    file_name = "DailyMarket"
    cols = ["DM_DATE", "DM_S_SYMB", "DM_CLOSE", "DM_HIGH", "DM_LOW", "DM_VOL"]
    
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, cols=cols)

@app.command(help="Load Date table.")
def tb_date(
    cur: Annotated[str, "Cursor object."],
    conn: Annotated[str, "Connection object."],
    file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
    ):
    file_name = "Date"
    cols = ["SK_DateID", "DateValue", "DateDesc", "Calendar_YearID", "CalendarYearDesc", "CalendarQtrID", "CalendarQtrDesc",
            "CalendarMonthID", "CalendarMonthDesc", "CalendarWeekID", "CalendarWeekDesc", "DayOfWeekNum", "DayOfWeekDesc",
            "FiscalYearID", "FiscalYearDesc", "FiscalQtrID", "FiscalQtrDesc", "HolidayFlag"]
    
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, cols=cols)

@app.command(help="Load Finwire CMP, SEC e FIN table.")
def finwire(
        cur: Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
        ):

    
    # Finwire - Find all files names and sort them
    fin_files = os.listdir(f"{file_path}")
    fin_files = filter(lambda y: "audit" not in y, filter(lambda x: "FINWIRE" in x, fin_files))
    fin_files = sorted(fin_files, reverse=False)
    
    # Read File
    df_temp = dd.read_csv(f"{file_path}/{fin_files[0]}", header=None, names=["temp_col"])
    for file in fin_files[1:]:
        df_temp = dd.concat( [df_temp, dd.read_csv(f"{file_path}/{file}", header=None, names=["temp_col"])])
    df_temp["type"] = df_temp.temp_col.map(lambda x: x[15:18])
    
    cmp_cols = ["PTS", "RecType", "CompanyName", "CIK", "Status", "IndustryID", "SPrating", "FoundingDate", "AddrLine1", "AddrLine2"
            , "PostalCode", "City", "StateProvince", "Country", "CEOname", "Description"]
    sec_cols = ["PTS", "RecType", "Symbol", "IssueType", "Status", "Name", "ExID", "Shout", "FirstTradeDate", "FirstTradeExchg",
                "Dividend", "CoNameOrCIK"]
    fin_cols = ["PTS", "RecType", "Year", "Quarter", "QtrStartDate", "PostingDate", "Revenue", "Earnings", "EPS", "DilutedEPS",
                "Margin", "Inventory", "Assets", "Liabilities", "Shout", "DilutedShOut", "CoNameOrCIK"]

    # Use boolean masks to split the dataframe
    cmp_mask = df_temp['type'] == "CMP"
    sec_mask = df_temp['type'] == "SEC"
    fin_mask = df_temp['type'] == "FIN"

    cols_widths = [15, 3, 60, 10, 4, 2, 4, 8, 80, 80, 12, 25, 20, 24, 46, 150]
    accum = 0
    accum_index = []
    for idx in cols_widths:
        v2 = accum+idx
        accum_index.append((accum, v2))
        accum = v2
        
    df_finwire_cmp = df_temp.loc[cmp_mask, "temp_col"].copy().map(
            lambda row: [ row[x:y].strip() for x, y in accum_index ]).to_bag().to_dataframe(columns=cmp_cols)
        
    # To CSV
    df_finwire_cmp.to_csv(f"{file_path}/Finwire_cmp.csv", index=False, single_file=True, **{"sep":"|", "header":False})

    cols_widths = [15, 3, 15, 6, 4, 70, 6, 13, 8, 8, 12, 60]
    accum = 0
    accum_index = []
    for idx in cols_widths:
        v2 = accum+idx
        accum_index.append((accum, v2))
        accum = v2
        
    df_finwire_sec = df_temp.loc[sec_mask, "temp_col"].copy().map(
            lambda row: [ row[x:y].strip() for x, y in accum_index ]).to_bag().to_dataframe(meta={key: "object" for key in sec_cols})
    df_finwire_sec.to_csv(f"{file_path}/Finwire_sec.csv", index=False, single_file=True, **{"sep":"|", "header":False})

    
    cols_widths = [15, 3, 4, 1, 8, 8, 17, 17, 12, 12, 12, 17, 17, 17, 13, 13, 60]
    accum = 0
    accum_index = []
    for idx in cols_widths:
        v2 = accum+idx
        accum_index.append((accum, v2))
        accum = v2
        
    df_finwire_fin = df_temp.loc[fin_mask, "temp_col"].copy().map(
            lambda row: [ row[x:y].strip() for x, y in accum_index ]).to_bag().to_dataframe(meta={key: "object" for key in fin_cols})

    df_finwire_fin.to_csv(f"{file_path}/Finwire_fin.csv", index=False, single_file=True, **{"sep":"|", "header":False})
    
    # Write do DB
    generic_write(cur=cur, conn=conn, file_name="Finwire_cmp", file_path=file_path, ext="csv", cols=cmp_cols)
    generic_write(cur=cur, conn=conn, file_name="Finwire_sec", file_path=file_path, ext="csv", cols=sec_cols)
    generic_write(cur=cur, conn=conn, file_name="Finwire_fin", file_path=file_path, ext="csv", cols=fin_cols)
    
    # del df_finwire_cmp, df_finwire_sec, df_finwire_fin

@app.command(help="Load HoldingHistory table.")
def holding_hist(
            cur: Annotated[str, "Cursor object."],
            conn: Annotated[str, "Connection object."],
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
    ):

    # Variables
    file_name = "HoldingHistory"
    cols = ["HH_H_T_ID", "HH_T_ID", "HH_BEFORE_QTY", "HH_AFTER_QTY"]
    # Write do DB
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, cols=cols)

@app.command(help="Load HoldingHistory table.")
def hr(
        cur: Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
    ):
    
    # Variables
    file_name = "HR"
    ext = "csv"
    sep = ','
    cols = ["EmployeeID", "ManagerID", "EmployeeFirstName", "EmployeeLastName", "EmployeeMI", "EmployeeJobCode",
        "EmployeeBranch", "EmployeeOffice", "EmployeePhone"]
    # Write do DB
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, ext=ext, sep=sep, cols=cols)

@app.command(help="Load Industry table.")    
def industry(
        cur: Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
    ):
    # Variables
    file_name = "Industry"
    cols = ["IN_ID", "IN_NAME", "IN_SC_ID"] 
    # Write do DB
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, cols=cols)

@app.command(help="Load Prospect table.")    
def prospect(
        cur: Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
    ):
    # Variables
    file_name = "Prospect"
    ext = "csv"
    sep = ','
    cols = ["AgencyID", "LastName", "FirstName", "MiddleInitial", "Gender", "AddressLine1", "AddressLine2", "PostalCode",
        "City", "State", "Country", "Phone", "Income", "NumberCars", "NumberChildren", "MaritalStatus", "Age", "CreditRating",
        "OwnOrRentFlag", "Employer", "NumberCreditCards", "NetWorth"]
    # Write do DB
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, ext=ext, sep=sep, cols=cols)

@app.command(help="Load StatusType table.")    
def statustype(
             cur: Annotated[str, "Cursor object."],
            conn: Annotated[str, "Connection object."],
            file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
        ):
    # Variables
    file_name = "StatusType"
    cols = ["ST_ID", "ST_NAME"]
    # Write do DB
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, cols=cols)
   
@app.command(help="Load TaxRate table.")    
def taxrate(     
        cur: Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
    ):                                                           
    # Variables
    file_name = "TaxRate"         
    cols = ["TX_ID", "TX_NAME", "TX_RATE"]
    # Write do DB
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, cols=cols)

@app.command(help="Load Time table.")    
def time(
        cur: Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
    ):
    # Variables
    file_name = "Time"
    cols = ["SK_TimeID", "TimeValue", "HourID", "HourDesc", "MinuteID", "MinuteDesc",
        "SecondID", "SecondDesc", "MarketHoursFlag", "OfficeHoursFlag"]
    # Write do DB
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, cols=cols)
    
@app.command(help="Load TradeHistory table.")    
def tradehistory( 
        cur: Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
    ):   
    # Variables
    file_name = "TradeHistory"
    cols = ["TH_T_ID", "TH_DTS", "TH_ST_ID"]  
    # Write do DB
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, cols=cols)

@app.command(help="Load Trade table.")    
def trade(
        cur: Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
    ): 
    # Variables
    file_name = "Trade"
    cols = ["T_ID", "T_DTS", "T_ST_ID", "T_TT_ID", "T_IS_CASH", "T_S_SYMB", "T_QTY", "T_BID_PRICE", "T_CA_ID", "T_EXEC_NAME",
        "T_TRADE_PRICE", "T_CHRG", "T_COMM", "T_TAX"]
    # Write do DB
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, cols=cols)
 
@app.command(help="Load TradeType table.")
def tradetype(
        cur: Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
    ):    
    # Variables
    file_name = "TradeType"
    cols = ["TT_ID", "TT_NAME", "TT_IS_SELL", "TT_IS_MRKT"]
    # Write to DB
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, cols=cols)

@app.command(help="Load WatchHistory table.")    
def watchhistory(
        cur: Annotated[str, "Cursor object."],
        conn: Annotated[str, "Connection object."],
        file_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
    ):
    # Variables
    file_name = "WatchHistory"
    cols = ["W_C_ID", "W_S_SYMB", "W_DTS", "W_ACTION"]
    # Write do DB
    generic_write(cur=cur, conn=conn, file_name=file_name, file_path=file_path, cols=cols)
    
@app.command(help="Run all load functions.")
def load_all(
        source_path: Annotated[str, "Path to the directory containing the files to be loaded."] = default_source_path,
        batch_path: Annotated[str, "Batch directory path."] = "Batch1",
        database: Annotated[str, "Database name."] = default_conn_params["database"],
        schema: Annotated[str, "Schema name."] = default_conn_params["schema"],
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
            "database": database,
            "schema": schema
        }
    
    files_path = files_source(source_path, batch_path)
    print(now(start_end="Starting"))
    conn, cur = create_connection(params=con_params)
    
    cash_trans(cur=cur, conn=conn, file_path=files_path)
    daily_market(cur=cur, conn=conn, file_path=files_path)
    finwire(cur=cur, conn=conn, file_path=files_path)
    holding_hist(cur=cur, conn=conn, file_path=files_path)
    hr(cur=cur, conn=conn, file_path=files_path)
    industry(cur=cur, conn=conn, file_path=files_path)
    prospect(cur=cur, conn=conn, file_path=files_path)
    statustype(cur=cur, conn=conn, file_path=files_path)
    taxrate(cur=cur, conn=conn, file_path=files_path)
    time(cur=cur, conn=conn, file_path=files_path)
    tradehistory(cur=cur, conn=conn, file_path=files_path)
    trade(cur=cur, conn=conn, file_path=files_path)
    tradetype(cur=cur, conn=conn, file_path=files_path)
    watchhistory(cur=cur, conn=conn, file_path=files_path)
    
    cur.close()
    conn.close()
    print(now(start_end="finished"))
    
if __name__ == "__main__":
    app()