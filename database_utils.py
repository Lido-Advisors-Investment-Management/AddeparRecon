
# import required packages
import pandas as pd
import pyodbc
import warnings
from collections import Counter 
import math

def connect_to_database(server, database, driver='SQL Server', username='', password='', debug=False) -> pyodbc.Connection:
    """
    Connect to the passed server and database. 
    *By default, this method will use Windows Authentication ('Trusted_Connection=Yes' in the 
    connection string). This will use the current logged in user's credentials to connect to the
    database. If both a username AND password are passed to this method, Windows Authentication 
    will NOT be used and instead the UN / PW combo will be used.

    Args:
        server (str): name of the server to connect to
        database (str): name of the database to connect to
        driver (str, optional): ODBC Driver to use to connect, which depends on the DBMS being 
                used. Defaults to 'SQL Server'.
        username (str, optional): UN to use for the SQL connection. This will cause Windows Auth
                NOT to be used. Defaults to ''.
        password (str, optional): PW associated with the passed Username to connect to the
                SQL server. Defaults to ''.
        debug (bool, optional): If False, will run normally and return a connection object. 
                If True, will only print the connection string for debugging purposes.
                Defaults to False.
        
    Returns:
        pyodbc.Connection: a pyodbc connection object representing the connection to the provided
                server and databse.
    """
    
    # Construct the connection string
    conn_str = f'Driver={{{driver}}};Server={server};Database={database};'
    if ((username == '') or (password == '')):
        # Default to using windows authentication
        conn_str += 'Trusted_Connection=Yes;'
    else:
        # Only append UN / PW if both are provided to the method
        conn_str += f'UID={username};PWD={password};'
        
    if debug:
        print(conn_str)
        return    
    
    # Attempt to connect to the database server
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as err:
        raise err
    
def list_bulk_insert(conn, table_name, data, column_list, debug=False) -> bool:
    """
    Insert a list of tuples of data records into a database table given an open connection and a
    list of database column names.

    Args:
        conn (pyodbc.Connection): An open connection to a database server
        table (str): The table name the data will be inserted into prefixed with the schema name: 
                'schema_name.table_name'
        data (list): A list containing the data to insert,
        column_list (list): list of database column names. The position of each column name in this
                list must match the position of the correspoding data values in the data record 
                tuples in the passed data list.
                *Database columns must be unique - this will be validated
        debug (bool, optional): If False, will run normally and return a connection object.
                If True, will only print the connection string for debugging purposes.
                Defaults to False.
                            
    Return:
        bool: Returns True if the data lsit was successfully loaded to the database.
    """
    
    # Validate input data and column_list 
    # Catch as many errors as possible before executing the SQL query to save time
    #**********************************************************************************************
    # Ensure data was actually passed
    if len(data) == 0:
        warnings.warn('Empty data list passed. No data inserted into database.', UserWarning)
        return False
    
    # Ensure the column name list is unique
    if len(set(column_list)) != len(column_list):
        raise ValueError('Error: database column names provided in column_list must be unique.')
    
    # Ensure all the records in data are the same size 
    # Create a dictionary of each unique record lenght and the frequency of the length
    record_len_freq = dict(Counter([len(x) for x in data]))
    if len(record_len_freq) > 1:
        raise ValueError('Error: all data record tuples in the data list must be the same length.')
    
    # If all the data records are the same length, ensure they match the column_list length
    if list(record_len_freq.keys())[0] != len(column_list):
        raise ValueError('''Error: the list of database column names must be the same length 
                            as the data record tuples in the passed data list.''')
    
    
    #**********************************************************************************************
    # Construct the SQL string piece by piece using the list of keys from the column_map dictionary
    sql = f'INSERT INTO {table_name} (['                    # INSERT INTO schema.table_name
    sql += '], ['.join(column_list) + ']) VALUES ('         # ([column1], [column2], ...) VALUES 
    sql += ('?, ' * (len(column_list) - 1)) + '?);'         # (?, ?, ...)
    
    if debug:
        print(sql)
        if len(data) > 5:
            print(data[:4])
        else:
            print(data)
        return False
    
    # Execute the SQL statement to insert the data into the table
    try:
        cursor = conn.cursor()
        cursor.executemany(sql, data)
        cursor.commit()
        cursor.close()
        return True
    except pyodbc.Error as err:
        cursor.close()
        print(err)
        return False
    
def dataframe_bulk_insert(conn, table_name, df, column_map, debug=False) -> bool:
    """
    Insert a pandas dataframe into a database table given an open connection and a dictionary
    mapping the dataframe columns to database columns.

    Args:
        conn (pyodbc.Connection): An open connection to a database server
        table (str): The table name the data will be inserted into prefixed with the schema name: 
                'schema_name.table_name'
        df (pd.DataFrame): data to be inserted into the database.
        column_map (dict): a dictionary mapping the dataframe columns to database columns with the
                dictionary keys being the database column names and the values being dataframe
                column names. 
                *Database columns must be unique. Dataframe columns can be duplicated - it is 
                possible that a user may wish to map the same DF column to multiple DB columns.
                The dictionary structure will ensure that database columns are unique.
                **Not all dataframe columns need to be inserted into the database table.
        debug (bool, optional): If False, will run normally and return a connection object.
                If True, will only print the connection string for debugging purposes.
                Defaults to False.
                
    Return:
        bool: Returns True if the data lsit was successfully loaded to the database.
    """
    
    # Validate input dataframe and column_map
    if not(set(column_map.values()).issubset(set(df.columns))):
        raise ValueError('''Error: the provided dataframe does not contain all the columns 
                            specified by the column_map values.''')
    
    # Extraxt the database column names from the column map dictionary keys
    column_list = list(column_map.keys())
    
    # Convert the provided dataframe into a list of tuples
    # While performing the coversion, also take the subset of columns as specified in the
    # column_map dictionary and convert all NaN values to None, which will map to NULL in
    # SQL Server (NaN will cause an error).
    l = [tuple(None if isinstance(i, float) and math.isnan(i) else i for i in r) 
         for r in df[list(column_map.values())].to_numpy()]
    
    # Call the list_bulk_insert method to insert the created data list into the database
    success_flag = list_bulk_insert(conn, table_name, l, column_list, debug)

    return success_flag

def query_to_list(conn, sql) -> list:
    """
    Executes the passed sql query and returns the results formatted as a list of dicts.

    Args:
        conn (pyodbc.Connection): An open connection to a database server
        sql (string): SQL to execute

    Returns:
        dict: the results of the SQL query formatted as a list of dictionaries.
    """
    
    cursor = None
    
    try:
        # Execute the SQL
        cursor = conn.cursor().execute(sql)
        
        # Extract the columns from the cursor
        columns = [column[0] for column in cursor.description]
        
        # Zip each data record with the columns list to create a dictionary
        results = [dict(zip(columns, record)) for record in cursor.fetchall()]
        
        # Clean up
        cursor.close()
    except pyodbc.Error as err:
        if cursor != None:
            cursor.close()
        print(err)
        print('SQL error encountered. No data returned.')
        return
    
    return results
    

# debug 
if __name__ == "__main__":
    pass
