import mysql.connector as connector
import pandas as pd

from config import *

# Database configuration
db_config = {
    'host': DATABASE_HOST,
    'user': DATABASE_USER,
    'password': DATABASE_PW,
    'database': DATABASE
}

def connect_to_db():
    '''
    Connect to database
    '''
    try:
        conn = connector.connect(**db_config)
        if conn.is_connected():
            print('Connected to MySQL database')
            return conn
    except connector.Error as err:
        print(f'Error: {err}')
        return None
    
def close_connection(conn):
    '''
    Close conn to database
    '''
    if conn.is_connected():
        conn.close()
        print('Connection closed')

def get_all_data_from_table(conn, table_name):
    '''
    Generic function to perform query data
    :param conn: Connection object
    :param query: Generic SQL Select Query
    :param table_name: Table name

    Opens and closes cursor within function using connection parameter
    '''
    query = f'''
        SELECT *
        FROM {table_name}
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = pd.DataFrame(cursor.fetchall())
        # print('Query executed successfully')
        return result
    except connector.Error as err:
        print(f'Error: {err}')
        conn.rollback()
    finally:
        cursor.close()

def insert_into_table(conn, data_df, table_name):
    '''
    Generic function to insert data (pandas dataframe) into a table
    :param conn: Connection object
    :param data_df: Pandas dataframe containing data to be inserted
    :param table_name: Table name

    Opens and closes cursor within function using connection parameter
    '''
    columns = ', '.join(data_df.columns)

    #Generate placeholders for the values in the DataFrame 
    #%s, %s, %s ... for dynamic SQL input
    placeholders = ', '.join(['%s'] * len(data_df.columns))

    query = f'''
        INSERT INTO {table_name} ({columns}) 
        VALUES ({placeholders})
    '''

    #Extract values from the DataFrame and insert into the table
    #Create list of values corresponding to placeholders variables
    values = [tuple(row) for row in data_df.values]

    try:
        cursor = conn.cursor()
        for value in values:
            try:
                cursor.execute(query, value)
            except connector.Error as err:
                print(f'Error inserting data: {err}')
        conn.commit()
        print('Data inserted successfully')
    except connector.Error as err:
        print(f'Error: {err}')
        conn.rollback()
    finally:
        cursor.close()
    return 1

def get_table_columns(conn, table_name):
    '''
    Returns a list of column names
    '''
    query = f'''
        DESCRIBE {table_name};
    '''

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = [row[0] for row in cursor.fetchall()]
        # print(f'Table columns: {result}')
    except connector.Error as err:
        print(f'Error: {err}')
        conn.rollback()
    finally:
        cursor.close()
    return result

def get_all_with_filter(conn: connector, filter_list: list, table_name: str, column_filter: str):
    '''
    Returns all rows under given filter constraints for a single column search else returns an empty dataframe
    Supports query of multiple filters in a list
    
    Parameters:
        - conn: Connector object
        - filter_list: List of values to search for
        - table_name: Name of table
        - column_filter: Name of column to search in
    '''
    filter_query = ', '.join([f"'{filter}'" for filter in filter_list])
    query = f'''
        SELECT *
        FROM {table_name}
        WHERE {column_filter}
        IN ({filter_query});
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = pd.DataFrame(cursor.fetchall())
        if not result.empty:
            col_names = get_table_columns(conn, table_name)
            result.columns = col_names
        else:
            print("No results")
            return result
    except connector.Error as err:
        print(f'Error: {err}')
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    return result

def get_all_with_multiple_filters(conn: connector, column_filters: list, filter_values: tuple, table_name: str):
    '''
    Returns all data from a multi-column filter search
    Only accepts 1 combination of values to filter for

    Parameters:
        - conn: mysql connector object
        - column_filters: Column names involved in filtering database
        - filter_values: 1 set of column values involved in filtering database
        - table_name: Table name
    '''
    where_clause = " AND ".join([f"{column} = %s" for column in column_filters])
    query = f'''
        SELECT *
        FROM {table_name}
        WHERE {where_clause};
    '''
    # print(query)
    try:
        cursor = conn.cursor()
        cursor.execute(query, filter_values)
        result = pd.DataFrame(cursor.fetchall())
        if not result.empty:
            col_names = get_table_columns(conn, table_name)
            result.columns = col_names
        else:
            print("No results")
            return result
    except connector.Error as err:
        print(f'Error: {err}')
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    
    return result

def update_multiple_columns_by_multi_filter(conn: connector, column_filters: list, filter_values: tuple, update_columns: list[str], update_values: list[str], table_name: str):
    '''
    Updates all values identified by multi-column filter (column_filter1 = column_filter_value1...)
    Only accepts 1 combination of values to filter for
    Sets all relevant columns to specific value (update_column1: update_value1...)

    Parameters:
        - conn: mysql connector object
        - column_filters: Column names involved in filtering database
        - filter_values: 1 set of column values involved in filtering database
        - update_columns: Column names involved in update
        - update_values: New values to replace old values
        - table_name: Table name
    '''
    #Sets relevant clauses
    where_clause = " AND ".join([f"{column} = %s" for column in column_filters])
    # print(update_values)
    update_set = zip(update_columns, update_values)
    # print(f"Where: {where_clause}")
    combined_set = [f"{column} = {repr(value)}" for column, value in update_set]
    # print(combined_set)
    set_clause = ", ".join(combined_set)
    # print(f"Set: {set_clause}")

    #Handle edge cases where filter single column filtering is performed instead
    if not isinstance(filter_values, tuple):
        filter_values = (filter_values, )

    query = f'''
        UPDATE {table_name}
        SET {set_clause}
        WHERE {where_clause};
    '''
    # print(query)
    try:
        cursor = conn.cursor()
        cursor.execute(query, filter_values)
        conn.commit()
        print('Dynamic data updated successfully')
    except connector.Error as err:
        print(f'Error: {err}')
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    return 1

def update_rows_in_database(conn: connector, new_df: pd.DataFrame, filter_col: str, table_name: str):
    '''
    Updates multiple rows in a table based on new data in the form of dataframe
    '''
    if new_df.empty:
        print("Empty dataframe passed!")
        return False
    df_columns = new_df.columns
    print(new_df)
    print(filter_col)
    for _, row in new_df.iterrows():
        update_status = update_multiple_columns_by_multi_filter(conn, [filter_col], (row[filter_col]), df_columns, row.values.tolist(), table_name)
        if not update_status:
            return False
    return True

def main():
    '''
    Test functions
    '''
    conn = connect_to_db()
    df = get_all_with_multiple_filters(conn, ['location_id', 'district'], (3, 'Hougang'), 'charging_location_details')
    print(df)
    return 1
    
if __name__ == '__main__':
    main()