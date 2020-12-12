import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    This function takes a cursor or connection and loads data into the staging tables.
    Using this cursor and connection the function iterates through the copy_tables_queries list and
    executes each of the SQL statements that loads the staging tabels.
    
    Args:
        cur : cursor object
        conn: connection object 
        
    Returns:
        Nothing.
        
    Note: There are additional print statements to check the progress of the function.
    '''
    for query in copy_table_queries:
        print('Query:  {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('--Done')
        print(' ')


def insert_tables(cur, conn):
    '''
    This function takes a cursor or connection and loads data into the start schema tables.
    Using this cursor and connection the function iterates through the insert_tables_queries list and
    executes each of the SQL statements that copies data from the staging tables into the fact and 
    dimension tables.
    
    Args:
        cur : cursor object
        conn: connection object 
        
    Returns:
        Nothing.
        
    Note: There are additional print statements to check the progress of the function.
    '''
    for query in insert_table_queries:
        print('Query:  {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('--Done')
        print(' ')


def main():
    #Get the config varibles
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Connect to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Load the staging tables then copy that data into the other tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    #Close the connection
    conn.close()


if __name__ == "__main__":
    main()