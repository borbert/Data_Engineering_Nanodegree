import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    This function takes a cursor or connection and drops the tables.
    Using this cursor and connection the function iterates through the drop_tables_queries list and
    executes each one of the drop tables SQL statements.
    
    Args:
        cur : cursor object
        conn: connection object 
        
    Returns:
        Nothing.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    This function takes a cursor or connection and creates all of the tables.
    Using this cursor and connection the function iterates through the create_tables_queries list and
    executes each one of the create table SQL statements.
    
    Args:
        cur : cursor object
        conn: connection object 
        
    Returns:
        Nothing.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    #Reads the config file 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #Get a connection and cursor to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Drop tables, if present, then create them
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    #Close the connection
    conn.close()


if __name__ == "__main__":
    main()