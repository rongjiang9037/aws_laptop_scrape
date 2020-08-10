import logging

import psycopg2
from sql_queries import * 
import configparser

logging.basicConfig(level=logging.INFO)

def connect_db():
    """
    This function connects database and return cur variable and connection object.

    input: None
    outpout: None
    """
    # get config 
    config = configparser.ConfigParser()
    config.read('config.cfg')

    # connect to database bnhlaptop
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()
    return cur, conn

def drop_tables(cur, conn):
    """
    Drop tables with queries from drop_table_quries list.
    
    input:
    cur - cursor variable
    conn - database connection object
    
    return: None
    """
    try:
        # loop drop_table_queries
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        logging.info("Error occurred dropping related tables.")
        print(e)

    logging.info('Dropped all tables in database bnhcomputer.')
    
    
def create_tables(cur, conn):
    """
    Create tables with quries from create_table_quries list
    
    input:
    cur - cursor variable
    conn - database connection object
    
    return: None
    """
    try:
        # loop over create_table_queries list
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        logging.info("Error occurred creating related tables.")
        print(e)
    
    logging.info('Created new tables in database bnhcomputer.')
    
        
def main():
    """
    -- create database
    
    -- drop tables if exists
    
    -- create tables
    
    -- close database connection
    
    """
    # create table and return connection object
    cur, conn = connect_db()
    
    # drop & create tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # print out current tables
    cur.execute("""select * from pg_catalog.pg_tables
                        where schemaname = 'public'""")
    print(cur.fetchall())

    # close database connection
    conn.close()
    

if __name__ == '__main__':
    main()
    
