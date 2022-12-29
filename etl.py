import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """loads data into staging_events
        and staging_songs from S3
        
        Keyword arguments:
            cur -- cursor which helps to execute the sql query
            conn -- connection to Redshift cluster
        """
    cn = 0
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit() 
        cn+=1
        print("Table{} loaded successfully...!!!".format(cn))


def insert_tables(cur, conn):
    """inserts data into songplays, users, artists
        time and songs tables from staging_events and staging_songs
        
        Keyword arguments:
            cur -- cursor which helps to execute the sql query
            conn -- connection to Redshift cluster
        """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print("staging_tables loaded successfully...!!!")
    insert_tables(cur, conn)
    print('ETL successfully completed')

    conn.close()


if __name__ == "__main__":
    main()