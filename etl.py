from sql_queries import copy_table_queries, insert_table_queries
from db.DBHandler import DBHandler


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    db_handler = DBHandler()
    cur, conn = db_handler.get_db_cursor_connection()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
