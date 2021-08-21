from sql_queries import create_table_queries, drop_table_queries
from db.DBHandler import DBHandler


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    db_handler = DBHandler()
    cur, conn = db_handler.get_db_cursor_connection()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
