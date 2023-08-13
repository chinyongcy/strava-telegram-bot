import mysql.connector.pooling
import time
import logging

class ConnectionPool:
    def __init__(self, host, user, password, database, port=3306, pool_name="Telegram",pool_size=5):
        self.pool_name = pool_name
        self.pool_size = pool_size
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.pool = None
        

    def create_pool(self):
        dbconfig = {
            "pool_name": self.pool_name,
            "pool_size": self.pool_size,
            "host": self.host,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "port": self.port
        }
        self.pool = mysql.connector.pooling.MySQLConnectionPool(**dbconfig)

    def close(self, conn, cursor):
        cursor.close()
        conn.close()

    def fetch(self, query, params=None, all=False, dictionary=True):
        result = None
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=dictionary)
                if params is None:
                    cursor.execute(query)
                else:
                    if isinstance(params, tuple):
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query, (params, ))
                if all:
                    result = cursor.fetchall()
                else:
                    result = cursor.fetchone()
        except Exception as e:
            logging.error(f"Error while executing: {query}")
            logging.error(f"Error: {e}")
        return result
    
    def write(self, query, params=None, last_row_id=False, many=False):
        affected_rows = 0
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                if many:
                    cursor.executemany(query, params)
                    conn.commit()
                else:
                    if params is None:
                        cursor.execute(query)
                    else:
                        if isinstance(params, tuple):
                            cursor.execute(query, params)
                        else:
                            cursor.execute(query, (params, ))
                    conn.commit()
                cursor_last_id = cursor.lastrowid
                affected_rows = cursor.rowcount
            except Exception as e:
                import traceback
                print(traceback.format_exc())
                try:
                    conn.rollback()
                except Exception as rollback_error:
                    logging.error(f"Rollback failed: {rollback_error}")
                logging.error(f"Error while executing write operation - {query}: {e}")
        if last_row_id:
            return affected_rows, cursor_last_id
        return affected_rows
    
    def get_connection(self, max_retries=3, retry_interval=10):
        if self.pool is None:
            self.create_pool()
            logging.debug(f"Created pool {self.pool_name} with size {self.pool_size} ({self.host}/{self.database}).")
        retries = 0
        while retries < max_retries:
            try:
                connection = self.pool.get_connection()
                if connection.is_connected():
                    return connection
                else:
                    connection.close()
            except Exception as e:
                logging.error(f"Error while establishing connection to {self.host} on {self.database} with {self.user}: {e}")
                retries += 1
                logging.debug(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
        logging.error("Failed to establish a database connection after retries")
        raise Exception(f"Failed to establish a database connection to {self.host} on {self.database} with {self.user}")

            



