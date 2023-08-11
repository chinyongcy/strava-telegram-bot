import aiomysql
import asyncio
import logging
import os
import time
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_DATABASE_NAME")

class AsyncConnectionPool:
    def __init__(self, host, user, password, database, loop, pool_name="Telegram", pool_size=5):
        self.pool_name = pool_name
        self.pool_size = pool_size
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.pool = None
        self.loop = loop
        

    async def __aenter__(self):
        await self.create_pool()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def create_pool(self):
        self.pool = asyncio.get_event_loop().run_until_complete(self.create_pool_async())

    async def create_pool_async(self):
        self.pool = await aiomysql.create_pool(
            host=self.host,
            user=self.user,
            password=self.password,
            db=self.database,
            pool_recycle=3600,
            maxsize=self.pool_size,
        )
        print(self.pool)
        
    
    async def fetch(self, query, params=None, all=False, dictionary=True):
        result = None
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    if all:
                        result = await cursor.fetchall()
                    else:
                        result = await cursor.fetchone()
        except Exception as e:
            logging.error(f"Error while executing: {query}")
            logging.error(f"Error: {e}")
        return result
    
    async def write(self, query, params=None):
        affected_rows = 0
        try:
            async with self.pool.acquire() as conn:
                print(conn.connection_id())
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    await conn.commit()
                    affected_rows = cursor.rowcount
        except Exception as e:
            try:
                await conn.rollback()
            except Exception as rollback_error:
                logging.error(f"Rollback failed: {rollback_error}")
            logging.error(f"Error while executing write operation - {query}: {e}")
        return affected_rows
    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()




    # Rest of the methods remain the same...



# # Example usage
# async def main():
#     try:
#         async with AsyncConnectionPool(host=host, user=user, password=password, database=db_name) as pool:
#             # Fetch single row
#             for i in range(1, 10):
#                 single_row_query = "SELECT * FROM tele_users WHERE tele_id = %s"
#                 single_row_params = (2,)
#                 # records = await pool.write(single_row_query, params=single_row_params)
#                 records = await pool.fetch(single_row_query, params=single_row_params, dictionary=True)
                
#             # query = "INSERT INTO tele_users (tele_id, strava_id) VALUES (%s, %s)"
#             # params = (6, 55555)
#             # records = await pool.write(single_row_query, params=single_row_params)
            

#                 print("Single Row:", records)
#                 time.sleep(10)
#     except Exception as e:
#         print("An error occurred:", e)

# if __name__ == "__main__":
#     asyncio.run(main())








# import mysql.connector.pooling
# import time
# import logging

# class ConnectionPool:
#     def __init__(self, host, user, password, database, pool_name="Telegram",pool_size=5):
#         self.pool_name = pool_name
#         self.pool_size = pool_size
#         self.host = host
#         self.database = database
#         self.user = user
#         self.password = password
#         self.pool = None

#     def create_pool(self):
#         print("Createe pool")
#         dbconfig = {
#             "pool_name": self.pool_name,
#             "pool_size": self.pool_size,
#             "host": self.host,
#             "database": self.database,
#             "user": self.user,
#             "password": self.password
#         }
#         self.pool = mysql.connector.pooling.MySQLConnectionPool(**dbconfig)

#     def close(self, conn, cursor):
#         cursor.close()
#         conn.close()

#     def fetch(self, query, params=None, all=False, dictionary=True):
#         result = None
#         try:
#             with self.get_connection() as conn:
#                 cursor = conn.cursor(dictionary=dictionary)
#                 if params is None:
#                     cursor.execute(query)
#                 else:
#                     if isinstance(params, tuple):
#                         cursor.execute(query, params)
#                     else:
#                         cursor.execute(query, (params, ))
#                 if all:
#                     result = cursor.fetchall()
#                 else:
#                     result = cursor.fetchone()
#         except Exception as e:
#             logging.error(f"Error while executing: {query}")
#             logging.error(f"Error: {e}")
#         return result
    
#     def write(self, query, params=None):
#         affected_rows = 0
#         with self.get_connection() as conn:
#             cursor = conn.cursor()
#             try:
#                 if params is None:
#                     cursor.execute(query)
#                 else:
#                     if isinstance(params, tuple):
#                         cursor.execute(query, params)
#                     else:
#                         cursor.execute(query, (params, ))
#                 conn.commit()
#                 affected_rows = cursor.rowcount
#             except Exception as e:
#                 try:
#                     conn.rollback()
#                 except Exception as rollback_error:
#                     logging.error(f"Rollback failed: {rollback_error}")
#                 logging.error(f"Error while executing write operation - {query}: {e}")

#         return affected_rows
    
#     def get_connection(self, max_retries=3, retry_interval=10):
#         if self.pool is None:
#             self.create_pool()
#             logging.debug(f"Created pool {self.pool_name} with size {self.pool_size} ({self.host}/{self.database}).")
#         retries = 0
#         while retries < max_retries:
#             try:
#                 connection = self.pool.get_connection()
#                 if connection.is_connected():
#                     return connection
#                 else:
#                     connection.close()
#             except Exception as e:
#                 logging.error(f"Error while establishing connection to {self.host} on {self.database} with {self.user}: {e}")
#                 retries += 1
#                 logging.debug(f"Retrying in {retry_interval} seconds...")
#                 time.sleep(retry_interval)
#         logging.error("Failed to establish a database connection after retries")
#         raise Exception(f"Failed to establish a database connection to {self.host} on {self.database} with {self.user}")

            



