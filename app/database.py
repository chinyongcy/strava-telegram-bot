import aiomysql
import asyncio
import logging

class ConnectionPool:
    
    def __init__(self, host, user, password, database, port=3306, autocommit = True, minsize = 3, maxsize= 10):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.pool = None
        self.lock = asyncio.Lock()
        self.minsize = minsize
        self.maxsize = maxsize
        self.autocommit = autocommit
        
    
    async def create_pool(self):
        self.pool = await aiomysql.create_pool(
            host=self.host,
            user=self.user,
            password=self.password,
            db=self.database,
            port=self.port,
            autocommit=self.autocommit,
            minsize=self.minsize,
            maxsize=self.maxsize
        )
    
    async def get_connection(self, max_retries=3, retry_interval=10):
        if self.pool is None:
            await self.create_pool()
            logging.debug(f"Created pool {self.pool_name} with size {self.pool_size} ({self.host}/{self.database}).")
        retries = 0
        while retries < max_retries:
            try:
                connection = await self.pool.acquire()
                if connection.is_connected():
                    return connection
                else:
                    await connection.ensure_closed()
            except Exception as e:
                logging.error(f"Error while establishing connection to {self.host} on {self.database} with {self.user}: {e}")
                retries += 1
                logging.debug(f"Retrying in {retry_interval} seconds...")
                await asyncio.sleep(retry_interval)
        logging.error("Failed to establish a database connection after retries")
        raise Exception(f"Failed to establish a database connection to {self.host} on {self.database} with {self.user}")

        


    async def close(self, conn, cursor):
        cursor.close()
        conn.close()

    async def fetch(self, query, params=None, all=False, dictionary=True):
        result = None
        try:
            async with self.pool.acquire() as conn:
                if dictionary:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        await cursor.execute(query, params)
                        if all:
                            result = await cursor.fetchall()
                        else:
                            result = await cursor.fetchone()
                else:
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
    
    async def write(self, query, params=None, last_row_id=False, many=False):
        affected_rows = 0
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    if many:
                        await cursor.executemany(query, params)
                    else:
                        await cursor.execute(query, params)
                    await conn.commit()
                    affected_rows = cursor.rowcount
                    cursor_last_id = cursor.lastrowid
        except Exception as e:
            try:
                await conn.rollback()
            except Exception as rollback_error:
                logging.error(f"Rollback failed: {rollback_error}")
            logging.error(f"Error while executing write operation - {query}: {e}")
        if last_row_id:
            return affected_rows, cursor_last_id
        return affected_rows
            



