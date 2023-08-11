import time
import asyncio
import aiomysql
import os
from dotenv import load_dotenv
load_dotenv()
from database import AsyncConnectionPool

host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_DATABASE_NAME")



class Strava:
    def __init__(self, db, loop, tele_id):
        self.tele_id = tele_id
        self.db = db
        self.loop = loop
        self.strava_id = None
        self.init_timestamp = int(time.time())
    
    async def get_strava_id_async(self):
        query = "SELECT strava_id FROM tele_users WHERE tele_id = %s"
        params = self.tele_id
        strava_id = await self.db.fetch(query, params)
        if strava_id:
            self.strava_id = strava_id[0]
            return self.strava_id
        return None

    def get_strava_id(self):
        strava_id = asyncio.get_event_loop().run_until_complete(self.get_strava_id_async())
        return strava_id





