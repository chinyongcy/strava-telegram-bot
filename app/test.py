from flask import Flask, jsonify
import threading
import time
from aiogram import Bot
from os import getenv
from dotenv import load_dotenv
import asyncio
from aiogram import Bot
from strava import Athlete, Strava
from build import Build
from database import ConnectionPool
import logging
import os

# import tracemalloc
# tracemalloc.start()
load_dotenv()

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

TG_BOT_TOKEN = getenv('TG_BOT_TOKEN')
TG_ADMIN_ID = getenv('TG_ADMIN_ID')
HOST = getenv("DB_HOST")
USER = getenv("DB_USER")
PASSWORD = getenv("DB_PASSWORD")
DB_NAME = getenv("DB_DATABASE_NAME")
app = Flask(__name__)



async def main():

    pool = ConnectionPool(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DB_NAME,
        port=3306,
    )
    await pool.create_pool()

    athlete = Athlete(tele_id=167708865, pool=pool)
    # test = await Build.stats_table(athlete)
    test = await Build.gpx(athlete, 868386143)

    print(test)

if __name__ == '__main__':
    asyncio.run(main())



