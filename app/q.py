from flask import Flask, jsonify
import threading
import time
from aiogram import Bot
from os import getenv
from dotenv import load_dotenv
import asyncio
from aiogram import Bot
from strava2 import Athlete
from database import ConnectionPool
load_dotenv()

TG_BOT_TOKEN = getenv('TG_BOT_TOKEN')
TG_ADMIN_ID = getenv('TG_ADMIN_ID')
HOST = getenv("DB_HOST")
USER = getenv("DB_USER")
PASSWORD = getenv("DB_PASSWORD")
DB_NAME = getenv("DB_DATABASE_NAME")
app = Flask(__name__)

pool = ConnectionPool(
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DB_NAME,
    port=3306,
    pool_size=2
)
pool.create_pool()
async def send_message_async(loop):
    bot = Bot(token=TG_BOT_TOKEN)
    await bot.send_message(chat_id=TG_ADMIN_ID, text="TEST")
    await bot.session.close()


# Blocking function
def blocking_task(loop):
    # Simulate a CPU-blocking task
    print('Blocking task is running')
    time.sleep(1)
    print('Blocking task is completed')

    # Run the asynchronous function using the provided event loop
    loop.run_until_complete(send_message_async(loop))

@app.route('/process', methods=['POST'])
async def process():
    
    athlete = Athlete(tele_id=167708865, pool=pool)

    profile = await athlete.update_profile()
    # print(profile)
    # Return immediate response
    

    # # Create a new event loop
    # loop = asyncio.new_event_loop()

    # # Start the blocking task in a separate thread
    # thread = threading.Thread(target=blocking_task, args=(loop,))
    # thread.start()
    return "OK"
    # return response

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5555)
