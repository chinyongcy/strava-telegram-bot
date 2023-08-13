import logging
from dotenv import load_dotenv
import os

load_dotenv()

# Configure logging outside the class

# Import Telegram Stuff
import asyncio
from aiogram import Bot, Dispatcher, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message
from reply import Reply
# Import Strava
from strava import Strava
from telegram_bot import process_task

from queue import Queue
from threading import Thread

from database import AsyncConnectionPool
HOST = os.getenv("DB_HOST")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_DATABASE_NAME")
TG_BOT_TOKEN = os.getenv('TG_BOT_TOKEN')


# Bot token can be obtained via https://t.me/BotFather
TOKEN = os.getenv('TG_TOKEN')

# All handlers should be attached to the Router (or Dispatcher)
router = Router()

NUM_OF_WORKERS = int(os.getenv("NUM_OF_WORKERS"))
CACHE_MAX = 300
CACHE_CLEAR = 300

cached_users = {}
task_queue = Queue()



# Add to queue
@router.message()
async def echo_handler(message: types.Message) -> None:
    message.answer("test")
    task_queue.put(message)  # Enqueue the message for processing

# Reply message
async def reply_message(message, reply):
    print(message)
    await message.answer(reply)

# Worker
def worker():
    while True:
        if not task_queue.empty():
            message = task_queue.get()
            reply = process_task(db, main_event_loop, message)
            asyncio.run_coroutine_threadsafe(reply_message(message, reply), loop=main_event_loop)
            message.answer("test")
            task_queue.task_done()
            print("task done")

# Create Threads
def create_worker(num_workers):
    # thread_list = []
    for _ in range(num_workers):
        p = Process(target=worker)
        p.start()
        




async def main() -> None:
    # Dispatcher is a root router
    dp = Dispatcher()
    # ... and all other routers should be attached to Dispatcher
    dp.include_router(router)
    # Initialize Bot instance with a default parse mode which will be passed to all API calls
    bot = Bot(TG_BOT_TOKEN, parse_mode=ParseMode.HTML)
    # create_threads 
    worker_threads = create_worker_threads(NUM_OF_WORKERS)
    # And the run events dispatching
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    asyncio.run(main())
