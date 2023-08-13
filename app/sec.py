import asyncio
import logging
import sys

from os import getenv
from dotenv import load_dotenv
load_dotenv()

from threading import Thread
from queue import Queue

from aiogram import Bot, Dispatcher, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, LoginUrl
from aiogram.utils.markdown import hbold

from telegram_bot import process_task
from database import ConnectionPool
from reply import Reply

    # keyboard = types.InlineKeyboardMarkup()
    # login_button = types.InlineKeyboardButton(text="Login", callback_data="login")
    # keyboard.add(login_button)

TOKEN = getenv("TG_BOT_TOKEN")
NUM_OF_WORKERS = int(getenv("NUM_OF_WORKERS"))

HOST = getenv("DB_HOST")
USER = getenv("DB_USER")
PASSWORD = getenv("DB_PASSWORD")
DB_NAME = getenv("DB_DATABASE_NAME")
TG_BOT_TOKEN = getenv('TG_BOT_TOKEN')

# All handlers should be attached to the Router (or Dispatcher)
router = Router()
# Queue
task_queue = Queue()
# DB Connection Pool
pool = ConnectionPool(host=HOST, user=USER, password=PASSWORD, database=DB_NAME)
# Create pool
pool.create_pool()


@router.message()
async def echo_handler(message: types.Message) -> None:
    print(message.text)
    task = (message, asyncio.get_event_loop())
    task_queue.put(task) 
    
def worker():
    while True:
        if not task_queue.empty():
            task = task_queue.get()
            message, loop = task[0], task[1]
            reply = process_task(message, pool)
            asyncio.run_coroutine_threadsafe(send_message(message, reply), loop=loop)  
            task_queue.task_done()

async def send_message(message: types.Message, reply: Reply) -> None:
    print("replied")
    await message.answer(**reply)


# Create Threads
def create_worker_threads(num_threads):
    thread_list = []
    for _ in range(num_threads):
        thread = Thread(target=worker)
        thread.start()
        thread_list.append(thread)
    return thread_list


async def main() -> None:
    # Dispatcher is a root router
    dp = Dispatcher()
    # ... and all other routers should be attached to Dispatcher
    dp.include_router(router)

    
    # Initialize Bot instance with a default parse mode which will be passed to all API calls
    bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
    worker_threads = create_worker_threads(NUM_OF_WORKERS)
    
    # And the run events dispatching
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped!")
