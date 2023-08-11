

import logging
import datetime
import time
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

main_event_loop = asyncio.get_event_loop()

# DB
db = AsyncConnectionPool(
                host=HOST, 
                user=USER, 
                password=PASSWORD, 
                database=DB_NAME,
                pool_size=NUM_OF_WORKERS,
                loop=main_event_loop
            )
db.create_pool()


async def cache_clearing_handler():
    while True:
        await asyncio.sleep(CACHE_CLEAR)
        print("Clearing cache...")
        current_time = int(time.time())
        keys_to_remove = []
        if cached_users:
            for tele_id, strava_obj in cached_users.items():
                if current_time - strava_obj.init_timestamp > CACHE_MAX:  # 600 seconds = 10 minutes
                    keys_to_remove.append(tele_id)
                    logging.debug(f"{current_time}: Removed {keys_to_remove} (Last Queried: {strava_obj.init_timestamp})")
            for key in keys_to_remove:
                cached_users.pop(key)

@router.message()
async def echo_handler(message: types.Message) -> None:
    """
    Handler will forward receive a message back to the sender

    By default, message handler will handle all message types (like a text, photo, sticker etc.)
    """
    # tele_id = message.from_user.id
    print("Received")
    task_queue.put((message))  # Enqueue the message for processing

    # if tele_id not in cached_users:
    #     user = Strava(tele_id)
    #     if user.strava_id:
    #         cached_users[tele_id] = user
    #         logging.debug(f"Added {tele_id} to cached ({user.init_timestamp})")
    #     else:
    #         # Ask user to register
    #         pass
    # else:
    #     now = int(time.time())
    #     user = cached_users[tele_id]
    #     user.init_timestamp = now
    #     logging.debug(f"Updated {tele_id} ({now})")



    # try:
    #     # Send a copy of the received message
    #     await message.send_copy(chat_id=message.chat.id)


    # except TypeError:
    #     # But not all the types is supported to be copied so need to handle it
    #     await message.answer("Nice try!")

# Threading Stuff

async def reply_message(message, reply):
    print(message)
    await message.answer(reply)
def worker():
    while True:
        if not task_queue.empty():
            message = task_queue.get()
            reply = process_task(db, main_event_loop, message)
            asyncio.run_coroutine_threadsafe(reply_message(message, reply), loop=main_event_loop)  # Use the main event loop
            task_queue.task_done()
            print("task done")
            #asyncio.run_coroutine_threadsafe(send_reply(msg, reply), loop=main_event_loop)  # Use the main event loop




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
    bot = Bot(TG_BOT_TOKEN, parse_mode=ParseMode.HTML)
    # Clear Cache
    asyncio.create_task(cache_clearing_handler())  # Clear cache every 60 seconds
    # create_threads 
    worker_threads = create_worker_threads(NUM_OF_WORKERS)
    # And the run events dispatching
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    asyncio.run(main())



 #### OLD ###

# from telethon import TelegramClient, events
# import os
# from dotenv import load_dotenv
# load_dotenv()
# import asyncio
# import threading
# import time

# from queue import Queue
# from threading import Thread
# import logging
# from telegram_bot import process_task

# from database import AsyncConnectionPool

# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# TG_API_TITLE= os.getenv('TG_API_NAME')
# TG_API_ID= os.getenv('TG_API_ID')
# TG_API_HASH = os.getenv('TG_API_HASH')
# TG_BOT_TOKEN = os.getenv('TG_BOT_TOKEN')

# HOST = os.getenv("DB_HOST")
# USER = os.getenv("DB_USER")
# PASSWORD = os.getenv("DB_PASSWORD")
# DB_NAME = os.getenv("DB_DATABASE_NAME")

# NUM_OF_WORKERS = int(os.getenv("NUM_OF_WORKERS"))

# # TG_API_TITLE=os.getenv()
# # TG_API_ID=os.getenv()"))

# bot = TelegramClient(TG_API_TITLE, TG_API_ID, TG_API_HASH).start(bot_token=TG_BOT_TOKEN)
# task_queue = Queue()
# main_event_loop = asyncio.get_event_loop()

# db = AsyncConnectionPool(
#                 host=HOST, 
#                 user=USER, 
#                 password=PASSWORD, 
#                 database=DB_NAME,
#                 pool_size=NUM_OF_WORKERS,
#                 loop=main_event_loop
#             )
# db.create_pool()

# logging.debug(f"{db.pool}")

# def worker():
#     while True:
#         if not task_queue.empty():
#             msg = task_queue.get()
#             reply = process_task(db, main_event_loop, msg)
#             asyncio.run_coroutine_threadsafe(send_reply(msg, reply), loop=main_event_loop)  # Use the main event loop
#             task_queue.task_done()

# def create_worker_threads(num_threads):
#     thread_list = []
#     for _ in range(num_threads):
#         thread = Thread(target=worker)
#         thread.start()
#         thread_list.append(thread)
#     return thread_list

# async def send_reply(msg, reply):
#     await bot.send_message(msg.chat_id, reply)

# @bot.on(events.NewMessage)
# async def echo(msg):
#     print("Received")
#     task_queue.put((msg))  # Enqueue the message for processing


# def main():
#     """Start the bot."""
#     print("Starting...")
   
#     worker_threads = create_worker_threads(NUM_OF_WORKERS)

#     bot.run_until_disconnected()

# if __name__ == '__main__':
#     main()








