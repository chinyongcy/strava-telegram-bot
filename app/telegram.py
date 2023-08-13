
import asyncio
import traceback
import logging
from os import getenv
from dotenv import load_dotenv
from aiogram import Bot

TG_BOT_TOKEN = getenv('TG_BOT_TOKEN')
TG_ADMIN_ID = getenv('TG_ADMIN_ID')

load_dotenv()

bot = Bot(token=TG_BOT_TOKEN)

def send_message(tele_id, message):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        async_send_message(tele_id, message)
    )
    loop.close()
# def send_error(input_message=None):
#     traceback_info = traceback.format_exc()
#     message = f"{input_message}: {traceback_info}" if input_message else traceback_info
#     logging.error(message) 
#     send_message(TG_ADMIN_ID, message)

def service_started(service):
    send_message(TG_ADMIN_ID, f"{service} started")


async def async_send_message(tele_id, message):
    await bot.send_message(tele_id, message)
    
async def async_send_admin(message):
    await bot.send_message(TG_ADMIN_ID, message)

async def async_send_error(input_message=None):
    traceback_info = traceback.format_exc()
    message = f"{input_message}: {traceback_info}" if input_message else traceback_info
    logging.error(message) 
    await async_send_admin(message)

