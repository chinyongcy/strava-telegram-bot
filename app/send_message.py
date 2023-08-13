from dotenv import load_dotenv
import os
from aiogram import Bot


load_dotenv()

# Retrieve token and admin ID from environment variables
TG_BOT_TOKEN = os.getenv('TG_BOT_TOKEN')
TG_ADMIN_ID = os.getenv('TG_ADMIN_ID')

# Initialize the bot
bot = Bot(token=TG_BOT_TOKEN)

async def send_online_message():
    await bot.send_message(TG_ADMIN_ID, 'Bot is online')
    await bot.close()

if __name__ == '__main__':
    import asyncio
    asyncio.run(send_online_message())
