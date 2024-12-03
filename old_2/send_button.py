import telepot   
from telepot.namedtuple import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton,InlineKeyboardMarkup
from os import getenv
from dotenv import load_dotenv
# import tracemalloc
# tracemalloc.start()
load_dotenv()

TG_BOT_TOKEN = getenv('TG_BOT_TOKEN')
TG_ADMIN_ID = getenv('TG_ADMIN_ID')
msg = "13/09/11, 3.50KM, 05:41/KM,     "


bot = telepot.Bot(TG_BOT_TOKEN)

InKM = InlineKeyboardMarkup(\
    inline_keyboard=[\
        [InlineKeyboardButton(text=\
          msg, callback_data="hello")]])

bot.sendMessage(TG_ADMIN_ID, text="test", reply_markup=InKM)