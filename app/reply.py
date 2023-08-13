
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, LoginUrl

def reply(  
            text, 
            parse_mode = None,
            entities = None,
            disable_web_page_preview = None,
            disable_notification = None, 
            protect_content = None, 
            reply_to_message_id = None,
            allow_sending_without_reply = None,
            reply_markup = None
    ):
    return {
            'text': text,
            'parse_mode': parse_mode,
            'entities': entities,
            'disable_web_page_preview': disable_web_page_preview,
            'disable_notification': disable_notification,
            'protect_content': protect_content,
            'reply_to_message_id': reply_to_message_id,
            'allow_sending_without_reply': allow_sending_without_reply,
            'reply_markup': reply_markup
}



class InlineKB:
    # Auth Strava
    auth_strava = InlineKeyboardMarkup(
                    inline_keyboard = [[InlineKeyboardButton(
                        text = "🔗 Connect with 🟧 Strava!",
                        login_url = LoginUrl(
                                        url = 'https://strava.thatcy.com/auth',
                                        request_write_access = True
                                    )             
                    )]]
                )
class Reply:
    auth_strava = reply(
                    text = "Please sign up first..",
                    reply_markup=InlineKB.auth_strava
    )

