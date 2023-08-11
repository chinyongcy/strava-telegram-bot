import time
import threading
import asyncio
from strava import Strava


def process_task(db, loop, msg):
    current_thread_name = threading.current_thread().name
    print(msg.text)
    reply_text = f"{current_thread_name}: {msg.text}"
    return reply_text
    # tele_id = msg.chat_id
    # user = Strava(db, loop, tele_id)
    # strava_id = user.get_strava_id()
    # return strava_id



    # Get Strava ID from strava module