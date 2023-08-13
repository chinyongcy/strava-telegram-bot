import time
import threading
import asyncio
from strava import Strava, Athlete
from reply import Reply, InlineKB
import logging






def process_task(message, pool):
    logging.debug(f"Processing from {threading.current_thread().name}")
    print('here')
    tele_id = message.from_user.id
    athlete = Athlete(tele_id=tele_id, pool=pool)
    strava_id = athlete.get_strava_id()
    if not strava_id:
        return "User not found..."
    
    # reply_text = f"{current_thread_name}: {message.text} from {tele_id}, strava_id: {strava_id}"
    return Reply.auth_strava

