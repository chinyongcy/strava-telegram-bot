from flask import Flask, request, abort, redirect


from os import getenv
from dotenv import load_dotenv
import sys

import hmac
import hashlib

import logging


sys.path.append('../app')
from strava import Athlete
# from ..app.strava import Strava, Athlete
# import threading
import queue

from telegram import async_send_error, async_send_message, service_started
from database import ConnectionPool

load_dotenv()

HOST = getenv("DB_HOST")
USER = getenv("DB_USER")
PASSWORD = getenv("DB_PASSWORD")
DB_NAME = getenv("DB_DATABASE_NAME")


SV_VERIFY_TOKEN = getenv("SV_VERIFY_TOKEN")
SV_CLIENT_ID = getenv('SV_CLIENT_ID')             
SV_CLIENT_SECRET = getenv('SV_CLIENT_SECRET')  
REDIRECT_TO_STRAVA = "https://www.strava.com/oauth/authorize?client_id={}&redirect_uri={}/new_user?tele_info={},{},{}&response_type=code&scope=activity:read_all,profile:read_all"

TG_BOT_TOKEN = getenv('TG_BOT_TOKEN')
TG_ADMIN_ID = getenv('TG_ADMIN_ID')

NUM_OF_WORKERS = int(getenv("NUM_OF_WORKERS"))

APP_DOMAIN = getenv('APP_DOMAIN')

app = Flask(__name__)
# q = queue.Queue()

pool = ConnectionPool(
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DB_NAME,
    port=3306,
    pool_size=2
)
pool.create_pool()
# This route will demonstrate an asynchronous function
@app.route('/auth', methods=['GET'])
async def auth_telegram():
    args = request.args
    sort_args = "\n".\
                join([f'{k}={args[k]}' for k in sorted(args) if k != 'hash']).\
                encode('utf-8').\
                decode('unicode-escape').\
                encode('ISO-8859-1')
    # Check Hash Existence
    if 'hash' not in args:
        logging.info(f"{request.path}: No hash found - {','.join(f'{k}: {args[k]}'for k in args)}")
        abort(400, "hash not found")
    hash_generated = hmac.new(
        hashlib.sha256(
            TG_BOT_TOKEN.encode('utf-8')
        ).digest(),
        sort_args,
        hashlib.sha256
    ).hexdigest()
    # Check Data Validity
    if hash_generated != args['hash']:
        logging.info(f"{request.path}: Invalid hash found - {','.join(f'{k}: {args[k]}'for k in args)}")
        abort(400, "invalid hash")
    strava_url = REDIRECT_TO_STRAVA.format(
                                            SV_CLIENT_ID,
                                            APP_DOMAIN,
                                            args['id'], 
                                            args['first_name'], 
                                            args['username']
                                        )
    return redirect(strava_url, 308)

@app.route('/new_event', methods=['GET'])
async def strava_new_event():
    args = request.args
    verify_token, hub_challenge = args.get("hub.verify_token"), args.get("hub.challenge")
    try:
        response = "hub challenge failed"
        if verify_token == SV_VERIFY_TOKEN:
            response = {
                "hub.challenge": hub_challenge
            }
        else:
            logging.info(f"{request.path}: Failed - {','.join(f'{k}: {args[k]}'for k in args)}")
    except:
        await async_send_error(f"{request.path} - Error in processing")
    return response


# @app.route('/new_event', methods=['POST'])
# def new_webhook_event():
#     try:
#         response = "processing"
#         incoming = request.json
#         if incoming is not None:
#             try:
#                 create_queue(incoming)
#                 response = {}
#             except Exception as e:
#                 send_error(f"Strava Webhook: Error in queueing {e}")
#                 send_error(f"Read More:  {incoming}")
#                 abort(500, "Error in processing.")
#         else:
#             logging.info(f"Strava Webhook: No JSON Received: {request.headers}")
#             abort(400)
#     except:
#         send_error(f"Strava Webhook(Post) Error in Processing: {incoming}, {request.headers}")
#         abort(401)
#     return response

@app.route('/new_user')
async def new_user_handler():
    try:
        args = request.args
        tele_info, auth_code = args['tele_info'], args['code']
        tele_id, first_name, username = tele_info.split(',')
        athlete = Athlete(tele_id, pool, username=username, first_name=first_name)
        signup_summary = athlete.create_user(auth_code)
        await async_send_message(TG_ADMIN_ID, signup_summary)
    except Exception as e:
        
        await async_send_error(f"{request.path} - {e}")
        await async_send_message(f"{request.path} - Error Creating New User @{username} ({first_name})")




    return "processing"

# This route will demonstrate a synchronous function
@app.route('/sync_example')
def sync_example():
    return "Synchronous example completed!"

    # Processing New Activities Thread Pool

# def worker():
#     threadName = threading.current_thread().name
#     logging.info(f"Started Thread {threadName}")
    
#     while True:
#         task = q.get()
#         if task is None:
#             break
#         logging.debug(f"Processing on {threadName}")
#         q.task_done()

# def start_workers(worker_pool=5):
#     for i in range(worker_pool):
#         t = threading.Thread(target=worker, name = i)
#         t.start()

# def create_queue(task_items):
#     q.put(task_items)


# workers = start_workers(worker_pool=NUM_OF_WORKERS)
#service_started("Strava Listener")



# serve(app, host="0.0.0.0", port=5000)
app.run(debug=True, host='0.0.0.0', port=5555)


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')