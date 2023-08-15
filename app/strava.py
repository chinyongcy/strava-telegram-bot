# OS, File Logging
import os
import logging
from dotenv import load_dotenv
# Date and Time
import time
from datetime import datetime, timedelta
# Asyncio Stuff
import aiohttp
import asyncio
import json




load_dotenv()

HOST = os.getenv("DB_HOST")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_DATABASE_NAME")

SV_CLIENT_ID= os.getenv("SV_CLIENT_ID")
SV_CLIENT_SECRET=os.getenv("SV_CLIENT_SECRET")

SV_AUTH_URL = "https://www.strava.com/oauth/token"

API_LIMIT_DAILY = os.getenv("API_LIMIT_DAILY")
API_LIMIT_DAILY = int(API_LIMIT_DAILY) if API_LIMIT_DAILY else 1000

API_LIMIT_BLOCK = os.getenv("API_LIMIT_BLOCK")
API_LIMIT_BLOCK = int(API_LIMIT_BLOCK) if API_LIMIT_BLOCK else 100

TZ_OFFSET = os.getenv("TIMEZONE_OFFSET")
TZ_OFFSET = int(TZ_OFFSET) if TZ_OFFSET else 0

NUM = 10

def to_datetime(date_string):
    return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")


class Athlete:
    def __init__(self, tele_id=None, strava_id=None, pool=None, username=None, first_name=None):
        self.pool = pool
        self.tele_id = tele_id
        self.strava_id = strava_id
        self.username = username
        self.first_name = first_name
        self.last_accessed = int(time.time())

    async def get_strava_id(self):
        if self.strava_id == None:
            query = "SELECT strava_id FROM users WHERE tele_id = %s"
            params = self.tele_id
            strava_id = await self.pool.fetch(query, params, dictionary=False)
            if strava_id:
                self.strava_id = strava_id[0]
        return self.strava_id

    async def get_tele_id(self):
        if self.tele_id == None:
            query = "SELECT tele_id FROM users WHERE strava_id = %s"
            params = self.strava_id
            tele_id = await self.pool.fetch(query, params, dictionary=False)
            if tele_id:
                self.tele_id = tele_id[0]
        return self.tele_id
    # Test Later
    async def create_user(self, auth_code):
        creds = await Strava.authenticate(auth_code)
        logging.debug(creds)
        if not "errors" in creds:
            # Strava ID
            self.strava_id = creds['athlete']['id']
            
            #bot.sendMessage(ADMIN_ID, f"New User Signed Up Strava ID:{athlete.strava_id}, Tele ID:{athlete.tele_id}, Tele Name:{athlete.tele_first_name}")
                       # Auth Details
            self.refresh_token = creds['refresh_token']
            self.access_token = creds['access_token']
            self.access_token_expires = creds['expires_at']
            
            # Create user in DB
            query = """
                    INSERT INTO users (tele_id, strava_id)
                    VALUES (%s, %s) 
                    ON DUPLICATE KEY UPDATE strava_id = %s
                    """
            params = (self.tele_id, self.strava_id, self.strava_id)
            await self.pool.write(query, params)
           
            # Update Token in DB
            query = """
                    INSERT INTO auth (strava_id, refresh_token, access_token, expires_at)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE refresh_token = %s, access_token = %s, expires_at = %s
                    """
            params = (self.strava_id, self.refresh_token, self.access_token, self.access_token_expires,
                      self.refresh_token, self.access_token, self.access_token_expires)   
            await self.pool.write(query, params)
            # Prep User Profile
            query = """
                    INSERT INTO athletes (strava_id) values (%s) 
                    ON DUPLICATE KEY UPDATE strava_id = %s
                    """
            params = self.strava_id, self.strava_id
            await self.pool.write(query, params)            
            signup_summary = f"New User Signed Up Strava ID:{self.strava_id}, Tele ID: {self.tele_id} @{self.username}"

        else:
            raise Exception("Error in getting access token when creating a new user!")
        return signup_summary

    async def get_auth(self, _retry = False, _retry_count = 0, _fetched_token_success = False):
        await self.get_strava_id()
         # Grab Athlete Auth, if it does not exist
        if not hasattr(self, "auth"):
            logging.debug(f"Athlete {self.strava_id} has no auth attributes, getting from DB.")
            self.auth = await self.pool.fetch("""
                                        SELECT refresh_token, access_token, expires_at
                                        from auth WHERE strava_id = %s 
                                        """,
                                        self.strava_id)
            if not self.auth:
                raise Exception(f"Reauthenticate with strava required. {self.strava_id} has no auth result from db.")
        else:
           logging.debug(f"Athlete {self.strava_id} already has auth attributes.")
        #Check for keys before attempting to check if access key has expired
        _current_time = int(datetime.now().strftime('%s'))
        if "refresh_token" in self.auth:
            if self.auth['refresh_token'] is not None:
                if self.auth['expires_at'] == "" or \
                    self.auth['expires_at'] is None or \
                    self.auth['access_token'] == ""  or \
                    self.auth['access_token'] is None or \
                    _current_time >= self.auth['expires_at'] or \
                    _retry == True:         
                    logging.debug(f"Required new token for {self.strava_id}")
                    creds = await Strava.refresh_access_token(refresh_token=self.auth['refresh_token'], pool=self.pool)
                    
                    self.auth = {"refresh_token": creds['refresh_token'], \
                       "access_token": creds['access_token'], "expires_at": creds['expires_at']}
                    query= """
                        INSERT into auth (strava_id, refresh_token, access_token, expires_at)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE refresh_token = %s, access_token = %s, expires_at = %s
                        """
                    params = (self.strava_id, creds['refresh_token'], creds['access_token'], creds['expires_at'], \
                            creds['refresh_token'], creds['access_token'], creds['expires_at'])
                    await self.pool.write(query, params)
                else:
                    logging.debug(f"{self.strava_id} got auth token from object.")
            else:
                raise Exception(f"Reauthenticate with strava required. {self.strava_id} has no refresh_token value.")
        else:
            
            raise Exception(f"Reauthenticate with strava required. {self.strava_id} has no refresh_token key.")
        return self.auth['access_token']
    
    async def update_profile(self, new_user=False):
        #Get Profile
        await self.get_strava_id()
        sv_profile = await Strava.get_profile(self)
        sv_user_name = sv_profile['username']
        sv_first_name = sv_profile['firstname']
        sv_last_name = sv_profile['lastname']
        sv_bio = sv_profile['bio']
        sv_premium = sv_profile['premium']
        sv_summit = sv_profile['summit']
        sv_weight = sv_profile['weight']
        sv_profile_pic = sv_profile['profile']
        sv_profile_pic_medium = sv_profile['profile_medium']
        sv_city = sv_profile['city']
        sv_state = sv_profile['state']
        sv_country = sv_profile['country']
        sv_sex = sv_profile['sex']
        sv_created = datetime.strptime(sv_profile['created_at'], '%Y-%m-%dT%H:%M:%SZ') + timedelta(hours=TZ_OFFSET)
        sv_updated = datetime.strptime(sv_profile['updated_at'], '%Y-%m-%dT%H:%M:%SZ') + timedelta(hours=TZ_OFFSET)
        last_refreshed = datetime.now()

        query = """
                INSERT into athletes
                (strava_id, username, first_name, last_name,
                bio, premium, summit, weight, profile_medium, profile,
                city, state, country, sex, created_at, updated_at, last_refreshed)
                VALUES (%s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                username = %s, first_name = %s, last_name = %s,
                bio = %s, premium = %s, summit = %s, weight = %s, profile_medium = %s, profile = %s, 
                city = %s, state = %s, country = %s, sex = %s, created_at = %s, updated_at = %s, last_refreshed = %s
                """

        params = (self.strava_id, sv_user_name, sv_first_name, sv_last_name, \
                sv_bio, sv_premium, sv_summit, sv_weight, sv_profile_pic_medium, sv_profile_pic, \
                sv_city, sv_state, sv_country, sv_sex, sv_created, sv_updated, last_refreshed, \
                sv_user_name, sv_first_name, sv_last_name, \
                sv_bio, sv_premium, sv_summit, sv_weight, sv_profile_pic_medium, sv_profile_pic, \
                sv_city, sv_state, sv_country, sv_sex, sv_created, sv_updated, last_refreshed)

        if new_user:
            await self.update_heart_rate()
        await self.pool.write(query, params)
        return sv_profile

    async def get_heart_rate(self):
        await self.get_strava_id()
        query = "SELECT z1, z2, z3, z4, last_updated from heartrates WHERE strava_id = %s"
        self.heart_rate = await self.pool.fetch(query, self.strava_id)
        if self.heart_rate is None or not all(key in self.heart_rate for key in ['z1','z2','z3','z4']) \
            or None in self.heart_rate.values() \
            or (datetime.now() - self.heart_rate['last_updated']).days > 30:
            self.heart_rate = await self.update_heart_rate()
        return self.heart_rate

    async def update_heart_rate(self):
        zones = await Strava.get_heart_rate(self)
        self.heart_rate = {}
        
        for i in range(0,4):
            self.heart_rate[f'z{i+1}'] = zones[i]['max']
        
        
        query = """
                INSERT INTO heartrates (strava_id, z1, z2, z3, z4, last_updated) \
                VALUES(%s, %s, %s, %s, %s, %s) AS HR
                ON DUPLICATE KEY UPDATE \
                z1 = HR.z1, z2 = HR.z2, z3 = HR.z3, z4 = HR.z4, last_updated = HR.last_updated
                """
        params = (self.strava_id, 
                self.heart_rate['z1'], self.heart_rate['z2'], 
                self.heart_rate['z3'], self.heart_rate['z4'],
                datetime.now())
        await self.pool.write(query, params)
        return self.heart_rate

    async def get_all_acitivites_id(self):
        await self.get_strava_id()
        query = "SELECT id from activities WHERE strava_id = %s"
        activity_ids = await self.pool.fetch(query, self.strava_id, all=True, dictionary=False)
        if activity_ids:
            return [x[0] for x in activity_ids]
        return activity_ids
    
    async def get_activity(self, activity_id):
        await self.get_strava_id()
        query = "SELECT * FROM activities WHERE id = %s AND strava_id = %s"
        params = (activity_id, self.strava_id)
        activity = await self.pool.fetch(query, params, all=False)
        return activity
    
    async def get_activity_list(self, hr_zone, act_type, page_number, page_size):
        # Fetch Both Strava ID and Heart Rate
        tasks = [self.get_strava_id(), self.get_heart_rate()]
        await asyncio.gather(*tasks)
        # Manage Activity Type
        if act_type == "all":
            act_type_query = ""
        else:
            act_type_query = f"AND type='{act_type}'"

        # Manage Heart Rate
        min_heart_rate, max_heart_rate = 0, 0
        if hr_zone != 0:
            # Set Min HR
            min_heart_rate = self.heart_rate[f'z{hr_zone-1}'] if hr_zone != 1 else 0
            # Set Max HR
            max_heart_rate = self.heart_rate[f'z{hr_zone}'] if hr_zone != 5 else 500
            zone_query = f"AND average_heartrate between {min_heart_rate} AND {max_heart_rate}"
        else:
            zone_query = ''

        query =f"""
                    SELECT start_date_local, type, distance, moving_time,
                    average_speed, average_heartrate, average_cadence,
                    total_elevation_gain FROM activities
                    WHERE 
                    strava_id={self.strava_id}
                    {act_type_query}
                    {zone_query}
                    ORDER BY start_date_local DESC
                    LIMIT {page_size + 1}
                    OFFSET {(page_number - 1) * page_size}
                """
        
        result = await self.pool.fetch(query, all=True)
        result_len = len(result)

        if result_len > page_number:
            next_page = True
            # remove last item
            result =  result[:-1]
        else:
            next_page = False
        
        hr_zone_data = {
            "zone": hr_zone,
            "min": min_heart_rate,
            "max": max_heart_rate
        }

        return result, next_page, hr_zone_data

    async def get_activity_no_poly(self):
        await self.get_strava_id()
        query = """
            SELECT activities.id FROM activities
            LEFT JOIN polylines ON 
            activities.id = polylines.id
            WHERE 
            activities.strava_id = %s AND
            polylines.id is NULL AND
            activities.start_latitude is NOT NULL AND 
            activities.start_longitude is NOT NULL
            """
        activity = await self.pool.fetch(query, self.strava_id, all=True, dictionary=False)
        if activity:
            return [x[0] for x in activity]
        else:
            return None

    async def update_activities(self, update_all=False):
        await self.get_strava_id()
        if update_all:
            cur_act = []
        else:
            cur_act = await self.get_all_acitivites_id()
        update_all = False
        new_activities = []
        skip = False
       
        page, per_page = 1, 200
        # Get Activities
        while skip == False:
            params = {'page': page, 'per_page': per_page}
            r = await Strava.get_activities(self, **params)
            # Break if empty result
            if not r:
                break
            for a in r:
                if a['id'] not in cur_act or update_all == True:
                    new_activities.append(a)
                else:
                    skip = True
                    break
            page += 1
        new_act_count = len(new_activities)
        filtered_acts = []
        if new_act_count:
            # Prep data
            for a in new_activities:
                filtered_acts.append(
                {
                    'id': a['id'], 'name': a['name'], 'strava_id': self.strava_id,

                    'type': a['type'], 
                    'sport_type': a['sport_type'] if 'sport_type' in a else None,
                    'workout_type': a['workout_type'] if 'workout_type' in a else None,

                    'start_date': to_datetime(a['start_date']),  'start_date_local': to_datetime(a['start_date_local']),  'utc_offset': a['utc_offset'],
                    
                    'location_country': a['location_country'] if 'location_country' in a else None, 
                    'location_city': a['location_city'] if 'location_city' in a else None, 
                    'location_state': a['location_state'] if 'location_state' in a else None,

                    'start_latitude': a['start_latlng'][0] if a['start_latlng'] else None,
                    'start_longitude': a['start_latlng'][1] if a['start_latlng'] else None,

                    'end_latitude': a['end_latlng'][0] if a['end_latlng'] else None,
                    'end_longitude': a['end_latlng'][1] if a['end_latlng'] else None,

                    'distance': a['distance'] if 'distance' in a else None,

                    'elev_high': a['elev_high'] if 'elev_high' in a else None, 
                    'elev_low': a['elev_low'] if 'elev_low' in a else None, 
                    'total_elevation_gain': a['total_elevation_gain'] if 'total_elevation_gain' in a else None,

                    'moving_time': a['moving_time'] if 'moving_time' in a else None,
                    'elapsed_time': a['elapsed_time'] if 'elapsed_time' in a else None,
                    'average_speed': a['average_speed'] if 'average_speed' in a else None,
                    'max_speed': a['max_speed'] if 'max_speed' in a else None, 
                    
                    'average_cadence': a['average_cadence'] if 'average_cadence' in a else None,
                    'average_temp': a['average_temp'] if 'average_temp' in a else None,

                    'has_heartrate': a['has_heartrate'] if 'has_heartrate' in a else None,
                    'average_heartrate': a['average_heartrate'] if 'average_heartrate' in a else None,
                    'max_heartrate': a['max_heartrate'] if 'max_heartrate' in a else None

                })
            # Update to DB
            query = "INSERT INTO activities ({}) VALUES ({}) AS acts ON DUPLICATE KEY UPDATE {}".format(
                ', '.join(filtered_acts[0].keys()),
                ', '.join(['%s'] * len(filtered_acts[0].keys())),
                ', '.join(['{} = acts.{}'.format(key, key) for key in filtered_acts[0] if key != 'id'])
            )
            values = [list(data.values()) for data in filtered_acts]
            await self.pool.write(query, values, many=True)
        return new_act_count
    
    async def get_polyline(self, activity_id):
        query = "SELECT polyline FROM polylines WHERE id = %s"
        result = await self.pool.fetch(query, activity_id, all=False)
        if result:
            return result['polyline']
        else:
            # Get from Strava
            detailed_activity = await Strava.get_detailed_activity(self, activity_id)
            polyline = detailed_activity['map']['polyline']
            
            query = """INSERT INTO polylines (id, polyline) VALUES (%s, %s) 
                    ON DUPLICATE KEY UPDATE
                    polyline = %s,
                    latlng = latlng
                    """
            params = (activity_id, polyline, polyline)
            await self.pool.write(query, params)
        return polyline

    async def update_latlng(self, activity_id):
        latlng = await Strava.get_activity_streams(self, activity_id, 'latlng')
        latlng_update_count = 0
        for _, fetched_types in enumerate(latlng):
            if fetched_types['type'] == "latlng":
                # Encode List as Json
                encoded_latlng = json.dumps(fetched_types['data'])
                text_latlng =  str(encoded_latlng)
                # Upload To DB
                query = f"""
                        INSERT INTO polylines (id, latlng)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE 
                        polyline = polyline,
                        latlng = %s
                        """
                args = (activity_id, text_latlng, text_latlng)
                await self.pool.write(query, args)
                latlng_update_count += 1
        return encoded_latlng, latlng_update_count
    
    async def get_latlng(self, activity_id):
        query = "SELECT latlng FROM polylines WHERE id = %s"
        result = await self.pool.fetch(query, activity_id, all=False, dictionary=False)
        if result and result[0] != None:
            return result[0]
        else:
            latlng, _ = self.update_latlng(activity_id)
            return latlng
    # Note
    async def get_info(self):
        await self.get_strava_id()
        query = """
                SELECT first_name, last_name, push_notification, reminder_on, reminder_days, reminder_last
                from athletes WHERE strava_id = %s
                """
        info = await self.pool.fetch(query, self.strava_id)
        self.strava_name  = f"{info['last_name']} {info['first_name']}"
        self.push_notification_status = info['push_notification']
        self.reminder_on = info['reminder_on']
        self.reminder_days, self.reminder_last = info['reminder_days'], info['reminder_last']
        return info

    async def init_strava_profile(self):
        await self.get_strava_id()
        query = "INSERT INTO athletes (strava_id) VALUES (%s) ON DUPLICATE KEY UPDATE strava_id = %s"
        params = (self.strava_id, self.strava_id)
        await self.pool.write(query, params)
        return True

    async def get_profile_photo(self):
        await self.get_strava_id()
        query = "SELECT profile,last_refreshed from athletes WHERE strava_id =%s"
        result = await self.pool.fetch(query, self.strava_id)
        if result['last_refreshed'] + timedelta(days=7) < datetime.now():
            sv_profile = await self.update_profile()
            return sv_profile['profile']
        else:
            return result['profile']
      
    async def delete_activity(self, activity_id):
        query = "DELETE FROM strava_activities WHERE id = %s AND strava_id = %s"
        args = (activity_id, self.strava_id)
        result = await self.pool.write(query, args)
        return result
    
    async def delete_profile(self):
        query = "DELETE FROM users WHERE strava_id = %s"
        result = await self.pool.write(query, self.strava_id)
        return result

    async def get_detailed_activity_photo(self, activity_id):
        req = await Strava.get_detailed_activity(self, activity_id)
        activity_name = req['name']
        try:
            activity_photo = req['photos']['primary']['urls']['600']
        except:
            activity_photo = None
        if activity_photo is None:
            activity_photo = self.get_profile_photo()


class Strava:

    async def api_add_counter(pool):
        query = "INSERT INTO api_count (id) VALUES (NULL)"

        await pool.write(query)
        
    async def api_read_count(pool):
        # For 15 Minutes Block
        # async with pool.lock:
        #     time.sleep(2)
        
        now = datetime.now()
        minutes_past_hour = now.minute + now.second / 60.0
        block_start_minute = int(minutes_past_hour // 15) * 15
        block_start_time = now.replace(minute=int(block_start_minute), second=0, microsecond=0)
        block_end_time = block_start_time + timedelta(minutes=15)
        # For Daily
        utc_now = now - timedelta(hours=TZ_OFFSET)
        utc_start = utc_now.replace(hour=0, minute=0, second=0) + timedelta(hours=TZ_OFFSET)
        utc_end = utc_now.replace(hour=23, minute=59, second=59)+ timedelta(hours=TZ_OFFSET)
        query = """
                SELECT
                    SUM(CASE WHEN last_updated >= %s THEN 1 ELSE 0 END) AS block,
                    SUM(CASE WHEN last_updated >= %s THEN 1 ELSE 0 END) AS daily
                FROM api_count
            """
        params = (block_start_time, utc_start)
        
        result = await pool.fetch(query, params=params, all=False)
        
        api_count_block, api_count_daily = result['block'], result['daily']
        pool.daily = api_count_daily
        pool.block = api_count_block

        if api_count_daily >= API_LIMIT_DAILY:
            reset = utc_end + timedelta(seconds=1)
        else:
            reset = block_end_time
        logging.debug(f"API Count: {api_count_block}/{API_LIMIT_BLOCK} {api_count_daily}/{API_LIMIT_DAILY}  Reset in : {reset}")
        return api_count_daily, api_count_block, reset


    async def api_rate_limit(pool, wait=False):
        
        api_count_daily, api_count_block, reset = await Strava.api_read_count(pool)
        

        if api_count_daily >= API_LIMIT_DAILY:
            if wait:
                left = (reset - datetime.now()).total_seconds()
                logging.debug(f"API Rate Limit Exceeded for the day, resuming in {left} seconds ({reset})")
                await asyncio.sleep(left + 5)
                return True
            else:
                raise Exception(f"API Rate Limit Exceeded for the day, please try agian after {reset}")
        else:
            if api_count_block >= API_LIMIT_BLOCK:
                if wait:
                    left = (reset - datetime.now()).total_seconds()
                    logging.debug(f"API Rate Limit Exceeded for the time being, resuming in {left} seconds ({reset})")
                    await asyncio.sleep(left + 5)
                    return True
                else:
                    raise Exception(f"API Rate Limit Exceeded for the time being, please try again in after {reset}")
            return False
   
    async def api_clear_counter(pool):
        # For Daily
        utc_now = datetime.utcnow()
        utc_start = utc_now.replace(hour=0, minute=0, second=0) + timedelta(hours=TZ_OFFSET)
        query = "DELETE FROM api_count WHERE last_updated < %s"
        result = await pool.write(query, utc_start)
        logging.info(f"Deleted {result} rows from api_count table")
        if result:
            # Drop the auto-increment column (e.g., 'id' column)
            drop_column_query = "ALTER TABLE api_count DROP COLUMN id"
            await pool.write(drop_column_query)
            # Add back to reindex
            readd_column = "ALTER TABLE api_count ADD id INT AUTO_INCREMENT PRIMARY KEY"
            await pool.write(readd_column)
            logging.debug("Reindex Done")
        return result
    
    async def get_req(athlete: Athlete, sub_url, activity_id=None, **req_params):
        _retry_count, success = 0, False
        if activity_id is None:
            sub_url = sub_url.replace("{id}", str(athlete.strava_id))
        else:
            sub_url = f"{sub_url}/{activity_id}"
        if len(req_params) > 0:
            param = req_params
        else:
            param = {}
        url = f"https://www.strava.com/api/v3{sub_url}"

        while not success and _retry_count <= 3:
            try:
                if _retry_count <= 3:
                    _retry_count += 1
                    header = {'Authorization': 'Bearer ' + await athlete.get_auth()}

                    
                    await Strava.api_rate_limit(athlete.pool)

                    await Strava.api_add_counter(athlete.pool)

                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, headers=header, params=param, timeout=aiohttp.ClientTimeout(total=30)) as response:
                            if response.status == 200:
                                request = await response.json()
                                success = True
                                break
                            else:
                                request = {
                                    "response.status": response.status,
                                    "strava_id": athlete.strava_id
                                }
                                raise Exception("Request failed...")
                else:
                    logging.error(f"Request failed for {athlete.strava_id}! {sub_url}. Max No. of retries reached.")
                    raise Exception(f"Request failed for {athlete.strava_id}! {sub_url}")
            except Exception as e:
                logging.warning(f"Request failed for {athlete.strava_id}! {sub_url}, retrying..")
                await athlete.get_auth(_retry=True)
        return request
    
    async def authenticate(auth_code, pool):
        payload={
            'client_id': SV_CLIENT_ID,
            'client_secret': SV_CLIENT_SECRET,
            'code': auth_code,
            'grant_type': 'authorization_code'
        }
        Strava.api_rate_limit(pool)
        Strava.api_add_counter(pool)
        async with aiohttp.ClientSession() as session:
            async with session.post(SV_AUTH_URL, data=payload, verify_ssl=False, timeout=30) as response:
                return await response.json()
    
    async def refresh_access_token(refresh_token, pool):
        payload = {
                "client_id": SV_CLIENT_ID,
                "client_secret": SV_CLIENT_SECRET,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
                "f": 'json'
        }
        _retry_count, _fetched_token_success = 0, False
        while _retry_count <= 3 and _fetched_token_success == False:
            _retry_count =+1
            #Strava.api_rate_limit(pool)
            #Strava.api_add_counter(pool)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(SV_AUTH_URL, data=payload, verify_ssl=False, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        req = await response.json()
                if not "errors" in req:
                    _fetched_token_success = True
                    return req
            except:
                if _retry_count > 3:
                    raise Exception("Failed in refreshing access token")           
   
    async def get_profile(athlete: Athlete, from_new_user = False):
        strava_profile = await Strava.get_req(athlete, "/athlete")        
        return strava_profile

    async def get_heart_rate(athlete: Athlete):
        fetched_heart_rate = await Strava.get_req(athlete, "/athlete/zones")
        zones = fetched_heart_rate['heart_rate']['zones']
        return zones
    
    async def get_activities(athlete, **req_params):
        activities = await Strava.get_req(athlete,"/athlete/activities", None, **req_params)
        return activities
    
    async def get_detailed_activity(athlete, activity_id):
        detailed_activity = await Strava.get_req(athlete, "/activities", str(activity_id))
        return detailed_activity
    
    async def get_activity_streams(athlete, activity_id, stream_type):
        params = {'keys': stream_type}
        streams = await Strava.get_req(athlete, f"/activities/{activity_id}/streams", None, **params)
        return streams