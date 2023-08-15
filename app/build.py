# OS, File Logging
import os
import aiofiles
import aiofiles.os
from io import BytesIO
import logging
# Pandas
import pandas as pd
import numpy as np
import dataframe_image as dfi
# Import: Matplot
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import ticker
from matplotlib.ticker import FormatStrFormatter
# GPX Needed
import gpxpy.gpx
import xml.etree.ElementTree as mod_etree
from datetime import timezone, timedelta
# Custom
from strava import Athlete, Strava

BG_COL = "#282828"
FONT_COL = "#FFFFFF"
SWIM_COL = "#2B9DE1"
BIKE_COL = "#9EBB02"
RIDE_COL = "#9EBB02"
RUN_COL = "#F8931F"
WALK_COL = "#75DBCD"
HIKE_COL = "#75DBCD"
OTHERS_COL = "#EF2D56"
GRAPH_BG_COL = "#282828"
SPEED_HR_COL = "red"

class Build:
    async def stats_table(athlete: Athlete, hr_zone=0, act_type="all", page_number=1, page_size=5) -> BytesIO: 
        
        result, _, hr_zone_data = await athlete.get_activity_list(hr_zone, act_type, page_number, page_size)
        hr_zone, min_heart_rate, max_heart_rate = hr_zone_data['zone'], hr_zone_data['min'], hr_zone_data['max']

        activities_df = pd.DataFrame(result)

        if len(activities_df.index) != 0:
            # Replace none with NaN to calculate values
            activities_df = activities_df.fillna(value=np.nan)

            # If HR not 0 then filter
            if hr_zone != 0:
                activities_df = activities_df[(activities_df['average_heartrate']>min_heart_rate) & (activities_df['average_heartrate']<=max_heart_rate)]
            # Convert Ride to Bike
            activities_df['type']=activities_df['type'].replace('Ride','Bike')
            # Convert Distance to KM
            activities_df['distance_m'] = activities_df['distance']
            activities_df['distance_km'] = (activities_df['distance']/1000).round(2)
            # Avg Speed to KM
            activities_df['average_speed'] = (activities_df['average_speed']*3600)/1000

            # Avg Pace for Running
            activities_df['pace_km_min'] = ((activities_df['moving_time'] / activities_df['distance'])*1000).apply(
                lambda x: '{:02d}:{:02d}/KM'.format(int(x // 60), int(x % 60)) if pd.notnull(x) and x != float('inf') else ''
            )
            # Avg Pace for Swimming
            activities_df['pace_100m_s'] = ((activities_df['moving_time']/activities_df['distance'])*100).apply(
                lambda x: '{:02d}:{:02d}/100m'.format(int(x // 60), int(x % 60)) if pd.notnull(x) and x != float('inf') else ''
            )
            # Avg Pace for Cycling
            activities_df['pace_km_hr'] = ((activities_df['distance'] / 1000) / (activities_df['moving_time'] / 3600)).apply(
                lambda x: '{:.2f} KM/h'.format(x) if pd.notnull(x) and x != float('inf') else ''
            )
            # Spd/HB
            activities_df['avg_sphb'] = (activities_df['average_speed']/activities_df['average_heartrate']).round(5)
            activities_df.loc[activities_df['avg_sphb'] == 0, 'avg_sphb'] = " - "
            # Set HR Zone
            activities_df.loc[(activities_df['average_heartrate'] > 0) & (activities_df['average_heartrate'] <= athlete.heart_rate['z1']),"Avg Zone"] = "1"
            activities_df.loc[(activities_df['average_heartrate'] > athlete.heart_rate['z1']) & (activities_df['average_heartrate'] <= athlete.heart_rate['z2']),"Avg Zone"] = "2"
            activities_df.loc[(activities_df['average_heartrate'] > athlete.heart_rate['z2']) & (activities_df['average_heartrate'] <= athlete.heart_rate['z3']),"Avg Zone"] = "3"
            activities_df.loc[(activities_df['average_heartrate'] > athlete.heart_rate['z3']) & (activities_df['average_heartrate'] <= athlete.heart_rate['z4']),"Avg Zone"] = "4"
            activities_df.loc[(activities_df['average_heartrate'] > athlete.heart_rate['z4']),"Avg Zone"] = "5"
        

            # Convert Date
            activities_df['start_date_local']=activities_df['start_date_local'].dt.strftime('%d/%m/%Y')
     
            try:
                activities_df['moving_time'] = pd.to_datetime(activities_df['moving_time'], unit='s')
                
                activities_df['moving_time'] = activities_df['moving_time'].apply(
                                                    lambda x: x.strftime("%H:%M:%S") if x.hour >= 1 else x.strftime("%M:%S")
                                                )
            except:
                print("Failed")
                pass
            activities_df['average_speed'] = activities_df['average_speed'].round(2)
            

           
            
            # Adjust Column Name
            # activities_df.columns = ['Date', 'Type', 'Distance', 'Duration', 'Avg/Speed', 'Avg/HR', 'Avg/Cad', 'TEG', 'DistanceM', 'DistanceKM', 'Pace_s_km', 'Pace_s_100m', 'AvgSphb', 'AvgZone']
           
            # Set Distance by Type
            activities_df['distance'] = np.where(
                (
                    (activities_df['type'] == 'Run') | 
                    (activities_df['type'] == 'Trail Run') | 
                    (activities_df['type'] == 'Virtual Run') | 
                    (activities_df['type'] == 'Ride') |
                    (activities_df['distance_m'] >= 5000)
                ) &
                ~(
                    (activities_df['distance'] == 0) |
                    activities_df['distance'].isna()
                ),
                activities_df['distance_km'].apply(lambda x: '{:.2f}KM'.format(x)),
                np.where(
                    (
                        (activities_df['distance'] == 0) |
                        activities_df['distance'].isna()
                    ),
                    '',
                    activities_df['distance'].apply(lambda x: '{:.2f}m'.format(x))
                )
            )
            # Set Pace by Type
            activities_df['average_speed'] = activities_df.apply(
                lambda row: row['pace_100m_s'] if row['type'] == 'Swim'
                    else row['pace_km_min'] if row['type'] in ['Run', 'Trail Run', 'Virtual Run']
                    else row['pace_km_hr'],
                    axis=1
            )

            # Remove Unwanted Columns
            columns_to_drop = ['total_elevation_gain', 'distance_m', 'distance_km','pace_km_min','pace_100m_s', 'pace_km_hr']
            activities_df = activities_df.drop(columns_to_drop, axis=1)
            # Adjust Column Names
            activities_df.columns = ['Date', 'Type', 'Dist', 'Duration', 'Pace', 'A.HR', 'A.Cad', 'A.Sphb', 'A.Zone']
            activities_df = activities_df[['Date', 'Type', 'Dist', 'Duration', 'Pace', 'A.Cad', 'A.HR', 'A.Zone', 'A.Sphb']]
            # Replace NA
            # activities_df = activities_df.replace(['NaN', 'None'], np.nan)
            
            cols_to_str = ['A.Cad', 'A.HR', 'A.Sphb']
            activities_df[cols_to_str] = activities_df[cols_to_str].astype(str)


            activities_df = activities_df.fillna("-").replace("", '-')


            activities_df = activities_df.reset_index(drop=True)
            activities_df.index = activities_df.index + 1
            
            # Style Graph
            styled_df = (
                    activities_df.style
                    
                    .set_properties(**{'text-align': 'left'})
                    .set_properties(subset=['A.Zone'], **{'text-align': 'center'})
                )
            table_style = [
     
                        dict(selector="th", props=[
                                                ("padding", "12px 20px"),
                                                ("color", FONT_COL),
                                                ("font-family", "'HelveticaNeueLight', 'HelveticaNeue-Light', 'Helvetica Neue Light', 'Helvetica Neue', Helvetica, Arial, 'Lucida Grande', sans-serif"),
                                                # ("border", "1px solid black"),
                                                ("background-color",  BG_COL)
                                                ]),                                              
                        dict(selector="td", props=[
                                                ("padding", "12px 20px"),
                                                ("color", FONT_COL),
                                                # ("border", "1px solid white"),
                                                ("border-collapse", "collapse"),
                                                ("font-size", "15px")
                                                ]),
                        dict(selector="tr", props=[
                                                ("border", "1px solid #3C3C3C")
                        ]),
                        dict(selector="tr:nth-of-type(odd)", props=[
                                                ("background-color",  BG_COL)
                                                
                        ]),
                        dict(selector="tr:nth-of-type(even)", props=[
                                                ("background-color",  "#4C4C4C")
                                                
                        ])
            ]
            
            # Export Table
            styled_df = styled_df.set_table_styles(table_style)
            file_df = BytesIO()
            file_df.name = f"stats_table_{athlete.strava_id}.png"
            dfi.export(styled_df, file_df,  max_rows=-1)
            file_df.seek(0)

            # Write the content of the BytesIO object to the file
            with open('output.png', 'wb') as f:
                f.write(file_df.getvalue())

        return file_df
    
    async def gpx(athlete: Athlete, activity_id):

        await athlete.get_strava_id()
        gpx_root = "gpx_files"
        athlete_gpx_folder = os.path.join(gpx_root, str(athlete.strava_id))
        gpx_path = os.path.join(athlete_gpx_folder, str(activity_id) + ".gpx")
        
        await aiofiles.os.makedirs(athlete_gpx_folder, exist_ok=True)
        if not os.path.isfile(gpx_path):
            # Get Strava Athlete Info and Name
            if not hasattr(athlete, 'strava_name'):
                await athlete.get_info()
            activity = await athlete.get_activity(activity_id)
            start_datetime_utc = activity['start_date'].astimezone(timezone.utc)
            activity_name = f"{athlete.strava_name} {activity['name']}"
            # Prepare Streams
            base_streams_to_build = ['latlng', 'time', 'altitude', 'distance']
            streams_to_build = []
            streams = {}
            # Check For Valid Streams
            if activity['average_heartrate'] is not None:
                streams_to_build.append("heartrate")
            if activity['average_cadence'] is not None:
                streams_to_build.append("cadence")
            if activity['average_temp'] is not None:
                streams_to_build.append("temp")

            # After checking valid activity and stream to fetch
            # Combine Streams
            streams_to_build = streams_to_build + base_streams_to_build
            # Download Streams
            for stream_type in streams_to_build:
                fetched_streams = await Strava.get_activity_streams(athlete, activity_id, stream_type)
                
                for stream in fetched_streams:
                    if stream['type'] == stream_type:
                        if stream_type not in streams:
                            streams[stream_type] = stream['data']

            # Get All Keys of Fetched Streams
            fetched_streams = [stream for stream in streams]
            # Get Fetched Ext Streams
            fetched_ext_streams = [ext_stream for ext_stream in fetched_streams if ext_stream not in base_streams_to_build]
            # Create Pandas Dataframe to merge streams
            combine_streams = pd.DataFrame([*streams['latlng']], columns=['lat', 'long'])
            combine_streams['altitude'] = streams['altitude']
            combine_streams['time'] = [(start_datetime_utc + timedelta(seconds=t)) for t in streams['time']]

            
            # If there are valid extension streams
            if len(fetched_ext_streams) > 0:
                # Declare Name Space and set up Extension Element
                # Add Extension Streams to pandas
                for ext_streams in fetched_ext_streams:
                    if "heartrate" in fetched_ext_streams:
                        combine_streams['hr'] = streams['heartrate']
                    if "cadence" in fetched_ext_streams:
                        combine_streams['cad'] = streams['cadence']
                    if "temp" in fetched_ext_streams:
                        combine_streams['atemp'] = streams['temp']

            # Prep GPX File
            gpx = gpxpy.gpx.GPX()
            gpx.name = f"{activity_name}"
            # Create first track in our GPX:
            gpx_track = gpxpy.gpx.GPXTrack()
            gpx.tracks.append(gpx_track)
            # Create first segment in our GPX track:
            gpx_segment = gpxpy.gpx.GPXTrackSegment()
            gpx_track.segments.append(gpx_segment)
            # Namespace for extensions
            ext_namespace = 'gpxtpx:'

            for idx in combine_streams.index:
                # Add Main Track Point
                gps_track_point = gpxpy.gpx.GPXTrackPoint(
                    combine_streams.loc[idx, 'lat'], combine_streams.loc[idx, 'long'], elevation=combine_streams.loc[idx, 'altitude'], time=combine_streams.loc[idx, 'time'])
                
                # Create Extension Element if there is
                if len(fetched_ext_streams) > 0:
                    ext_data_root = mod_etree.Element(f'{ext_namespace}TrackPointExtension')
                    for ext_streams in fetched_ext_streams:
                        if ext_streams =="heartrate":
                            ext_shortname = "hr"
                        elif ext_streams == "cadence":
                            ext_shortname = "cad"
                        elif ext_streams == "temp":
                            ext_shortname = "atemp"
                        ext_data_sub = mod_etree.SubElement(ext_data_root, f'{ext_namespace}{ext_shortname}').text=str(combine_streams.loc[idx, f'{ext_shortname}'])
                        # Append Extensions
                    gps_track_point.extensions.append(ext_data_root)
                #Append Track Points to Segments
                gpx_segment.points.append(gps_track_point)
            
            # Save GPX File
            async with aiofiles.open(gpx_path, mode='w') as f:
                await f.write(gpx.to_xml())
            # Some Hacky Method To Modify GPX Header to get extension.
            with open(gpx_path, "r+") as f:
                gpx_header = '<gpx creator="StravaGPX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.topografix.com/GPX/1/1 http://www.topografix.com/GPX/1/1/gpx.xsd http://www.garmin.com/xmlschemas/GpxExtensions/v3 http://www.garmin.com/xmlschemas/GpxExtensionsv3.xsd http://www.garmin.com/xmlschemas/TrackPointExtension/v1 http://www.garmin.com/xmlschemas/TrackPointExtensionv1.xsd" version="1.1" xmlns="http://www.topografix.com/GPX/1/1" xmlns:gpxtpx="http://www.garmin.com/xmlschemas/TrackPointExtension/v1" xmlns:gpxx="http://www.garmin.com/xmlschemas/GpxExtensions/v3">'
                lines = f.readlines()
                f.seek(0)
                for idx, line in enumerate(lines, start = 1):
                    if idx == 2:
                        f.write(gpx_header)
                    else:
                        f.write(line)
                f.truncate()
                #if len(fetched_ext_streams) != 0:
                logging.debug(f"Generated GPX file for Activity ID: {activity_id} for {athlete.strava_id} with {', '.join(fetched_ext_streams)} ext streams")
            return gpx_path
        # Send Straight
        else:
            return gpx_path
        
    async def gen_summary(athlete: Athlete, activity_ids):
        pass
    async def gen_map(athete: Athlete, activity_ids):
        pass
    async def activity_map(athlete: Athlete, activity_ids):
        pass
    # Max 5
    async def gen_graph(athlete: Athlete, activities_ids):
        pass
    
    # Max 3
    async def gen_faux_im(athlete: Athlete, activities_ids, photo_activity_id=None):
        await athlete.get_info()
        name = athlete.strava_name.upper()

        # Get Last Activity Photo
        if photo_activity_id is None:
            photo_activity_id = activities_ids[:-1]
        
        
        

        act_ids_query = ",".join(map(str, activities_ids))
        
        act_details_query = f"""
                            SELECT * FROM activities 
                            WHERE id IN ({act_ids_query})
                            """
    

        
        # activities_len = len(activities_ids)
        # for activity in activities_ids:
        #     pass
        



        pass



# Menu
# - Show Summary of Swim Bike Run
# - View Activities
# - Trend for Swim, Bike, Run
# - Media Generator
#    - Activities Overview
#    - Faux IM
#    - Timelapse (Coming Soon)
# - Timelapse [Year Range]
# - Configure
#    - Summary View
#    - Enable/Disable Push Notification
#    - Enable/Disable Reminder
#    - Reauthtenticate with Strava
#    - Delete Profile


