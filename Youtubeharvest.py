#-----------------------------------------------------------------------------------------------------#
# Run in the below in the terminal 
#-----------------------------------------------------------------------------------------------------#
#pip install google-api-python-client 
#pip install isodate
#-----------------------------------------------------------------------------------------------------#

#-----------------------------------------------------------------------------------------------------#
# Importing required packages
#-----------------------------------------------------------------------------------------------------

import streamlit as st 
import pandas as pd 
import mysql.connector as db
from mysql.connector import Error 
import numpy as np
import isodate
from datetime import datetime
import isodate
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import logging 

#-----------------------------------------------------------------------------------------------------#
# set logging details 
#-----------------------------------------------------------------------------------------------------#
file_dt = datetime.now().strftime("_%Y%m%d_%H%M%S")
date_time = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
#str_date_time = str(date_time)
# create a file object along with extension
file_name = "YoutubeHarvest"+file_dt+".log" 
Logfile = open (file_name,"w",encoding='utf-8') 
Logfile.write ("Log Starting "+ repr(date_time) + '\n' )

#filename='YH{current_date}.log'
logging.basicConfig(
    filename = 'Error.log',
    level=logging.DEBUG,                  # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Log format
    datefmt='%Y-%m-%d %H:%M:%S'           # Date format
    )

logger = logging.getLogger(__name__)  # Create a logger object


#-----------------------------------------------------------------------------------------------------#
# Defining Youtube api_service_name, api_version, developerKey 
#-----------------------------------------------------------------------------------------------------#

api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyA9fRsLUzUD52RwVghJDrQRkYNxZZp_ijo"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

logger.info("Define Youtube api_service_name, api_version, developerKey") 
Logfile.write("\nDefine Youtube api_service_name, api_version, developerKey") 
#-----------------------------------------------------------------------------------------------------#
# Defining Database Connection 
#-----------------------------------------------------------------------------------------------------#

db_conn = db.connect (
    host = 'localhost', #server ip
    user = "Senthil",
    password = "Mysql2@24",
    database = "yth" ,
    connect_timeout=10000
    )
curr = db_conn.cursor()

curr.execute("SET GLOBAL innodb_lock_wait_timeout = %s", (1000,))
# Commit the change
db_conn.commit() 

logger.info("Define Database Connection")
Logfile.write("\nDefine Database Connection") 

#-----------------------------------------------------------------------------------------------------#
# Define function to Harvest Channel details 
#-----------------------------------------------------------------------------------------------------#
def Chan_data (chan_id):
    #-----------------------------------------------------------------------------------------------------#
    # Channel request details 
    #-----------------------------------------------------------------------------------------------------#
    chan_request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=chan_id
    )
    chan_response = chan_request.execute() 
        
    chan_name=chan_response['items'][0]['snippet']['title']
    chan_desc=chan_response['items'][0]['snippet']['description']
    chan_pubat=chan_response['items'][0]['snippet']['publishedAt']
    chan_playid=chan_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    chan_sub=chan_response['items'][0]['statistics']['subscriberCount']
    chan_vcnt=chan_response['items'][0]['statistics']['viewCount']
    chan_vidcnt=chan_response['items'][0]['statistics']['videoCount']

    Logfile.write("\nChannel id : " + repr(chan_id) + '\n' )
    Logfile.write("Channel playlist ID : " + repr(chan_playid) + '\n' )
    Logfile.write("Channel name : " + repr(chan_name) + '\n' )
    Logfile.write("Channel desc : " + repr(chan_desc) + '\n' )
    Logfile.write("Channel publish date : " + repr(chan_pubat) + '\n' )
    Logfile.write("Channel subscriber count : " + repr(chan_sub) + '\n' )
    Logfile.write("Channel view count: " + repr(chan_vcnt) + '\n' )
    Logfile.write("Channel video count: " + repr(chan_vidcnt) + '\n' )

    #-----------------------------------------------------------------------------------------------------#
    # DB insert to channels table     
    #-----------------------------------------------------------------------------------------------------#

    chan_sql = """INSERT INTO yth.channels (chn_id, chn_name, chn_type , chn_views , chn_desc , chn_status ) VALUES (%s, %s, %s, %s, %s, %s)"""
    chan_val = (chan_id,chan_name, None, chan_vcnt, chan_desc, None) 
    try:
        curr.execute(chan_sql, chan_val) 
        print  ('Data inserted in to channels table') 
        Logfile.write("Data inserted in to channels table : " + repr(chan_name) + '\n' )
    except db.Error as error:
        print("Error Inserting data into channels table Check the log:", error)
        Logfile.write("Error Inserting data into channels table Check the log : " + repr(error) + '\n' )

        
    #-----------------------------------------------------------------------------------------------------#
    # Playlist & Video request details 
    #-----------------------------------------------------------------------------------------------------#
    playlst_request = youtube.playlistItems().list(
        part="snippet,status,contentDetails",
        playlistId=chan_playid 
    )
    playlst_response = playlst_request.execute() 
    playlst_id = chan_playid
    playlst_dtls = playlst_response['items']
    playlst_name = playlst_response ['items'][0]['snippet']['channelTitle']

    Logfile.write("\nplaylists Name : " + repr(playlst_name) + '\n' )

    #-----------------------------------------------------------------------------------------------------#
    # DB insert to playlist table     
    #-----------------------------------------------------------------------------------------------------#

    play_sql = """INSERT INTO yth.playlists (playlist_id, chn_id, playlist_name ) VALUES (%s, %s, %s)"""
    play_val = (playlst_id, chan_id, playlst_name) 

    try:
        curr.execute(play_sql, play_val) 
        print  ('Data inserted in to playlists table') 
        Logfile.write("Data inserted in to playlists table : " + repr(playlst_name) + '\n' )
    except db.Error as error:
        print("Error Inserting data into playlists table Check the log:", error)
        Logfile.write("Error Inserting data into playlists table Check the log : " + repr(error) + '\n' )

        
    video_id=[]
    for i in playlst_dtls:
        video_id = i ['snippet']['resourceId']['videoId']
        video_name = i ['snippet']['title']
        video_desc = i ['snippet']['description']
        temp_pub_date = i ['snippet']['publishedAt']
        
        # Parse the string into a datetime object
        parse_pub_date = datetime.strptime(temp_pub_date, '%Y-%m-%dT%H:%M:%SZ')
        # Convert datetime object to MySQL datetime format
        convert_pub_date = parse_pub_date.strftime('%Y-%m-%d %H:%M:%S')
        pub_date = convert_pub_date
         
        Logfile.write("\nvideo Id : " + repr(video_id) + '\n' )
        Logfile.write("video Name : " + repr(video_name) + '\n' )
        Logfile.write("video Desc : " + repr(video_desc) + '\n' )
        Logfile.write("Publish Date : " + repr(pub_date) + '\n' )
        
        video_request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        video_response = video_request.execute() 
        video_dtls = video_response['items']
        try:
            for j in video_dtls:
                view_cnt = j ['statistics']['viewCount']
                like_cnt = j ['statistics']['likeCount']
                fav_cnt = j ['statistics']['favoriteCount']
                if j ['statistics']['favoriteCount'] == 1:
                    cmmt_cnt = j ['statistics']['commentCount']
                else: cmmt_cnt = None
                temp_duration = j ['contentDetails']['duration']
                caption_status = j ['contentDetails']['caption']
                thumbnail = j ['snippet']['thumbnails']
                
                Logfile.write("View cnt : " + repr(view_cnt) + '\n' )
                Logfile.write("Like cnt : " + repr(like_cnt) + '\n' )
                Logfile.write("Favorite Cnt : " + repr(fav_cnt) + '\n' )
                Logfile.write("Comment Cmt : " + repr(cmmt_cnt) + '\n' )
                Logfile.write("Duration : " + repr(temp_duration) + '\n' )
                Logfile.write("Caption Status : " + repr(caption_status) + '\n' )
                Logfile.write("Thumbnail : " + repr(thumbnail) + '\n' )

                # Parse the duration string
                parse_duration = isodate.parse_duration(temp_duration)
                # Convert duration to seconds
                duration_seconds = int(parse_duration.total_seconds())
                duration = duration_seconds

                #-----------------------------------------------------------------------------------------------------#
                ## DB insert to Video table     
                #-----------------------------------------------------------------------------------------------------#
                vid_sql = """INSERT INTO yth.videos (video_id,playlist_id,video_name,video_desc,pub_date,view_cnt,like_cnt,dislike_cnt,favorite_cnt,comment_cnt,duration,thumnail,caption_status ) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s)"""
                vid_val = (video_id,playlst_id,video_name, video_desc, pub_date, view_cnt, like_cnt, None, fav_cnt, cmmt_cnt, duration, None, caption_status ) 
            
                curr.execute(vid_sql, vid_val) 
                print  (video_name,'Data inserted in to Videos table')
                Logfile.write("\nData inserted in to Videos table : " + repr(video_name) + '\n' )

        except db.Error as error:
            print (video_name, "iteration Error Inserting data into videos table Check the log:", error)
            Logfile.write("\niteration Error Inserting data into videos table : " + repr(error) + '\n' )

        #-----------------------------------------------------------------------------------------------------#
        # Comments request details 
        #-----------------------------------------------------------------------------------------------------#
        try:
            comments_request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id
            )
            comments_response = comments_request.execute() 
        
            for k in comments_response['items']:
                Comment_Id = k ['snippet']['topLevelComment']['id']
                Comment_Text =  k ['snippet']['topLevelComment']['snippet']['textDisplay']
                Comment_Author = k ['snippet']['topLevelComment']['snippet']['authorDisplayName']
                temp_Comment_Pub_date =  k ['snippet']['topLevelComment']['snippet']['publishedAt']
                
                Logfile.write("\nComment Id : " + repr(Comment_Id) + '\n' )
                Logfile.write("Comment Text : " + repr(Comment_Text) + '\n' )
                Logfile.write("Comment Author : " + repr(Comment_Author) + '\n' )
                Logfile.write("Comment Published Date : " + repr(temp_Comment_Pub_date) + '\n' )

                # Parse the string into a datetime object
                parse_pub_date = datetime.strptime(temp_Comment_Pub_date, '%Y-%m-%dT%H:%M:%SZ')
                # Convert datetime object to MySQL datetime format
                convert_pub_date = parse_pub_date.strftime('%Y-%m-%d %H:%M:%S')
                Comment_Pub_date = convert_pub_date
                #-----------------------------------------------------------------------------------------------------#
                ## DB insert to Comments table     
                #-----------------------------------------------------------------------------------------------------#
                cmt_sql = """INSERT INTO yth.comments (comment_id, video_id, comment_text, comment_authr, comment_pub_date) VALUES (%s, %s, %s, %s, %s)"""
                cmt_val = (Comment_Id, video_id, Comment_Text, Comment_Author, Comment_Pub_date ) 
                
                curr.execute(cmt_sql, cmt_val)  
                print  ('Data inserted in to Comments table')
                Logfile.write("Data inserted in to Comments table : " + repr(Comment_Id) + '\n' )

        except HttpError as error:
                if error.resp.status in [403, 404]:
                # Handle specific HTTP status codes (e.g., access denied or resource not found)
                    print("Error: YouTube comments are unavailable.")
                    Logfile.write("\nError: YouTube comments are unavailable." )

                else: 
                    print("Error Inserting data into Comments table Check the log:", error)
                    Logfile.write("Error Inserting data into Comments table Check the log : " + repr(error) + '\n' )

    else:
        print  ('entered else')   
        Logfile.write("\nentered else " )
       
    db_conn.commit() 
    print  ('Commit completed - data entered in to the Database') 
    Logfile.write("\nCommit completed - data entered in to the Database" )
logger.info("Fucntion Completed")

#-----------------------------------------------------------------------------------------------------#
## Code for Streamlit page      
# #-----------------------------------------------------------------------------------------------------#

st.set_page_config(layout="wide")
with st.sidebar:
    st.sidebar.image('youtube_logo.jpg', use_column_width=True)
    st.title("Youtube Harvesting by SK")

label = "Channel ID"
Channel_id_Entered = st.text_input(label)
chan_id = Channel_id_Entered
st.write("The Channel ID entered :", Channel_id_Entered)
if st.button('Submit'):
    sql = """select chn_name, count(Chn_id) from yth.channels Where chn_id = %s;"""
    val = (chan_id)
    curr.execute(sql, [val])  
    chn_chk = curr.fetchall()
    if chn_chk[0][1] == 1: 
        st.write(chn_chk[0][0], " Channel details already present") 
    else:
        st.write("Harvesting data for :", Channel_id_Entered)
        Chan_data (chan_id) 
        logger.info("Fucntion called")
        st.write("Data Harvest Completed")

#-----------------------------------------------------------------------------------------------------#
# Code List of SQL Queries 
#-----------------------------------------------------------------------------------------------------#

Q1 = "Q1 - List all Videos & their corresponding Channels"
Q2 = "Q2 - Channels with most No of videos & their Count"
Q3 = "Q3 - Top 10 most viewed Videos & their Channels"
Q4 = "Q4 - No of Comments made in each video & their corresponding Video Name"
Q5 = "Q5 - Videos with Highest Likes & their corresponding Channels"
Q6 = "Q6 - Total No of Likes and Dislikes of all videos & their corresponding Channels"
Q7 = "Q7 - Total No of Views of each Channel & their Channel Names"
Q8 = "Q8 - Channels that published videos in 2022 & their Channels"
Q9 = "Q9 - Average duration of all videos in a channel & their corresponding Channel Names"
Q10 = "Q10 - Videos with highest no of comments & their corresponding Channel Names"

option = st.selectbox(
    "Please select a Question to run SQL Query :)",
    (Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10))

if option == Q1: 
    curr.execute("select c.video_name as 'Video Name', a.chn_name as 'Channel Name' from yth.channels a, yth.playlists b, yth.videos c Where a.chn_id = b.chn_id and b.playlist_id = c.playlist_id order by a.chn_id") 
    dtls = curr.fetchall() 
    df = pd.DataFrame(dtls, columns= ['Video Name','Channel Name'])
    st.table(df)
elif option == Q2:    
     curr.execute("select a.chn_name as 'Channel Name', count(c.video_id) as 'No of Videos' from yth.channels a, yth.playlists b, yth.videos c Where a.chn_id = b.chn_id and b.playlist_id = c.playlist_id group by c.playlist_id order by a.chn_id limit 5") 
     dtls = curr.fetchall()
     df = pd.DataFrame(dtls, columns= ['Channel Name', 'No of Videos'])
     st.table(df)
elif option == Q3:    
     curr.execute("select c.video_name as 'Video Name', c.view_cnt as 'View Count', a.chn_name as 'Channel Name' from yth.channels a, yth.playlists b, yth.videos c Where a.chn_id = b.chn_id and b.playlist_id = c.playlist_id order by c.view_cnt desc limit 10") 
     dtls = curr.fetchall()
     df = pd.DataFrame(dtls, columns= ['Video Name', 'View Count', 'Channel Name'])
     st.table(df)
elif option == Q4:    
     curr.execute("select * from (select a.video_name as 'Video Name', count(b.comment_id) as 'Comment Count' from yth.videos a, yth.Comments b Where a.video_id = b.video_id group by b.video_id  UNION select c.video_name , c.comment_cnt from yth.videos c Where c.comment_cnt = 0) d order by 1") 
     dtls = curr.fetchall()
     df = pd.DataFrame(dtls, columns= ['Video Name', 'Comment Count'])
     st.table(df)
elif option == Q5:    
     curr.execute("select c.video_name as 'Video Name', c.like_cnt as 'Like Count', a.chn_name as 'Channel Name' from yth.channels a, yth.playlists b, yth.videos c Where a.chn_id = b.chn_id and b.playlist_id = c.playlist_id order by c.view_cnt desc limit 10") 
     dtls = curr.fetchall()
     df = pd.DataFrame(dtls, columns= ['Video Name', 'Like Count', 'Channel Name'])
     st.table(df)
elif option == Q6:    
     curr.execute("select video_name as 'Video Name', like_cnt as 'Like Count' from yth.videos order by video_name") 
     dtls = curr.fetchall()
     df = pd.DataFrame(dtls, columns= ['Video Name', 'Like Count'])
     st.table(df)
elif option == Q7:    
     curr.execute("select chn_name as 'Channel Name', chn_views as 'Total Channel Views' from yth.channels order by chn_name") 
     dtls = curr.fetchall()
     df = pd.DataFrame(dtls, columns= ['Channel Name', 'Total Channel Views'])
     st.table(df)
elif option == Q8:    
     curr.execute("select a.chn_name as 'Channel Name', c.pub_date as 'Publish Date' from yth.channels a, yth.playlists b, yth.videos c Where a.chn_id = b.chn_id and b.playlist_id = c.playlist_id and year (c.pub_date) = 2022 order by a.chn_name") 
     dtls = curr.fetchall()
     df = pd.DataFrame(dtls, columns= ['Channel Name', 'Publish Date'])
     st.table(df)
elif option == Q9:    
     curr.execute("select a.chn_name as 'Channel Name', concat (avg(c.duration), ' Seconds') as 'Average Duration of Videos' from yth.channels a, yth.playlists b, yth.videos c Where a.chn_id = b.chn_id and b.playlist_id = c.playlist_id group by c.playlist_id order by a.chn_name") 
     dtls = curr.fetchall()
     df = pd.DataFrame(dtls, columns= ['Channel Name', 'Average Duration of Videos'])
     st.table(df)
else:    
     curr.execute("select a.chn_name as 'Channel Name', c.video_name as 'Video Name', c.comment_cnt as 'Comment Count' from yth.channels a, yth.playlists b, yth.videos c Where a.chn_id = b.chn_id and b.playlist_id = c.playlist_id order by c.comment_cnt desc limit 10") 
     dtls = curr.fetchall()
     df = pd.DataFrame(dtls, columns= ['Channel Name', 'Video Name','Comment Count'])
     st.table(df)
     
     
curr.close()
db_conn.close() 
Logfile.write ("\nLog Closing" + repr(date_time) + '\n' ) 
Logfile.close()
logger.info("Streamlit Created")