import streamlit as st
import pymongo
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import isodate
from dateutil import parser
import sqlalchemy as sq
from sqlalchemy import create_engine
import pymysql
import plotly.express as px

st.set_page_config(layout='wide')
# Title
st.title(':red[Youtube Data Harvesting]')
with st.sidebar:
    channel_id=st.text_input("Enter 11 digit channel_id")
    option = st.radio(
    'Select any one to perform',
    ('Home','Getdata','Migratedata','Querydata')) 
if option == "Getdata":
        st.header(':violet[Data collection zone]')
        st.write ('(Note:- This zone **collect data** by using channel id and **stored it in the :green[MongoDB] database**.)')
        api_service_name = 'youtube'
        api_version = 'v3'
        api_key = 'AIzaSyCi5IRr7-82xryfUID9koA4otQJoItSxG8'
        youtube = build(api_service_name,api_version,developerKey =api_key)
        def get_channel_details(channel_id):
            ch_data = []
            response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id).execute()

            for i in range(len(response['items'])):
                data = dict(
                    Channel_name = response['items'][i]['snippet']['title'],
                    playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description']
                    )
                ch_data.append(data)
            return ch_data

                
        
        def get_channel_playlists(channel_id):
            response = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=100  # Adjust the maximum number of playlists 
        ).execute()
            playlists = response['items']
            channel_playlists=[]
        
            for playlist in playlists:
               data= dict(playlist_id = playlist['id'],
               playlist_title = playlist['snippet']['title'],
               )
               channel_playlists.append(data)
            return channel_playlists
        
        def get_video_ids(youtube, playlist_id):
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_id,
                maxResults = 50)
            response=request.execute()
            video_ids = []
    
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            next_page_token = response.get('nextPageToken')
            more_pages = True
    
            while more_pages:
                if next_page_token is None:
                    more_pages = False
                else:
                    request = youtube.playlistItems().list(
                                  part='contentDetails',
                                  playlistId = playlist_id,
                                  maxResults = 50,
                                  pageToken = next_page_token)
                    response = request.execute()
    
                    for i in range(len(response['items'])):
                        video_ids.append(response['items'][i]['contentDetails']['videoId'])
                    next_page_token = response.get('nextPageToken')
        
            return video_ids
        
        def get_video_details(youtube,video_ids):
                all_video_info = []
                for i in range(0, len(video_ids), 50):
                    request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(video_ids[i:i+50])
                  )
                    response = request.execute() 

                    for video in response['items']:
                        stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount','dislikeCount', 'favouriteCount', 'commentCount'],
                             'contentDetails': ['duration', 'definition', 'caption']
                            }
                        video_info = {}
                        video_info['video_id'] = video['id']

                        for k in stats_to_keep.keys():
                           for v in stats_to_keep[k]:
                              try:
                                video_info[v] = video[k][v]
                              except:
                                video_info[v] = None

                        all_video_info.append(video_info)
                return all_video_info
        
        def get_comment_data(youtube, video_ids):
            comments_data = []
            for ids in video_ids:
                try: 
                  video_data_request = youtube.commentThreads().list(
                  part="snippet",
                  videoId=ids,
                  maxResults=50
                  ).execute()
                  video_info = video_data_request['items']
                  for comment in video_info:
                      comment_info = {
                           'Video_id': comment['snippet']['videoId'],
                            'Comment_Id': comment['snippet']['topLevelComment']['id'],
                            'Comment_Text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            'Comment_Author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'Comment_Published_At': comment['snippet']['topLevelComment']['snippet']['publishedAt'],
                        }
                      comments_data.append(comment_info)
                except HttpError as e:
                    if e.resp.status == 403 and 'disabled comments' in str(e):
                        comment_info = {
                            'Video_id': ids,
                            'Comment_Id': 'comments_disabled',
                             }
                        comments_data.append(comment_info)
                    else:
                      print(f"An error occurred while retrieving comments for video: {ids}")
                      print(f"Error details: {e}")
            return comments_data
        
       
        channel_details = get_channel_details(channel_id)
        A=pd.DataFrame(channel_details)
        st.dataframe(A)
                 
        playlist_Details=get_channel_playlists(channel_id)
        B= pd.DataFrame(playlist_Details)
        st.dataframe(B)
               
        playlist_id =A['playlist_id'][0]
        video_ids=get_video_ids(youtube,playlist_id)
        C=pd.DataFrame(video_ids)
        st.dataframe(C)
                 
        video_details=get_video_details(youtube,video_ids)
        D=pd.DataFrame(video_details)
        D['publishedAt'] = D['publishedAt'].apply(lambda x: parser.parse(x)) 
        D['pushblishDayName'] =D['publishedAt'].apply(lambda x: x.strftime("%A")) 
        D['durationSecs'] = D['duration'].apply(lambda x: isodate.parse_duration(x))
        D['durationSecs'] = D['durationSecs'].astype(str)
        D["tags"]=D['tags'].apply(lambda x:','.join(x) if x is not None else x)

        video_details=D.to_dict("records")
        st.dataframe(D)

        comment_details=get_comment_data(youtube,video_ids)
        E=pd.DataFrame(comment_details)
        E['Comment_Published_At'] = E['Comment_Published_At'].apply(lambda x: parser.parse(x)) 
        comment_details=E.to_dict("records")
        st.dataframe(E)
        
# create a client instance of MongoDB
        client = pymongo.MongoClient('mongodb://localhost:27017/')

        # create a database or use existing one
        mydb = client['Youtube_channels']
        # create a collection
        collection = mydb['Youtube_data']
        # define the data to insert
        data = {'Channel_name' :A["Channel_name"][0],
                          'channel_details' : channel_details,
                          'playlist_details' : playlist_Details,
                           'video_details' :video_details,
                            'comment_details' : comment_details
                       }

        # insert or update data in the collection
        upload = collection.replace_one({'_id': channel_id},data, upsert=True)

        # print the result of the insertion operation
        st.write(f"Updated document id: {upload.upserted_id if upload.upserted_id else upload.modified_count}")

        # Close the connection
        client.close()
        
if option=="Migratedata":
    st.header(':violet[Data Migrate zone]')
    st.write ('''(Note:- This zone specific channel data **Migrate to :blue[MySQL] database from  :green[MongoDB] database** depending on your selection,
                if unavailable your option first collect data.)''')
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = client['Youtube_channels']
    collection = mydb['Youtube_data']
    document_names = []
    for document in collection.find():
        document_names.append(document['Channel_name'])
    document_name = st.selectbox('**Select Channel_name**', options = document_names, key='document_names')
    st.write('''Migrate to MySQL database from MongoDB database to click below **:blue['Migrate to MySQL']**.''')
    Migrate = st.button('**Migrate to MySQL**')
    
     # Define Session state to Migrate to MySQL button
    if 'migrate_sql' not in st.session_state:
        st.session_state_migrate_sql = False
    if Migrate or st.session_state_migrate_sql:
        st.session_state_migrate_sql = True
        result = collection.find_one({"Channel_name": document_name})
        client.close()
        a=pd.DataFrame(result["channel_details"])
        b=pd.DataFrame(result["playlist_details"])
        c=pd.DataFrame(result["video_details"])
        d=pd.DataFrame(result["comment_details"])
        
        #1.creating new database
        myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd="Aradhana@2509")
        cur = myconnection.cursor()
        cur.execute("create database IF NOT EXISTS Youtube_Channels")
        
        #2.creating new table(channel_details,playlist_details,video_details,comment_details)
        myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd="Aradhana@2509",database="Youtube_Channels")
        cur = myconnection.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS channel_collection(Channel_details varchar(225),Playlist_id varchar(225),Subscribers int,Views int,Total_Videos int,Description varchar(5000))")
        cur.execute("CREATE TABLE IF NOT EXISTS playlist_collection(playlist_id varchar(225),playlist_title varchar(1000))")
        cur.execute("CREATE TABLE IF NOT EXISTS  Video_Collection(video_id varchar(225),channelTitle varchar(225),title varchar(225),description varchar(5000),tags varchar(1000),publishedAt datetime,viewCount varchar(10),likeCount varchar(10),dislikeCount varchar(20),favouriteCount varchar(20),commentCount varchar(20),duration varchar(20),definition varchar(20),caption varchar(20),pushblishDayName varchar(20),durationSecs varchar(20))")
        cur.execute("CREATE TABLE IF NOT EXISTS comment_collection(video_id varchar(225),Comment_Id varchar(200),Comment_Text TEXT,Comment_Author varchar(100),Comment_Published_At datetime)")
        
        
        #3.Inserting the values in table
        
        sql="insert into channel_collection(Channel_name,Playlist_id,Subscribers,Views,Total_Videos,Description)values(%s,%s,%s,%s,%s,%s)";
        for i in range(0,len(a)):
           cur.execute(sql,tuple(a.iloc[i]))
           myconnection.commit()
           
        sql1="insert into playlist_collection(playlist_id,playlist_title)values(%s,%s)";
        for i in range(0,len(b)):
           cur.execute(sql1,tuple(b.iloc[i]))
           myconnection.commit()
        
        sql2="insert into Video_Collection(video_id,channelTitle,title,description,tags,publishedAt,viewCount,likeCount,dislikeCount,favouriteCount,commentCount,duration,definition,caption,pushblishDayName,durationSecs)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)";
        for i in range(0,len(c)):
            cur.execute(sql2,tuple(c.iloc[i]))
            myconnection.commit()
            
        sql3="insert into comment_collection(Video_id,Comment_Id,Comment_Text,Comment_Author,Comment_Published_At)values(%s,%s,%s,%s,%s)";
        for i in range(0,len(d)):
            cur.execute(sql3,tuple(d.iloc[i]))
            myconnection.commit()
            
if option == "Querydata":
    st.header(':violet[Channel Data Analysis zone]')
    st.write ('''(Note:- This zone **Analysis of channel data** depends on your question selection and gives a table format output.)''')

        # Check available channel data
    Check_channel = st.checkbox('**Check available channel data for analysis**')

    if Check_channel:
   # Create database connection
        myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd="Aradhana@2509",database="Youtube_Channels")
        cur = myconnection.cursor()
    # Execute SQL query to retrieve channel names
        query = "SELECT channel_name FROM channel_collection;"
        results = pd.read_sql(query,myconnection)
     # Get channel names as a list
        channel_names_fromsql = list(results['channel_name'])
    # Create a DataFrame from the list and reset the index to start from 1
        df_at_sql = pd.DataFrame(channel_names_fromsql, columns=['Available channel data']).reset_index(drop=True)
    # Reset index to start from 1 instead of 0
        df_at_sql.index += 1  
    # Show dataframe
        st.dataframe(df_at_sql)
        option = st.selectbox('**Select your Question**',
           ('1. What are the names of all the videos and their corresponding channels?',
            '2. Which channels have the most number of videos, and how many videos do they have?',
            '3. What are the top 10 most viewed videos and their respective channels?',
            '4. How many comments were made on each video, and what are their corresponding video names?',
            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
            '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
            '8. What are the names of all the channels that have published videos in the year 2022?',
            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
            '10. Which videos have the highest number of comments, and what are their corresponding channel names?'), key = 'collection_question')
        
        if  option == '1. What are the names of all the videos and their corresponding channels?':
            query="SELECT title AS video_title, channelTitle AS channel_name FROM video_collection ORDER BY channelTitle"
            r1= pd.read_sql(query,myconnection)
            df1 = pd.DataFrame(r1, columns=['channel_name', 'video_title']).reset_index(drop=True)
            df1.index += 1
            st.dataframe(df1)
        elif option == '2. Which channels have the most number of videos, and how many videos do they have?':
            query="SELECT Channel_name, Total_Videos FROM channel_collection ORDER BY Total_Videos DESC;"
            r2 = pd.read_sql(query,myconnection)
            df2 = pd.DataFrame(r2,columns=['Channel_name','Total_Videos']).reset_index(drop=True)
            df2.index += 1
            st.dataframe(df2)
            fig_vc = px.bar(df2, y='Total_Videos', x='Channel_name', text_auto='.2s', title="Most number of videos", )
            fig_vc.update_traces(textfont_size=16,marker_color='#E6064A')
            fig_vc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
            st.plotly_chart(fig_vc,use_container_width=True)
        elif option == '3. What are the top 10 most viewed videos and their respective channels?':
             query= "SELECT channelTitle AS Channel_Name,title AS Video_Name,viewCount AS View_count from video_collection ORDER BY ViewCount DESC LIMIT 10;"
             r3 = pd.read_sql(query,myconnection)
             df3 = pd.DataFrame(r3,columns=['Channel_Name', 'Video_Name', 'View_count']).reset_index(drop=True)
             df3.index += 1
             st.dataframe(df3)
             fig_topvc = px.bar(df3, y='View_count', x='Video_Name', text_auto='.2s', title="Top 10 most viewed videos")
             fig_topvc.update_traces(textfont_size=16,marker_color='#E6064A')
             fig_topvc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
             st.plotly_chart(fig_topvc,use_container_width=True)
        elif option == '4. How many comments were made on each video, and what are their corresponding video names?':
             query = "SELECT channelTitle AS Channel_Name,title AS Video_Name,commentCount AS Comment_count FROM video_collection;"
             r4 = pd.read_sql(query,myconnection)
             df4 = pd.DataFrame(r4,columns=['Channel_Name', 'Video_Name', 'Comment_count']).reset_index(drop=True)
             df4.index += 1
             st.dataframe(df4)
        elif option == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
             query = "SELECT channelTitle AS Channel_Name,title AS Video_Name,likeCount AS Like_count FROM video_collection ORDER BY likeCount DESC;"
             r5= pd.read_sql(query,myconnection)
             df5 = pd.DataFrame(r5,columns=['Channel_Name', 'Video_Name', 'Like_count']).reset_index(drop=True)
             df5.index += 1
             st.dataframe(df5)
             fig_vc = px.bar(df5, y='Like_count', x='Video_Name', text_auto='.2s', title="Highest Like Count", )
             fig_vc.update_traces(textfont_size=16,marker_color='#E6064A')
             fig_vc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
             st.plotly_chart(fig_vc,use_container_width=True)
        elif option == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
             st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
             query = "SELECT channeltitle AS Channel_Name, title AS Video_Name, likeCount AS Like_count,dislikeCount AS Dislike_count FROM video_collection ORDER BY likeCount DESC;"
             r6= pd.read_sql(query,myconnection)
             df6 = pd.DataFrame(r6,columns=['Channel_Name', 'Video_Name', 'Like_count','Dislike_count']).reset_index(drop=True)
             df6.index += 1
             st.dataframe(df6)
        elif option == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
             query = "SELECT Channel_name AS Channel_Name,views AS Channel_Views FROM channel_collection ORDER BY views DESC;"
             r7= pd.read_sql(query,myconnection)
             df7 = pd.DataFrame(r7,columns=['Channel_Name', 'Channel_Views']).reset_index(drop=True)
             df7.index += 1
             st.dataframe(df7)
             fig_topview = px.bar(df7, y='Channel_Views', x='Channel_Name', text_auto='.2s', title="Total number of views", )
             fig_topview.update_traces(textfont_size=16,marker_color='#E6064A')
             fig_topview.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
             st.plotly_chart(fig_topview,use_container_width=True)
        elif option == '8. What are the names of all the channels that have published videos in the year 2022?':
             query = "SELECT channelTitle AS Channel_Name, title AS Video_Name,publishedAt AS Year_2022 FROM video_collection WHERE EXTRACT(YEAR FROM publishedAt) = 2022;"
             r8= pd.read_sql(query,myconnection)
             df8 = pd.DataFrame(r8,columns=['Channel_Name','Video_Name', 'Year_2022']).reset_index(drop=True)
             df8.index += 1
             st.dataframe(df8)
        elif option == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
             query = "SELECT Channel_name AS Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.Duration)))), '%H:%i:%s') AS duration  FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id GROUP by Channel_Name ORDER BY duration DESC ;"
             r9= pd.read_sql(query,myconnection)
             df9 = pd.DataFrame(r9,columns=['Channel_Name','Average_duration_of_videos_(HH:MM:SS)']).reset_index(drop=True)
             df9.index += 1
             st.dataframe(df9)
        elif option == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
             query = "SELECT channelTitle AS Channel_Name, title AS Video_Name, commentCount AS Number_of_comments FROM video_collection ORDER BY commentCount DESC;"
             r10= pd.read_sql(query,myconnection)
             df10 = pd.DataFrame(r10,columns=['Channel_Name','Video_Name', 'Number_of_comments']).reset_index(drop=True)
             df10.index += 1
             st.dataframe(df10)
