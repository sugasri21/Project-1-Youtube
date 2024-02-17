from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt


#API key connection
def Api_connect():
    Api_Id="AIzaSyAFozrb1_Tv5kcwLWMhHrcTkWAzypx_mOc"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()


#get channel information
def get_channel_info(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response1=request.execute()

    for i in range(0,len(response1["items"])):
        
        data = dict(
                    Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Channel_Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data
    


#get playlist ids
def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data



#get video ids
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


#get video information
def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Video_Description = item['snippet']['description'],
                        PublishedAt = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        View_Count = item['statistics']['viewCount'],
                        Like_Count = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']             
                        )
            video_data.append(data)
    return video_data



#get comment information
def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published_date = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information



#MongoDB Connection
client = pymongo.MongoClient("mongodb+srv://sugasri:suga1921@cluster0.5mzxouy.mongodb.net/?retryWrites=true&w=majority")
db = client["YouTubeDataDetails"]


# upload to MongoDB

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db["ChannelsDetails"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,
                     "comment_information":com_details})
    
    return "upload completed successfully"



#Table creation for channels,playlists, videos, comments
#Channels
def channels_table():
    mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database= "YouTubeData"
            )
    cursor = mydb.cursor()
    print(cursor)

  
    drop_query = "drop table if exists Channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists Channels(Channel_Name varchar(255),
                        Channel_Id varchar(255) primary key, 
                        Subscription_Count int, 
                        Channel_Views int,
                        Total_Videos int,
                        Channel_Description text,
                        Playlist_Id varchar(50))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Channels Table alredy created") 

    
    ch_list = []
    db = client["YouTubeDataDetails"]
    coll1 = db["ChannelsDetails"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query = '''INSERT into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscription_Count,
                                                    Channel_Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''
            

        values =(
                row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:                     
            cursor.execute(insert_query,values)
            mydb.commit()
            
        except:
            st.write("Channels values are already inserted")



#playlist

def playlists_table():
    mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database= "YouTubeData"
            )
    cursor = mydb.cursor()

    drop_query = "drop table if exists Playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists Playlists(Playlist_Id varchar(255) primary key,
                        Playlist_Name varchar(255), 
                        Channel_Id varchar(255), 
                        Channel_Name varchar(255),
                        PublishedAt timestamp,
                        VideoCount int
                        )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Playlists Table alredy created")    


    db = client["YouTubeDataDetails"]
    coll1 =db["ChannelsDetails"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    df = pd.DataFrame(pl_list)
    
    for index,row in df.iterrows():
        insert_query = '''INSERT into playlists(Playlist_Id,
                                                    Playlist_Name,
                                                    Channel_Id,
                                                    Channel_Name,
                                                    PublishedAt,
                                                    VideoCount)
                                        VALUES(%s,%s,%s,%s,%s,%s)'''            
        values =(
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount'])
                
        try:                     
            cursor.execute(insert_query,values)
            mydb.commit()    
        except:
            st.write("Playlists values are already inserted")


def videos_table():

    mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database= "YouTubeData"
            )
    cursor = mydb.cursor()

    drop_query = "drop table if exists Videos"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists Videos(
                        Channel_Name varchar(255),
                        Channel_Id varchar(255),
                        Video_Id varchar(255) primary key, 
                        Video_Name varchar(255), 
                        Tags text,
                        Thumbnail varchar(255),
                        Video_Description text, 
                        Published_Date timestamp,
                        Duration int, 
                        View_Count int, 
                        Like_Count int,
                        Comment_Count int,
                        Favorite_Count int, 
                        Definition varchar(255), 
                        Caption_Status varchar(255)
                        )''' 
                        
        cursor.execute(create_query)             
        mydb.commit()
    except:
        st.write("Videos Table alrady created")
    
    vi_list = []
    db = client["YouTubeDataDetails"]
    coll1 =db["ChannelsDetails"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
   
    for index, row in df2.iterrows():   
        insert_query = '''
                    INSERT INTO Videos (Channel_Name,
                        Channel_Id,
                        Video_Id, 
                        Video_Name, 
                        Tags,
                        Thumbnail,
                        Video_Description, 
                        Published_Date,
                        Duration, 
                        View_Count, 
                        Like_Count,
                        Comment_Count,
                        Favorite_Count, 
                        Definition, 
                        Caption_Status 
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                '''
        values = tuple(str(value) for value in(
                    row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Video_Description'],
                    row['PublishedAt'],
                    row['Duration'],
                    row['View_Count'],
                    row['Like_Count'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status']))
    try:
        cursor.execute(insert_query, values)
        mydb.commit()
    except:
        st.write("Videos Table alrady created")
                              


#comment detail
def comments_table():
    
    mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database= "YouTubeData"
            )
    cursor = mydb.cursor()

    drop_query = "drop table if exists Comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists Comments(Comment_Id varchar(255) primary key,
                       Video_Id varchar(255),
                       Comment_Text text, 
                       Comment_Author varchar(255),
                       Comment_Published timestamp)'''
        cursor.execute(create_query)
        mydb.commit()
        
    except:
        st.write("Commentsp Table already created")

    com_list = []
    db = client["YouTubeDataDetails"]
    coll1 =db["ChannelsDetails"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)


    for index, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (Comment_Id,
                                      Video_Id ,
                                      Comment_Text,
                                      Comment_Author,
                                      Comment_Published)
                VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published_date']
            )
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
               st.write("This comments are already exist in comments table")


def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"


def show_channels_table():
    ch_list = []
    db = client["YouTubeDataDetails"]
    coll1 = db["ChannelsDetails"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table

def show_playlists_table():
    db = client["YouTubeDataDetails"]
    coll1 =db["ChannelsDetails"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    playlists_table = st.dataframe(pl_list)
    return playlists_table

def show_videos_table():
    vi_list = []
    db = client["YouTubeDataDetails"]
    coll2 = db["ChannelsDetails"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table

def show_comments_table():
    com_list = []
    db = client["YouTubeDataDetails"]
    coll3 = db["ChannelsDetails"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table



with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    
channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["YouTubeDataDetails"]
        coll1 = db["ChannelsDetails"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel)
            st.success(output)
            
if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    show_channels_table()
elif show_table == ":orange[playlists]":
    show_playlists_table()
elif show_table ==":red[videos]":
    show_videos_table()
elif show_table == ":blue[comments]":
    show_comments_table()

#SQL connection
mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database= "YouTubeData"
            )
cursor = mydb.cursor()
    
question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))


if question == '1. All the videos and the Channel Name':
    query1 = "select Video_Name as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1) 
    t1=cursor.fetchall()
    mydb.commit()
    data = pd.DataFrame(t1, columns=["Video Title","Channel Name"])
    st.write(data)

elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    t2=cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. The top 10 most viewed videos':
    query3 = '''select View_Count as views , Channel_Name as ChannelName,Video_Name as VideoTitle from videos 
                        where View_Count is not null order by View_Count desc limit 10;'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. Comments in each video':
    query4 = "select Comment_Count as No_comments ,Video_Name as VideoTitle from videos where Comment_Count is not null;"
    cursor.execute(query4)
    t4=cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Videos with highest likes':
    query5 = '''select Video_Name as VideoTitle, Channel_Name as ChannelName, Like_Count as LikesCount from videos 
                       where Like_Count is not null order by Like_Count desc;'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. likes of all videos':
    query6 = '''select Like_Count as likeCount,Video_Name as VideoTitle from videos;'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7. views of each channel':
    query7 = "select Channel_Name as ChannelName, Channel_Views as Channelviews from channels;"
    cursor.execute(query7)
    t7=cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. videos published in the year 2022':
    query8 = '''select Video_Name as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    t8=cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    t9=cursor.fetchall()
    mydb.commit()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. videos with highest number of comments':
    query10 = '''select Video_Name as VideoTitle, Channel_Name as ChannelName, Comment_Count as Comments from videos 
                       where Comment_Count is not null order by Comment_Count desc;'''
    cursor.execute(query10)
    t10=cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))




