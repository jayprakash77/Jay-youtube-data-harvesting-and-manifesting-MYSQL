from googleapiclient.discovery import build
import streamlit as st
import numpy as np
import pandas as pd
import json
import mysql.connector
import pprint

#this is the api key we can get it from google api clients
api_key = 'api key'
#connecting our python to mysql using mysql connecter
con = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "password"
)
cursor = con.cursor()
#creating one database to store the data
create_db = "CREATE database if not exists capstone"
show_db = "SHOW databases"
#using that created database 
use_db = "USE capstone"
cursor.execute(use_db)

#calling api using api key to get the details we need from you tube
youtube = build('youtube','v3',developerKey=api_key)

#this function is to get the channel details
def channel_data(youtube,channel_ids):
    data=[]
    channel={}
    #sending the channel ids one by one
    for i,channel_id in enumerate(channel_ids):
        #calling the channels api to get the channel details
        response = youtube.channels().list(
            id=channel_id,
            part='snippet,statistics,contentDetails,status'
        )

        channel_data = response.execute()
        for item in channel_data['items']:
            #storing the data in dictionary format
            channel['channel_'+str(i+1)] = {

                'channel_name' : item['snippet']['title'],
                'channal_Id' : item['id'],
                'Subscription_count': item['statistics']['subscriberCount'],
                'Channel_views': item['statistics']['viewCount'],
                'total_Videos': item['statistics']['videoCount'],
                'channel_description' : item['snippet']['description'],
                'playlists' : item['contentDetails']['relatedPlaylists']['uploads'],
                'status' : item['status']['privacyStatus']
                }
    data.append(channel)
    return data

#this function is to get the playlist details
def playlist_data(youtube,channel_ids):
    All_data=[]
    h=1
    data= {}
    for channel_id in channel_ids:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=25 #single request can get a max of 25 playlists
        )
        response = request.execute()
        for item in response['items']: 
            
            data['Playlist_'+str(h)]={
                'PlaylistId':item['id'],
                'Title':item['snippet']['title'],
                'Channel_Id':item['snippet']['channelId'],
                'channel_name':item['snippet']['channelTitle'],
                'PublishedAt':item['snippet']['publishedAt'],
                'Video_Count':item['contentDetails']['itemCount']
                }
            h= h+1
        # All_data.append(data)
            
            
        next_page_token = response.get('nextPageToken') #to get further next page token is stored in a variable
        
        while next_page_token is not None: #it continues collecting the playlists as long as it is not none

            request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=25)
            response = request.execute()

            for item in response['items']: 
                data['Playlist_'+str(h)]={
                    'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'Channel_Id':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']
                    }
                h = h+1
            next_page_token = response.get('nextPageToken')
    #storing the whole dict into a list
    All_data.append(data)
    return All_data

#this function is to get the video ids of the channel
def get_video_ids(youtube,playlists):
    video_ids = []
    for playlist in playlists:
        request = youtube.playlistItems().list(
                part='snippet,contentDetails,id,status',
                playlistId = playlist,
                maxResults = 50)
        response = request.execute()
        
        # playlist_exe = request.execute()
        
        
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])
        
        next_page_token = response.get('nextPageToken') #to get further next page token is stored in a variable
        more_pages = True

        while more_pages:#it continues collecting the playlists as long as it is true
            
            if next_page_token is None:
                more_pages = False
            else:
                request = youtube.playlistItems().list(
                            part='snippet,contentDetails,id,status',
                            playlistId = playlist,
                            maxResults = 50,
                            pageToken = next_page_token)
                response = request.execute()
                
                for i in range(len(response['items'])):
                    video_ids.append(response['items'][i]['contentDetails']['videoId'])
                
                next_page_token = response.get('nextPageToken')
    return video_ids

#this function is to get the video,comment details of the channel
def video_comment_data(youtube,video_ids):
    master_data = []
    output={}
    # print(video_ids,len(video_ids))
    for v,video_id in enumerate(video_ids):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        video_response = request.execute()
        comment_info=[]
        comment ={}
        #converting the duration P12H33M54S to hours
        duration1 = str(video_response['items'][0]['contentDetails']['duration'])

        duration = np.array(['00','00','00'])

        for d in duration1:
            if d == 'H':
                h =duration1.index(d)
                for a in range(h-2,h-1):
                    if duration1[h-2].isnumeric():
                        hrs = duration1[h-2]+duration1[h-1]
                        duration1[0]=hrs
                    else:
                        duration[0]='0'+duration1[h-1]
            if d =='M':
                m =duration1.index(d)
                for o in range(m-2,m-1):
                    if duration1[m-2].isnumeric():
                        Min = duration1[m-2]+duration1[m-1]
                        duration[1]=Min
                    else:
                        duration[1]='0'+duration1[m-1]
            if d == 'S':
                s =duration1.index(d)
                for b in range(s-2,s-1):
                    if duration1[s-2].isnumeric():
                        sec = duration1[s-2]+duration1[s-1]
                        duration[2]=sec
                    else:
                        duration[2]='0'+duration1[s-1]
            
        dur = duration[0]+':'+duration[1]+':'+duration[2]

        try:   
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id
            )
            comment_response = request.execute()
            j=1
            for item in comment_response['items']:
                
                
                comment['comment_Id_'+str(j)]={
                    "comment_Id" : item['id'],
                    "video_Id" : video_id,
                    "comment_Text" : item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "commentAuthor": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_PublishedAt": item['snippet']['topLevelComment']['snippet']['publishedAt']
                }    
                j = j+1
            # comment_info.append(comment)
            next_page_token1 = comment_response.get('nextPageToken')
            comment_pages = True
            
            while comment_pages:
                
                if next_page_token1 is None:
                    comment_pages = False
                else:
                    
                    request = youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=video_id,
                        pageToken = next_page_token1
                    )
                    comment_response = request.execute()
                    for item in comment_response['items']:
                        # comment = {}
                
                        comment['comment_Id_'+str(j)]={
                            "comment_Id" : item['id'],
                            "video_Id" : video_id,
                            "comment_Text" : item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            "commentAuthor": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            "Comment_PublishedAt": item['snippet']['topLevelComment']['snippet']['publishedAt']
                        }
                        
                        j = j+1
                        
                    # comment_info.append(comment)
                    next_page_token1 = comment_response.get('nextPageToken')
        except Exception as e:
            pass
        comment_info.append(comment)

        output['video_id'+str(v+1)] = {

            "Video_Id": video_id,
            "channel_Id": video_response['items'][0]['snippet']['channelId'],
            "channel_title": video_response['items'][0]['snippet']['channelTitle'],
            "Video_Name": video_response['items'][0]['snippet']['title'],
            "Video_Description": video_response['items'][0]['snippet']['description'],
            "Tags": video_response['items'][0]['etag'],
            "PublishedAt": video_response['items'][0]['snippet']['publishedAt'],
            "View_Count": video_response['items'][0]['statistics']['viewCount'],
            "Like_Count": video_response['items'][0]['statistics']['likeCount'],
            "Dislike_Count": video_response['items'][0]['statistics']['dislikeCount'] if 'dislikeCount' in video_response['items'][0]['statistics'] else "Not Available",
            "Favorite_Count": video_response['items'][0]['statistics']['favoriteCount'],
            "Comment_Count": video_response['items'][0]['statistics']['commentCount'] if 'commentCount' in video_response['items'][0]['statistics'] else '0',
            "Duration": dur,
            "Thumbnail": video_response['items'][0]['snippet']['thumbnails']['default']['url'],
            "caption_status": video_response['items'][0]['contentDetails']['caption'],
            "comments": comment_info,
        }
        # print(output)
    master_data.append(output)
    # print(master_data)
        
    return master_data
    
# creating a channel table in and inserting the values 
def channel_sql():
    try:
        with open (r"channel.json",mode = 'r+') as data :
            item = json.load(data)
        channel =[]
        channel_table1 = """CREATE table if not exists channel(
                                            name varchar(255),
                                            id varchar(255),
                                            type varchar(255),
                                            views int,
                                            videoCount int,
                                            description text,
                                            status varchar(255)
        )"""
        cursor.execute(channel_table1)
        try:
            cursor.execute("ALTER TABLE channel ADD PRIMARY KEY(id)")
        except Exception as e:
            pass
        channel_query = "INSERT into channel VALUES(%s,%s,%s,%s,%s,%s,%s)"
        for s in range(len(item)):
            for i in item[s].keys():
                # print(i)
                row = (str(item[s][i]['channel_name']),str(item[s][i]['channal_Id']),int(item[s][i]['Subscription_count']),int(item[s][i]['Channel_views']),int(item[s][i]['total_Videos']),str(item[s][i]['channel_description']),str(item[s][i]['status']))
                channel.append(row)
        cursor.executemany(channel_query,channel)
        con.commit()
    except Exception as e:
        st.write('Data is already existing on channel table')
    
# creating a playlist table in and inserting the values 
def playlist_sql():
    try:
        with open (r"playlist.json",mode = 'r+') as data :
            item = json.load(data)
        playlist = []
        playlist_table1 = """CREATE table if not exists playlist(
                                            id varchar(255),
                                            channel_id varchar(255),
                                            name varchar(255)
        )"""
        cursor.execute(playlist_table1)
        try:
            cursor.execute("ALTER TABLE playlist ADD PRIMARY KEY(id)")
        except Exception as e:
            pass
        playlist_query = "INSERT into playlist VALUES(%s,%s,%s)"
        for s in range(len(item)):
            for i in item[s].keys():
                # print(i)
                row = (str(item[s][i]['PlaylistId']),str(item[s][i]['Channel_Id']),str(item[s][i]['Title']))
                playlist.append(row)
        cursor.executemany(playlist_query,playlist)
        con.commit()
    except Exception as e:
        st.write('Data is already existing on playlist table')

# creating a video table in and inserting the values
def video_sql():
    try:
        with open (r"video_com.json",mode = 'r+') as data :
            item = json.load(data)
        video = []
        video_table1 = """CREATE table if not exists video(
                                            id varchar(255),
                                            channel_id varchar(255),
                                            channel_title varchar(255),
                                            video_name varchar(255),
                                            description text,
                                            published_data datetime,
                                            view_count int,
                                            like_count int,
                                            favorite_count int,
                                            comment_count int,
                                            duration int,
                                            thumbnail varchar(255),
                                            caption_status varchar(255)
        )"""
        cursor.execute(video_table1)
        try:
            cursor.execute("ALTER TABLE video ADD PRIMARY KEY(id)")
        except Exception as e:
            pass
        video_query = "INSERT into video VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        for s in range(len(item)):
            for i in item[s].keys():
                duration = item[s][i]['Duration']
                splitted_dur = duration.split(':')
                new_duration =int(splitted_dur[0]) * 60 * 60 + int(splitted_dur[1]) * 60 + int(splitted_dur[2])
                # print(new_duration)
                timestamp = item[s][i]['PublishedAt']
                for character in 'TZ':
                    timestamp = timestamp.replace(character, ' ')
                # print(timestamp)
                row = (str(item[s][i]['Video_Id']),str(item[s][i]['channel_Id']),str(item[s][i]['channel_title']),str(item[s][i]['Video_Name']),str(item[s][i]['Video_Description']),str(timestamp),int(item[s][i]['View_Count']),int(item[s][i]['Like_Count']),int(item[s][i]['Favorite_Count']),int(item[s][i]['Comment_Count']),int(new_duration),str(item[s][i]['Thumbnail']),str(item[s][i]['caption_status']))
                video.append(row)
        cursor.executemany(video_query,video)
        con.commit()

    except Exception as e:
        st.write('Data is already existing on video table')

# creating a comment table in and inserting the values
def comment_sql():
    try:
        with open (r"video_com.json",mode = 'r+') as data :
            item = json.load(data)
        comment = []
        comment_table1 = """CREATE table if not exists comment(
                                            id varchar(255),
                                            video_id varchar(255),
                                            text text,
                                            author varchar(255),
                                            published_data datetime
        )"""
        cursor.execute(comment_table1)
        try:
            cursor.execute("ALTER TABLE comment ADD PRIMARY KEY(id)")
        except Exception as e:
            pass
        comment_query = "INSERT into comment VALUES(%s,%s,%s,%s,%s)"
        for s in range(len(item)):
            for i in item[s].keys():
                com =item[s][i]['comments']
                for d in range(len(com)):
                    for m in com[d].keys():
                        timestamp = com[d][m]['Comment_PublishedAt']
                        # print(com[s][m]['comment_Id'])
                        for character in 'TZ':
                            timestamp = timestamp.replace(character, ' ')
                        row = (str(com[d][m]['comment_Id']),str(com[d][m]['video_Id']),str(com[d][m]['comment_Text']),str(com[d][m]['commentAuthor']),str(timestamp))
                        comment.append(row)
        cursor.executemany(comment_query,comment)
        con.commit()

    except Exception as e:
        st.write('Data is already existing on comment table')

# this is the main function where all the other functions where called
def stored_data(channel_ids):
    data_channel = channel_data(youtube,channel_ids)
    # pprint.pprint(data_channel,compact=True)
    #storing the data in json file 
    with open (r"channel.json",mode = 'w+') as data :
        json.dump(data_channel,data,indent =4,separators=(',',':'))
    Playlist = []
    for s in range(len(data_channel)):
        for i in data_channel[s].keys():
        # print(i)
            Playlist.append(data_channel[s][i]['playlists'])
    data_playlist = playlist_data(youtube,channel_ids)
    # pprint.pprint(data_playlist,compact=True)
    with open (r"playlist.json",mode = 'w+') as data :
        json.dump(data_playlist,data,indent =4,separators=(',',':'))
    video_ids = get_video_ids(youtube,Playlist)
    video_comment = video_comment_data(youtube,video_ids)
    with open (r"video_com.json",mode = 'w+') as data :
        json.dump(video_comment,data,indent =4,separators=(',',':'))
    # pprint.pprint(video_comment,compact=True)
    return "Data retriving and storing has been completed"

def tables():
    channel_sql()
    playlist_sql()
    video_sql()
    comment_sql()

    return 'Migration done'

#selecting the table of channel
def display_channels():  
    cursor.execute("SELECT * FROM channel")
    tableofchannels=cursor.fetchall()
    tableofchannels=pd.DataFrame(tableofchannels)
    tableofchannels=st.dataframe(tableofchannels)
    return tableofchannels

#selecting the table of playlist
def display_playlists():
    cursor.execute("SELECT * FROM playlist")
    tableofplaylists=cursor.fetchall()
    tableofplaylists=pd.DataFrame(tableofplaylists)
    tableofplaylists=st.dataframe(tableofplaylists)
    return tableofplaylists

#selecting the table of video
def display_videos():
    cursor.execute("SELECT * FROM video")
    tableofvideos=cursor.fetchall()
    tableofvideos=pd.DataFrame(tableofvideos)
    tableofvideos=st.dataframe(tableofvideos)
    return tableofvideos

# selecting the table of comments
def display_comments():
    cursor.execute("SELECT * FROM comment")
    tableofcomments=cursor.fetchall()
    tableofcomments=pd.DataFrame(tableofcomments)
    tableofcomments=st.dataframe(tableofcomments)
    return tableofcomments

# writing query to find the required data
def oneQuery():
    try:
        cursor.execute("SELECT video_name,channel_title from video")
        output = cursor.fetchall()
        output = pd.DataFrame(output)
        output = st.dataframe(output)
        return output
    except Exception as e:
        pass

def twoQuery():
    try:
        cursor.execute("SELECT name,videoCount from channel ORDER BY videoCount desc LIMIT 1")
        output = cursor.fetchall()
        # output = pd.DataFrame(output)
        output = st.dataframe(output)
        return output
    except Exception as e:
        pass

def threeQuery():
    try:
        cursor.execute("SELECT view_count,channel_title from video ORDER BY view_count desc LIMIT 10")
        output = cursor.fetchall()
        # output = pd.DataFrame(output)
        output = st.dataframe(output)
        return output
    except Exception as e:
        pass

def fourQuery():
    try:
        cursor.execute("SELECT comment_count,video_name from video")
        output = cursor.fetchall()
        # output = pd.DataFrame(output)
        output = st.dataframe(output)
        return output
    except Exception as e:
        pass

def fiveQuery():
    try:
        cursor.execute("SELECT video_name,channel_title,like_count from video ORDER BY like_count desc")
        output = cursor.fetchall()
        # output = pd.DataFrame(output)
        output = st.dataframe(output)
        return output
    except Exception as e:
        pass

def sixQuery():
    try:
        cursor.execute("SELECT like_count,video_name from video ")
        output = cursor.fetchall()
        # output = pd.DataFrame(output)
        output = st.dataframe(output)
        return output
    except Exception as e:
        pass

def sevenQuery():
    try:
        cursor.execute("SELECT views,name from channel ")
        output = cursor.fetchall()
        # output = pd.DataFrame(output)
        output = st.dataframe(output)
        return output
    except Exception as e:
        pass

def eightQuery():
    # try:
        cursor.execute("SELECT video_name,published_data,channel_title from video WHERE extract(year from published_data) = 2022")
        output = cursor.fetchall()
        # output = pd.DataFrame(output)
        output = st.dataframe(output)
        return output
    # except Exception as e:
    #     pass

def nineQuery():
    try:
        cursor.execute("SELECT AVG(duration),channel_title from video GROUP BY channel_title")
        output = cursor.fetchall()
        # output = pd.DataFrame(output)
        output = st.dataframe(output)
        return output
    except Exception as e:
        pass

def tenQuery():
    try:
        cursor.execute("SELECT comment_count,channel_title from video ORDER BY comment_count desc")
        output = cursor.fetchall()
        # output = pd.DataFrame(output)
        output = st.dataframe(output)
        return output
    except Exception as e:
        pass

Onakennapaa = 'UC0nNpfv2PxWaoqB5OLPY4LQ'
Prushdeva ='UCCjnklm2IdEIUfk82gsgFkA'
Life_of_sangu = 'UCzIKyAZ7H2HkG8i5sdqlhiw'
techsanthosh = 'UCDfWfRdXkouQvLNtTi9N8Jw'
FilmyFam = 'UC2KAEGTZRKHlkDIS6y3TxiQ'
NareshKumar = 'UCvdNgLfzQiNmJXuzcYCI3Kg'
gb ='UCWxrfGAvLWRKLzM39b8ZMLw'
PaathutuPonga = 'UC8pvWvshxrRI6rVGZMgraxA'
editz = 'UCEfMfUGqivZCF6THK7E3wFg'
agFlex = 'UCrzqhNOyQswNqsXu1VCfaPA'

with st.sidebar:
    st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB (Atlas) and SQL")

options = st.multiselect(
    'select the channel here',
    [Onakennapaa,Prushdeva,Life_of_sangu,techsanthosh,FilmyFam,NareshKumar,gb,PaathutuPonga,editz,agFlex],
    [])

# st.write('You have Selected:', options)

if st.button("Collect and Store data"):    
    stored_data(options)
    # for i in options:
    #     # print(i)
    #     output=stored_data(i)
    #     st.write(output)

st.write("Click here to Migrate the data in sql tables")        
if st.button("Migrate"):
    display=tables()
    st.write(display)

frames = st.radio(
     "Select the table you want to view",
    ('None','Channel', 'Playlist', 'Video', 'Comment'))

st.write('You selected:', frames)

if frames=='None':
    st.write("select a table")
elif frames=='Channel':
    display_channels()
elif frames=='Playlist':
    display_playlists()
elif frames=='Video':
    display_videos()
elif frames=='Comment':
    display_comments()

query = st.selectbox(
    'let us do some analysis',
    ('None','All the videos and the Channel Name', 'Channels with most number of videos', '10 most viewed videos',
     'Comments in each video','Videos with highest likes', 'likes of all videos', 'views of each channel',
     'videos published in the year 2022','average duration of all videos', 'videos with highest number of comments'))

if query=='None':
    st.write("you selected None")
elif query=='All the videos and the Channel Name':
    oneQuery()
elif query=='Channels with most number of videos':
    twoQuery()
elif query=='10 most viewed videos':
    threeQuery()
elif query=='Comments in each video':
    fourQuery()
elif query=='Videos with highest likes':
    fiveQuery()
elif query=='likes of all videos':
    sixQuery()
elif query=='views of each channel':
    sevenQuery()
elif query=='videos published in the year 2022':
    eightQuery()
elif query=='average duration of all videos':
    nineQuery()
elif query=='videos with highest number of comments':
    tenQuery()