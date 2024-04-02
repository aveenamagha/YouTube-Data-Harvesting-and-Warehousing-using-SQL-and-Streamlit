from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import streamlit as st
from datetime import datetime
from isodate import parse_duration

def get_channel_ids(channel_id):
  youtube=build('youtube','v3',developerKey='AIzaSyDSqH9RXJ87XN9_sdnRsPoGw_8hF_11q7M')
  request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
  response = request.execute()
  for i in response['items']:
    details=dict(channel_name=i['snippet']['title'],
        channel_ID=channel_id,
        channel_dis=i['snippet']['description'],
        channel_joined=i['snippet']['publishedAt'],
        channel_sc=i['statistics']['subscriberCount'],
        channel_vc=i['statistics']['videoCount'],
        channel_views=i['statistics']['viewCount'],
        overallplaylistid=i['contentDetails']['relatedPlaylists']['uploads'])
  return details

def get_video_ids(channel_id):
    video_ids=[]
    youtube=build('youtube','v3',developerKey='AIzaSyDSqH9RXJ87XN9_sdnRsPoGw_8hF_11q7M')
    response = youtube.channels().list(
         part='contentDetails',
         id=channel_id).execute()
    overallplaylistid=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    response_1 = youtube.playlistItems().list(
         part='snippet',
         playlistId=overallplaylistid,
         maxResults=50,
         pageToken=next_page_token).execute()

    while True:
        response_1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=overallplaylistid,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
            
        for i in range(len(response_1['items'])):
            video_ids.append(response_1['items'][i]['snippet']['resourceId']['videoId'])
            
        next_page_token=response_1.get('nextPageToken')
    
        if next_page_token is None:
            break
    return video_ids

def get_video_details(video_ids):
    video_details=[]
    for video_id in video_ids:
        youtube=build('youtube','v3',developerKey='AIzaSyDSqH9RXJ87XN9_sdnRsPoGw_8hF_11q7M')
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()
    
        for i in response['items']:
            details=dict(channel_name=i['snippet']['channelTitle'],
                         channel_id=i['snippet']['channelId'],
                         Video_Id=i['id'],
                         Title=i['snippet']['title'],
                         Tags=i['snippet'].get('tag'),
                         Thumbnails=i['snippet']['thumbnails']['default']['url'],
                         Description=i['snippet'].get('description'),
                         Publish_Date=i['snippet']['publishedAt'],
                         Duration=i['contentDetails']['duration'],
                         Views=i['statistics'].get('viewCount'),
                         Likes=i['statistics'].get('likeCount'),
                         Comments=i['statistics'].get('commentCount'),
                         Favorite_count=i['statistics']['favoriteCount'],
                         Definition=i['contentDetails']['definition'],
                         Caption_status=i['contentDetails']['caption']
                         )
            video_details.append(details)
    return video_details

def get_comment_details(video_ids):
    Comment_details=[]
    try:
        for video_id in video_ids:
            youtube=build('youtube','v3',developerKey='AIzaSyDSqH9RXJ87XN9_sdnRsPoGw_8hF_11q7M')
            request=youtube.commentThreads().list(
                part='snippet',
                videoId='UaMyVxy3Os4',
                maxResults=50
            )
            response=request.execute()
            for i in response['items']:
                details=dict(Comment_id=i['snippet']['topLevelComment']['id'],
                        Video_Id=i['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_test=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_published=i['snippet']['topLevelComment']['snippet']['publishedAt'],
                             )
                Comment_details.append(details)
    except:
        pass
    return Comment_details

def get_playlist_details(channel_id):
    nextpagetoken=None
    all_details=[]
    while True:
        youtube=build('youtube','v3',developerKey='AIzaSyDSqH9RXJ87XN9_sdnRsPoGw_8hF_11q7M')
        request=youtube.playlists().list(
                part="snippet,contentDetails",
                channelId='UC84whx2xxsiA1gXHXXqKGOA',
                maxResults=50,
                pageToken=nextpagetoken
        )
        response=request.execute()

        for item in response['items']:
                details=dict(playlist_id=item['id'],
                             Title=item['snippet']['title'],
                             channel_id=item['snippet']['channelId'],
                             channel_name=item['snippet']['channelTitle'],
                             publishedAt=item['snippet']['publishedAt'],
                             video_count=item['contentDetails']['itemCount'])
                all_details.append(details)             
        nextpagetoken=response.get('nextPageToken')
        if nextpagetoken is None:
                break
    return all_details

client=pymongo.MongoClient('mongodb://127.0.0.1:27017/')
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_ids(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_details(vi_ids)
    com_details=get_comment_details(vi_ids)

    coll1=db['channel_details']
    coll1.insert_one({'channel_information':ch_details,'playlist_information':pl_details,
                      'video_information':vi_details,'comment_information':com_details})
    return 'upload successfully'

def channels_table():
    mysql= pymysql.connect(host = '127.0.0.1',
                        user='root',
                        passwd='Aa@11234',
                        port=3306)
    cursor=mysql.cursor()
    cursor.execute("create database if not exists youtube_data")
    cursor.execute('use youtube_data')


    cursor.execute('drop table if exists channels')

    query='''create table if not exists channels (  Channel_Name varchar(100),
                                                    Channel_Id varchar(80) primary key,
                                                    Subscribers bigint,
                                                    Views bigint,
                                                    Total_Videos int,
                                                    Channel_Description text,
                                                    Playlist_Id varchar(80))'''

    cursor.execute(query)
    mysql.commit()




    ch_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(data['channel_information'])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id ,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                                        
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],
                row['channel_ID'],
                row['channel_sc'],
                row['channel_views'],
                row['channel_vc'],
                row['channel_dis'],
                row['overallplaylistid'])
        try:
            cursor.execute(insert_query,values)
            mysql.commit()
        except:
            print('channel values inserted')
        

def playlists_table():
    mysql= pymysql.connect(host = '127.0.0.1',
                        user='root',
                        passwd='Aa@11234',
                        port=3306)
    cursor=mysql.cursor()
    cursor.execute('use youtube_data')
    cursor.execute('drop table if exists playlists')
    mysql.commit()

    cursor.execute("create database if not exists youtube_data")
    cursor.execute('use youtube_data')
    query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                Title varchar(100),
                                                Channel_Id varchar(100),
                                                Channel_Name varchar(100),
                                                PublishedAt timestamp,
                                                Video_Count int )'''
    cursor.execute(query)
    mysql.commit()

    pl_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id ,
                                                Title,
                                                Channel_Id ,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count )
                                                        
                                            values(%s,%s,%s,%s,%s,%s)'''
        values=(row['playlist_id'],
                row['Title'],
                row['channel_id'],
                row['channel_name'],
                row['publishedAt'],
                row['video_count'])
    
        cursor.execute(insert_query,values)
        mysql.commit()

def videos_table():
    mysql= pymysql.connect(host = '127.0.0.1',
                            user='root',
                            passwd='Aa@11234',
                            port=3306)
    cursor=mysql.cursor()
    cursor.execute('use youtube_data')
    cursor.execute('drop table if exists videos')
    mysql.commit()

    cursor.execute("create database if not exists youtube_data")
    cursor.execute('use youtube_data')
    query='''create table if not exists videos (Channel_Name varchar(100),
                                                Channel_Id varchar(100),
                                                Video_Id varchar(30) primary key,
                                                Title varchar(150),
                                                Tags text,
                                                Thambnails varchar(200),
                                                Description text,
                                                Publish_Date timestamp,
                                                Duration int,
                                                Views bigint,
                                                Likes bigint,
                                                Comments int,
                                                Favorite_Count int,
                                                Definition varchar(10),
                                                Caption_Status varchar(50)
                                                )'''
    cursor.execute(query)
    mysql.commit()


    vi_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=pd.DataFrame(vi_list)


    for index,row in df2.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thambnails,
                                            Description,
                                            Publish_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_Count,
                                            Definition,
                                            Caption_Status)

                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        duration_seconds = int(parse_duration(row['Duration']).total_seconds())
        values=(row['channel_name'],
                row['channel_id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnails'],
                row['Description'],
                datetime.strptime(row['Publish_Date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S'),
                duration_seconds,
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_count'],
                row['Definition'],
                row['Caption_status'])

        cursor.execute(insert_query,values)
        mysql.commit()


def comments_table():
    mysql= pymysql.connect(host = '127.0.0.1',
                        user='root',
                        passwd='Aa@11234',
                        port=3306)
    cursor=mysql.cursor()
    cursor.execute('use youtube_data')
    cursor.execute('drop table if exists comments')
    mysql.commit()

    cursor.execute("create database if not exists youtube_data")
    cursor.execute('use youtube_data')
    query='''create table if not exists comments (Comment_Id varchar(100),
                                                    Video_Id varchar(50),
                                                    Comment_Test text,
                                                    Comment_Author varchar(150),
                                                    Comment_Published timestamp)'''

    cursor.execute(query)
    mysql.commit()

    com_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for com_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
        insert_query='''insert into comments(Comment_Id,
                                            Video_Id,
                                            Comment_Test,
                                            Comment_Author,
                                            Comment_Published)


                                            values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_id'],
                row['Video_Id'],
                row['Comment_test'],
                row['Comment_author'],
                datetime.strptime(row['Comment_published'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S'))
                
        cursor.execute(insert_query,values)
        mysql.commit()

def Tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()

    return 'tables created'

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(data['channel_information'])
    df=st.dataframe(ch_list)

    return df

def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for com_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df3=st.dataframe(com_list)

    return df3

with st.sidebar:
    st.title(":blue[Youtube Data Haversting And Warehousing]")
    st.header('SKILLS TAKE AWAY')
    st.caption('python scripting')
    st.caption('Data Collection')
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data management")

channel_id=st.text_input('Enter the channel id')
if st.button('collect and store data'):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_ids.append(data['channel_information']['channel_ID'])
    if channel_id in ch_ids:
        st.success('channel details of the given channel_id already exists ')
    else:
        insert=channel_details(channel_id)

if st.button("Migrate to SQL"):
    Table=Tables()
    st.success(Table)

show_table=st.radio('select the table',('CHANNELS','PLAYLISTS','VIDEOS','COMMENTS'))
if show_table=='CHANNELS':
    show_channels_table()

elif show_table=='PLAYLISTS':
    show_playlists_table()

elif show_table=='VIDEOS':
    show_videos_table()
    
elif show_table=='COMMENTS':
    show_comments_table()


mysql= pymysql.connect(host = '127.0.0.1',
                user='root',
                passwd='Aa@11234',
                port=3306)
cursor=mysql.cursor()
cursor.execute('use youtube_data')

question=st.selectbox("select your question",("1.What are the names of all the videos and their corresponding channels?",
                                              "2.Which channels have the most number of videos, and how many videos do they have?",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video, and what are their corresponding video names?",
                                              "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6.What is the total number of likes for each video, and what are their corresponding video names?",
                                              "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8.What are the names of all the channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

mysql= pymysql.connect(host = '127.0.0.1',
                user='root',
                passwd='Aa@11234',
                port=3306)
cursor=mysql.cursor()
cursor.execute('use youtube_data')

if question=="1.What are the names of all the videos and their corresponding channels?":
    q1='''select title as videos,channel_name as channel from videos'''
    cursor.execute(q1)
    mysql.commit()
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=['VIDEO TITLE','CHANNEL NAME'])
    st.write(df1)

elif question=="2.Which channels have the most number of videos, and how many videos do they have?":
    q2='select channel_name as channelname,total_videos as num_videos from channels order by total_videos desc'
    cursor.execute(q2)
    mysql.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=['CHANNEL NAME','NO OF VIDEOS'])
    st.write(df2)


elif question=="3.What are the top 10 most viewed videos and their respective channels?":
    q3='''select views as views,channel_name as channelname,title as videotitle from videos 
          where views is not null order by views desc limit 10'''
    cursor.execute(q3)
    mysql.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=['VIEWS','CHANNEL NAME',' VIDEO TITLE'])
    st.write(df3)

elif question=="4.How many comments were made on each video, and what are their corresponding video names?":
    q4='''select comments as num_comments,title as video_title from videos where comments is not null '''
    cursor.execute(q4)
    mysql.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=['NO OF COMMENTS','VIDEO TITLE'])
    st.write(df4)

elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    q5='''select title as video_title,channel_name as channelname,likes as likecount from videos 
          where likes is not null order by likes desc'''
    cursor.execute(q5)
    mysql.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=['VIDEO TITLE','CHANNEL NAME','LIKECOUNT'])
    st.write(df5)

elif question=="6.What is the total number of likes for each video, and what are their corresponding video names?":
    q6='''select likes as likescount,title as video_title from videos '''
    cursor.execute(q6)
    mysql.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=['LIKE COUNT','VIDEO TITLE'])
    st.write(df6)

elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    q7='''select channel_name as channelname,views as totalviews from channels '''
    cursor.execute(q7)
    mysql.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=['CHANNEL NAME','TOTAL VIEWS'])
    st.write(df7)

elif question=="8.What are the names of all the channels that have published videos in the year 2022?":
    q8='''select title as videotitle,Publish_Date as videorelease,channel_name as channelname from videos where 
    extract(year from Publish_Date)=2022'''
    cursor.execute(q8)
    mysql.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=['VIDEO TITLE','PUBLISHED DATE','CHANNEL NAME'])
    st.write(df8)

elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    q9='''select channel_name as channelname,avg(duration) as avgduration from videos group by channel_name'''
 
    cursor.execute(q9)
    mysql.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=['CHANNEL NAME','AVG DURATON'])
    
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row['CHANNEL NAME']
        avg_duration=row['AVG DURATON']
        T9.append(dict(channeltitle=channel_title,averageduration=str(avg_duration)))

    df=pd.DataFrame(T9)
    st.write(df)

elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    q10='''select title as videotitle ,channel_name as channelname,comments as comments from videos where comments is 
    not null order by comments desc'''
 
    cursor.execute(q10)
    mysql.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=['VIDEO TITLE','CHANNEL NAME','COMMENTS'])
    st.write(df10)

    


