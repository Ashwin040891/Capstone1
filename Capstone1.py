#Import YouTube api
from googleapiclient.discovery import build
#Import MongoDB
import pymongo
#Import MySQL
import mysql.connector
#Import Streamlit
import streamlit as st
#Import Regex
import re

#api_key = 'AIzaSyAweP72v8kS88EzfJCt50bcEfDr26AaTmI'
#Build YouTube api request key
api_key = 'AIzaSyDW-Gol2dgnnztJRRSUsfl_tsahInfMfeA'
api_service_name = "youtube"
api_version = "v3"
youtube = build(
    api_service_name, api_version, developerKey=api_key
)

#Build connection to MongoDB
connection=pymongo.MongoClient('mongodb://127.0.0.1:27017/')
db=connection['Capstone2']
coll=db['channel']

#Build connection to MySQL
connection=mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345678',
    database='D102'
)
cursor=connection.cursor()

#This function will fetch Youtube details(channel, video and comment details) for the input channel_id
#and insert them into MongoDB
def get_channel_data(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    
    if "items" not in response or len(response["items"]) == 0:
        st.error("Invalid channel ID. Please enter a valid channel ID.")
        return None

    #Fetching channel data
    channel_data={
        "Channel_Name": response['items'][0]['snippet']['title'],
        "Channel_Id": channel_id,
        "Subscription_Count": response['items'][0]['statistics']['subscriberCount'],
        "Channel_Views": response['items'][0]['statistics']['viewCount'],
        "Channel_Description": response['items'][0]['snippet']['description'],
        "Playlist_Id":response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    }

    #save playlist ID for fetching video IDs
    playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    #Fetch all video IDs for the playlist ID
    video_ids = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()
    
        for item in response["items"]:
            video_ids.append(item["contentDetails"]["videoId"])
    
        next_page_token = response.get("nextPageToken")
    
        if not next_page_token:
            break

    #Fetch video data for all the video IDs
    video_data = []

    for i in range(0,len(video_ids),50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
            )
        response = request.execute()
        
        for video in response["items"]:
            snippet = video.get("snippet", {})
            statistics = video.get("statistics", {})
            content_details = video.get("contentDetails", {})
        
            video_details = {
                "Video_Id": video["id"],
                "Video_Name": snippet['title'],
                "Video_Description": snippet['description'],
                "Tags": snippet.get('tags', []),
                "PublishedAt": snippet['publishedAt'],
                "View_Count": statistics.get('viewCount', 0),
                "Like_Count": statistics.get('likeCount', 0),
                "Dislike_Count": statistics.get('dislikeCount', 0),
                "Favorite_Count": statistics.get('favoriteCount', 0),
                "Comment_Count": statistics.get('commentCount', 0),
                "Duration": content_details.get("duration", ""),
                "Thumbnail": snippet['thumbnails']['default']['url'],
                "Caption_Status": content_details.get("caption", "")
            }
            video_data.append(video_details)

    #Fetch comment data for all the video IDs
    for video in video_data:
        video_id = video["Video_Id"]
        video["Comments"] = []

        try:
            next_page_token = None
            while True:
                request = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=next_page_token
                )
                response = request.execute()

                for item in response["items"]:
                    comment_details = {
                        "Comment_Id": item['id'],
                        "Comment_Text": item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        "Comment_Author": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        "Comment_PublishedAt": item['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                    video['Comments'].append(comment_details)
            
                if "nextPageToken" in response:
                    next_page_token = response["nextPageToken"]
                else:
                    break
                
        except Exception as e:
            print(f"Failed to retrieve comments for video ID: {video_id}")

    channel_data["Videos"] = video_data

    #Insert all channel, video and comment data into MongoDB
    existing_data = db.coll.find_one({"Channel_Id": channel_id})

    #If data alreadys exists in MongoDB for the input channel_ID, then do update, else insert
    if existing_data:
        db.coll.update_one({"Channel_Id": channel_id}, {"$set": channel_data})
    else:
        channel_data["Channel_Id"] = channel_id
        db.coll.insert_one(channel_data)
        
    st.write(f"Channel details successfully fetched for Channel ID: {channel_id}")

def convert_dur(duration):
    
    regex=r'PT((\d+)H)?((\d+)M)?((\d+)S)?'
    match = re.match(regex,duration)
    
    if match:
        h = int(match.group(2) or 0)
        m = int(match.group(4) or 0)
        s = int(match.group(6) or 0)
        total_s = (h*3600)+(m*60)+s
        return total_s
    else:
        return 0

def migrate_to_sql(channel_id):

    #Fetch data from MongoDB for the input channel ID
    channel_data = db.coll.find_one({"Channel_Id": channel_id})

    #If data already exists in MySQL for the input channel ID, delete the data before inserting
    if channel_data:
        query="select count(*) from channel where channel_id = %s"
        data=(channel_id,)
        cursor.execute(query,data)
        count=0
        for data in cursor.fetchall():
            count+=1
        if count>0:
            query="delete from comment where video_id in (select video_id from video where channel_id = %s)"
            data=(channel_id,)
            cursor.execute(query,data)
            query="delete from video where channel_id = %s"
            data=(channel_id,)
            cursor.execute(query,data)
            query="delete from channel where channel_id = %s"
            data=(channel_id,)
            cursor.execute(query,data)

    #Inserting channel data into MySQL
    query="""insert into channel (channel_id, channel_name, channel_views, channel_description)
             values(%s, %s, %s, %s)"""
    data=(channel_data["Channel_Id"], channel_data["Channel_Name"], channel_data["Channel_Views"], channel_data["Channel_Description"])
    cursor.execute(query,data)
    connection.commit()

    #Inserting video data into MySQL
    for video in channel_data["Videos"]:
        
        published_date = video["PublishedAt"][0:10]+" "+video["PublishedAt"][11:19]

        converted_dur = convert_dur(video["Duration"])

        query="""insert into video(video_id, channel_id, video_name, video_description, published_date, view_count, like_count, dislike_count,
                                   favorite_count, comment_count, duration, thumbnail, caption_status)
                 values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        data=(video["Video_Id"], channel_data["Channel_Id"], video["Video_Name"], video["Video_Description"], published_date, int(video["View_Count"]),
          int(video["Like_Count"]), int(video["Dislike_Count"]), int(video["Favorite_Count"]), int(video["Comment_Count"]), converted_dur,
              video["Thumbnail"], video["Caption_Status"])
        
        cursor.execute(query,data)
        connection.commit()

        #Inserting comment data into MySQL
        for comment in video["Comments"]:
            comment_published_date = comment["Comment_PublishedAt"][0:10]+" "+comment["Comment_PublishedAt"][11:19]
            query="""insert into comment(comment_id, video_id, comment_text, comment_author, comment_published_date)
                     values(%s, %s, %s, %s, %s)"""
            data=(comment["Comment_Id"], video["Video_Id"], comment["Comment_Text"], comment["Comment_Author"], comment_published_date)
            cursor.execute(query,data)
            connection.commit()

def main():
    
    st.set_page_config(layout='wide')
    # Title
    st.title(":red[YouTube] Data API and :red[Streamlit] Demo")
    # columns for fetching & migration
    col1, col2 = st.columns(2)

    with col1:
        
        st.header(':blue[Data Collection]')
        st.write('''**Get :red[YouTube] channel details by entering valid channel_id**''')

    #Get chanel ID as input from user
        channel_id = st.text_input("Enter Channel ID")
        st.write('''Click the **:blue['Get Channel Data']** button to get the channel details of the required channel_id.''')
    #Trigger get_channel_data function when user presses the button
        if st.button(":green[Get Channel Data]"):
             with st.spinner('Please wait...'):
                 get_channel_data(youtube, channel_id)

    with col2:
        
        st.header(':blue[Data Migration]')
        st.write('''**Choose channel_id(s) from dropdown to store the :red[YouTube] channel details**''')

        #Initialize fetched_channel_ids
        fetched_channel_ids = st.session_state.setdefault('fetched_channel_ids', [])

        #isinstance() function returns True if the specified object is of the specified type
        if isinstance(fetched_channel_ids, str):
            fetched_channel_ids = [fetched_channel_ids]

        #Add existing channel IDs from MongoDB to fetched_channel_ids list for user to select for migration to MySQL
        for ch_id in [item["Channel_Id"] for item in db.coll.find()]:
            if ch_id not in fetched_channel_ids:
                fetched_channel_ids.append(ch_id)

        #Add the new user-input channel ID to fetched_channel_ids if not already there
        if channel_id not in fetched_channel_ids:
            fetched_channel_ids.append(channel_id)
        
        #session state is a way to share variables between reruns, for each user session
        st.session_state['fetched_channel_ids'] = fetched_channel_ids

        #Store the user selected channel IDs for migration to MySQL
        selected_channel_ids = st.multiselect("Select Channel IDs to migrate",
                                              st.session_state.get('fetched_channel_ids', []))
        
        st.write('''Click on **:blue['Migrate to SQL']** to store the channel details in **SQL**''')
        
        if st.button(":green[Migrate to SQL]"):
            for selected_id in selected_channel_ids:
                try:
                    with st.spinner('Please wait...'):
                        migrate_to_sql(selected_id)
                    st.write(f"Data migrated to MySQL for Channel ID: {selected_id}")
                except Exception as e:
                    st.error("Error occurred during migration: {}".format(str(e)))

    queries = {
        "1. What are the names of all the videos and their corresponding channels?": """
            select video_name, channel_name
            from video join channel on video.channel_id = channel.channel_id
        """,
        "2. Which channels have the most number of videos, and how many videos do they have?": """
            select c.channel_name, count(v.video_id) as video_count
            from channel c join video v on c.channel_id = v.channel_id
            group by v.channel_id
            order by video_count desc
            limit 1
        """,
        "3. What are the top 10 most viewed videos and their respective channels?": """
            select v.video_name, c.channel_name, v.view_count
            from video v join channel c on v.channel_id = c.channel_id
            order by v.view_count desc
            limit 10
        """,
        "4. How many comments were made on each video, and what are their corresponding video names?": """
            select video_name, comment_count from video
            order by comment_count desc
        """,
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?": """
            select v.video_name, c.channel_name, like_count
            from video v join channel c on v.channel_id = c.channel_id
            where like_count = (select max(like_count) from video)
        """,
        "6. What is the total number of likes and favorites for each video, and what are their corresponding video names?": """
            select video_name, like_count, favorite_count
            from video
            order by like_count desc
        """,
        "7. What is the total number of views for each channel, and what are their corresponding channel names?": """
            select channel_name, channel_views
            from channel
            order by channel_views desc
        """,
        "8. What are the names of all the channels that have published videos in the year 2022?": """
            select distinct (channel_name)
            from channel c join video v on c.channel_id = v.channel_id
            where year(v.published_date) = 2022
        """,
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?": """
            select channel_name, avg(duration)
            from channel c join video v ON c.channel_id = v.channel_id
            group by channel_name
        """,
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?": """
             select video_name, channel_name, comment_count
             from channel c join video v ON c.channel_id = v.channel_id
             where comment_count = (select max(comment_count) from video)
        """
    }

    # Sidebar section
    st.sidebar.header(':violet[Query Section]')

    # Create a dropdown menu to select the question
    selected_question = st.sidebar.selectbox("Select a question", list(queries.keys()))

    if st.sidebar.button(":green[Display Data]"):
        # Execute the selected query
        selected_query = queries[selected_question]
        cursor.execute(selected_query)
        results = cursor.fetchall()

        # Display the query results
        if results:
            st.sidebar.table(results)
        else:
            st.write("No results found.")

if __name__ == "__main__":
    main()
