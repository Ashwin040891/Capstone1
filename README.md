# Capstone1
Capstone Project - YouTube Data Harvesting

The purpose of this project is to create a Streamlit basic app which allows users to access and analyze YouTube channel details using channel IDs.

This app has the option for users to enter channel IDs - using which this app fetches all the requisite information via YouTube api, and store them in MongoDB.
This app also allows the user the option of migrating the data to MySQL.

Some of the details that are fetched and stored are:
i. Channel details - like channel name, view count, and, description
ii. Video details - like video name, description, tags, published date, view count, like count, comment count, duration, and, thumbnail
iii. Comment details - like comment text, comment author, published date

Overall flow of the code:
1. Import libraries for YouTube api, MongoDB, MySQL, Streamlit
2. Build YouTube api request key and connections to MongoDB and MySQL
3. Under main(), set the layout for the web application using streamlit
4. Under *col1*, the code is written to allow user to enter channel ID as input, fetch the channel details using YouTube api, and store in MongoDB by pressing the 'Get Channel Data' button
5. On pressing 'Get Channel Data' button, get_channel_data() function is triggered
6. This function stores all channel, video and comment details in a single dictionary 'channel_data';
   All video details are stored in this dictionary under the key 'Videos' as a list
   All comment details are stored under each video as 'Comments' key
7. At the end of the function, the dictionary 'channel_data' is inserted to MongoDB using channel ID as key
   If the data for that channel ID is already available in MongoDB, then update is done
8. Under *col2*, the code is written to allow user to migrate required channel data to MySQL for the purpose of data analysis by pressing the 'Migrate to SQL'
   On pressing 'Migrate to SQL' button, migrate_to_sql() function is triggered
   This function will first check whether the data for the provided channel ID is already present in MySQL; if yes, it will delete those data before inserting
   Then, this function will pull all channel, video and comment details from MongoDB for the provided channel ID; and insert them into the correpsonding tables
   Table name:
   Channel, Video and Comment
9. The final functionality provided is for the user to view and analyze channel data based on the queries selected
    For this project, we were given 10 queries which is displayed in the form of a dropdown
