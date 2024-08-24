# Youtube-Data-Harvesting-And-Warehousing
YouTube Data Harvesting and Warehousing is a project that intends to provide users with the ability to access and analyse data from numerous YouTube channels. MYSQL and Streamlit are used in the project to develop a user-friendly application that allows users to retrieve, save, and query YouTube channel and video data.

# TOOLS AND LIBRARIES USED:
this project requires the following components:

# STREAMLIT:
Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.

# PYTHON:
Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.

# GOOGLE API CLIENT:
The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.

# JSON:
JSON has been used to store the data from the youtube api clients and using mysql we can load the data and insert them to the table

# PANDAS:
Pandas has been used to convert the query out to proper tables

# MYSQL:
MySQL is an open-source, Relational Database Management System that stores data in a structured format using rows and columns. It’s software that enables users to create, manage, and manipulate databases.

# YOUTUBE DATA SCRAPPING AND ITS ETHICAL PERSPECTIVE:
When engaging in the scraping of YouTube content, it is crucial to approach it ethically and responsibly. Respecting YouTube's terms and conditions, obtaining appropriate authorization, and adhering to data protection regulations are fundamental considerations. The collected data must be handled responsibly, ensuring privacy, confidentiality, and preventing any form of misuse or misrepresentation. Furthermore, it is important to take into account the potential impact on the platform and its community, striving for a fair and sustainable scraping process. By following these ethical guidelines, we can uphold integrity while extracting valuable insights from YouTube data.

# REQUIRED LIBRARIES:
1.googleapiclient.discovery

2.streamlit

3.JSON

4.Pandas

5.Mysql

# FEATURES:

The following functions are available in the YouTube Data Harvesting and Warehousing application:
Retrieval of channel and video data from YouTube using the YouTube API Clients.

Storage of data in a JSON file as a data lake.

Migration of data from the JSON file to a MYSQL database for efficient querying and analysis.

Search and retrieval of data from the MYSQL database using different search options.

