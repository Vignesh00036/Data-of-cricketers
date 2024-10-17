<div align="center">
    <img src="Image/logo.png" alt="Logo" width="80" height="80">


<h3 align="center">Data Of Cricketers</h3>

  <p align="center">
    The program designed to assist you in building the best team possible!
    <br/>
    <a href="https://github.com/Vignesh00036/Data-of-cricketers"><strong>Explore the docs »</strong></a>
    <br />
  </p>
</div>


<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
        <li><a href="#structure-of-a-database">Structure of a database</a></li>
      </ul>
    </li>
    <li><a href="#how-to-run">How to run</a></li>
    <li><a href="#etl-journeyworkflow">ETL Journey/Workflow</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>

### About The Project
The Data of Cricketers program is a comprehensive application designed to collect and analyze cricket statistics from various formats of the game. This innovative platform aims to empower cricket enthusiasts, coaches, and analysts by providing them with real-time insights and historical context about player performances. This program is going to collects and aggregates data from various cricket match formats (T20,IPL,ODI,TEST).

<b><u>Here's Why Our Program Is The Best Program:</h1></b></u>
<ul>
    <li>
        <strong>Real-Time Data Updates:</strong><br>
        Stay informed with the latest player statistics and match results, ensuring access to the most current information at your fingertips.
    </li>
    <li>
        <strong>Historical Context:</strong><br>
        Gain insights from historical data to identify long-term trends and performance patterns, providing essential context to current player statistics.
    </li>
    <li>
        <strong>Scalability:</strong><br>
        Effortlessly expand the database to include new players, teams, and matches, ensuring that the program evolves alongside the sport.
    </li>
    <li>
        <strong>Comprehensive Analysis:</strong><br>
        Access a wealth of cricket statistics in one centralized location, facilitating detailed analysis and comparisons between players and teams.
    </li>
    <li>
        <strong>Data Loss:</strong><br>
        We have implemented four types of storage solutions to safeguard your data and prevent loss, ensuring that your information remains secure and accessible.
    </li>
</ul>      
Join us in our program as we guide you step by step through the process of scraping, modifying, and inserting data into a specific table.

## Built With:
<p>Our program is developed using the most efficient programming languages, robust libraries, and top-tier database management tools to enhance performance and optimize storage solutions.</p>

<b>Languages:</b>
<ul>
    <li>Python</li>
    <li>SQL</li>
</ul>

<b>Libraries:</b>
In this program, we utilized a variety of libraries to enhance functionality and performance. Below are the most frequently used libraries:
<ul>
    <li>Selenium</li>
    <li>Beautiful Soup (bs4)</li>
    <li>Pandas</li>
    <li>Requests</li>
</ul>
For a complete list of all libraries used in this program, please refer to the requirements.txt file available at the following link: <a href='https://github.com/Vignesh00036/Data-of-cricketers/blob/6864cbae8b42e2f54ad27e8f6b25115c93e077d5/Program%20Files/requirements.txt'>Libraries</a>

<b>Tools:</b>
<ul>
    <li>Psql</li>
    <li>Snowflake</li>
</ul>
<b>Cloud storage:</b>
<ul>
    <li>Amazon aws s3</li>
</ul>
We have also implemented Snowflake's internal stage for storage purposes, ensuring data security and preventing potential data loss.

## Structure Of A Database:
The database schema I’ve developed consists of four tables, each representing a specific cricket match format, with an id column serving as the primary key in each table. A separate table for player information has also been created, which references these primary keys using foreign key constraints to establish relationships. For a detailed view of the structure, you can refer to the data model available through the link provided: <a href="https://github.com/Vignesh00036/Data-of-cricketers/blob/cef7dc87cf51f0b76ed040db0e010feba816ed09/Image/Data_Model.jpg">Data Model</a>

## How To Run:
To execute this program, please ensure that all required Python libraries and tools are installed. For a comprehensive list of necessary libraries, refer to the following link: <a href='https://github.com/Vignesh00036/Data-of-cricketers/blob/6864cbae8b42e2f54ad27e8f6b25115c93e077d5/Program%20Files/requirements.txt'> Libraries</a>. Once the libraries are installed, open the main file located at <a href="https://github.com/Vignesh00036/Data-of-cricketers/blob/fedbc7d9565f24db5d3c0c964377379278737ff0/Program%20Files/main.py"> Main File</a> in your preferred Python environment. I recommend using Visual Studio Code (VS Code) for ease of use. After running the main file, the program will automatically handle the remaining processes.


## ETL Journey/Workflow:
<p><b>1. Data Scraping Process (Extract):</b></p>
<p>We utilized the website <strong>www.sadian.com</strong> as our data source for scraping players information. The process involves the following steps:</p>
<ul>
<u><b>1. Website Interaction:</b></u>
<ul>
    <li>
    Our program utilizes Selenium to seamlessly access and interact with the website, ensuring efficient data retrieval.
    </li>
</ul>
<b><u>2. Data Extraction:</u></b>
<ul>
    <li>
    Using <strong>Beautiful Soup</strong>, a powerful Python library, we efficiently scrape the list of players from the website along with players links, enabling us to collect the necessary data for analysis.
    </li>
    <li>
    Using Selenium, the program systematically loops through each player's profile link to access individual player details. 
    </li>
    <li>
    Within each profile, the program navigates through four available formats, extracting relevant data from each and storing it in a designated variable for further processing.
    </li>
</ul>
</ul>
<p><b>2. Data Transforming Process (Transform):</b></p>
<ul>
<b><u>1. Data Transformation:</u></b>
<ul>
    <li>
    Using Pandas (Python library), I perform data transformation and cleaning to convert raw data into a well-structured and highly accessible format.
    </li>
</ul>
<b><u>2. Data Validation:</u></b>
<ul>
    <li>
        During data validation, invalid characters are replaced with '0' to ensure data integrity. For example, any instances of '-' identified as invalid are transformed to '0'.
    </li>
</ul>
</ul>
<p><b>3. Data Loading Process (Load):</b></p>
<ul>
<b><u>1. Data Loading:</u></b>
<ul>
    <li>
        Once the data is structured, it is loaded into a database management system using PostgreSQL, where all data is systematically inserted into specific tables for efficient storage and retrieval.
    </li>
    <li>
        After data insertion into specific tables, it is extracted using the COPY command and subsequently uploaded to Snowflake’s internal storage as well as Amazon AWS S3 bucket, ensuring robust data accessibility and backup.
    </li>
</ul>
</ul>
<p><b>4. Auto Ingest (SQS):</b></p>
<ul>
    <li>
    We have established a Snowflake pipeline to facilitate automated data ingestion into specific tables from an S3 bucket, utilizing an SQS notification channel.
    </li>
</ul>

## Contact:
<ul>Name : Vignesh Elumalai</ul>
<ul>Email Id : <a href="mailto:vigneshoffl36@gmail.com">vigneshoffl36@gmail.com</a></ul>
<ul>LinkedIn profile : <a href="https://www.linkedin.com/in/vignesh-elumalai-2a4684332">Vignesh Elumalai</a></ul>

