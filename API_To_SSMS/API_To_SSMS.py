import requests
import pyodbc

# Function to extract relevant information from the API response
def extract_data(api_response):
    extracted_data = []
    items = api_response.get("items", [])
    for item in items:
        snippet = item.get("snippet", {})
        title = snippet.get("title", "")
        description = snippet.get("description", "")
        published_at = snippet.get("publishedAt", "")
        thumbnail_url = snippet.get("thumbnails", {}).get("default", {}).get("url", "")

        extracted_data.append({
            "Title": title,
            "Description": description,
            "PublishedAt": published_at,
            "ThumbnailURL": thumbnail_url
        })
    return extracted_data

# Extract data from YouTube API
url = "https://youtube-v31.p.rapidapi.com/playlistItems"
querystring = {"playlistId": "RDZiQo7nAkQHU", "part": "snippet", "maxResults": "30"}
headers = {
    "X-RapidAPI-Key": "b9f9be2f58mshd3a74c1e4bd9de3p111331jsn116387773cfc",
    "X-RapidAPI-Host": "youtube-v31.p.rapidapi.com"
}
response = requests.get(url, headers=headers, params=querystring)
api_data = response.json()

# Transform data
transformed_data = extract_data(api_data)

# Load data into SQL Server
conn_str = (
    r"DRIVER={SQL Server Native Client 11.0};"
    r"SERVER=DESKTOP-######123;"
    r"DATABASE=######123;"
    r"Trusted_Connection=yes;"
)
cnxn = pyodbc.connect(conn_str)
cursor = cnxn.cursor()

# Create table if it doesn't exist
cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[YouTubeData]') AND type in (N'U'))
    BEGIN
    CREATE TABLE YouTubeData (
        Title NVARCHAR(255),
        Description NVARCHAR(MAX),
        PublishedAt DATETIME,
        ThumbnailURL NVARCHAR(255)
    )
    END
""")

# Insert data into the table
for item in transformed_data:
    cursor.execute("""
        INSERT INTO YouTubeData (Title, Description, PublishedAt, ThumbnailURL)
        VALUES (?, ?, ?, ?)
    """, item["Title"], item["Description"], item["PublishedAt"], item["ThumbnailURL"])

# Commit the transaction and close connection
cnxn.commit()
cnxn.close()
