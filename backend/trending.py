from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime,date
import time
import os.path
from os import path

def trending_now(country_l= ["MX"]): 
    country_list = country_l
    original_path= "data"
    date_new, hour,day,month,year= get_dates()
    df_videos= get_videolist(countries = country_list , date = date_new)
    df_videos.to_csv(f"{original_path}/{month}_{day}_{year}.csv")
    print("Dataframe Created!") 
    return df_videos

def get_dates():
    date_new= date(datetime.now().year,datetime.now().month,datetime.now().day).isoformat()
    hour= int(datetime.now().hour)
    day = datetime.now().day
    month = datetime.now().month
    year = datetime.now().year
    return date_new, hour,day,month,year

def get_videolist(countries,date):
    empty_list = []
    nextPageToken = None

    api_key= ""
    youtube = build("youtube","v3", developerKey=api_key)

    while True:
        c=countries
        
        for cont in range(len(c)):
            chart_mx= youtube.videos().list(
                part=["id","snippet","statistics","contentDetails"],
                chart="mostPopular",
                regionCode=c[cont],
                pageToken=nextPageToken,
                maxResults=50)
            
            response = chart_mx.execute()
            count= len(response["items"])

            for i in range(count):
                id_v = response["items"][i]
                empty_list.append({
                    "video_id": id_v["id"],
                    "country_trending": c[cont],
                    "day_trending": date
                })
                
        nextPageToken= response.get("nextPageToken")
        
        
        if not nextPageToken:
            break
            
      
    df_videos = pd.DataFrame.from_dict(empty_list)
    
    return df_videos

if __name__ == "__main__":
    trending_now()