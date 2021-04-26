from trending import trending_now
import pandas as pd
from googleapiclient.discovery import build
import os 
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
API_KEY = os.getenv("API_KEY")

def build_df(listOfCountries):
    youtube = get_youtube(API_KEY)
    videos_trending = trending_now(country_l= listOfCountries)
    ids_trending= videos_trending["video_id"].drop_duplicates().to_list()
    list_of_fifty= getting_fifty_elements_lists(ids_from_videos=ids_trending)
    df, channel_ids  = stats_from_videos(videos_list= list_of_fifty, youtube= youtube)
    list_of_channels = getting_fifty_elements_lists(ids_from_videos=channel_ids)
    f_df, upload_id = data_from_channels(ch_ids=list_of_channels, youtube=youtube)
    categories = get_categories_names(country=listOfCountries[0], youtube=youtube)
    final_df = df.merge(f_df, how='inner', on="channel_id").merge(categories, how="inner", on="categoryId")
    final_df["country"] = listOfCountries[0]
    return final_df

def get_youtube(api_key):
    youtube = build("youtube", "v3", developerKey=api_key)
    return youtube

def getting_fifty_elements_lists(ids_from_videos):

    first50 = list(ids_from_videos)
    empty_list = []
    count = len(first50)

    while count > 0:
        var1 = count - 50
        var2 = count

        if var1 < 0:
            var1 = 0

        empty_list.append(first50[var1:var2])

        count -= 50

    return empty_list

def stats_from_videos(videos_list, youtube):
    stats_dict = []

    for i in range(len(videos_list)):
        vid_request = youtube.videos().list(
            part=["snippet", "statistics", "contentDetails"],
            id=",".join(videos_list[i]))
        b = vid_request.execute()

        for si in range(len(b["items"])):
            bstats = b["items"][si]["statistics"]
            binfo = b["items"][si]["snippet"]
            bid = b["items"][si]["id"]
            bcontent = b["items"][si]["contentDetails"]

            stats_dict.append({
                "channel_id": binfo["channelId"],
                "video_id": bid,
                "publishedAt": binfo["publishedAt"],
                "title": binfo["title"],
                f"viewCount": bstats.get("viewCount"),
                f"likeCount": bstats.get("likeCount"),
                f"dislikeCount": bstats.get("dislikeCount"),
                f"commentCount": bstats.get("commentCount"),
                "duration": bcontent["duration"],
                "thumbnail": binfo["thumbnails"]["medium"]["url"],
                "link": f"https://youtu.be/{bid}",
                "video_lang": binfo.get("defaultAudioLanguage"),
                "categoryId": binfo["categoryId"],
                "description": binfo["description"]
            })

    vid_stats = pd.DataFrame.from_dict(stats_dict)
    channel_ids = vid_stats["channel_id"].drop_duplicates().to_list()

    return vid_stats, channel_ids

def data_from_channels(ch_ids, youtube):
    stats_dict = []
    upload_playlist = None

    for i in range(len(ch_ids)):
        vid_request = youtube.channels().list(
            part=["statistics", "snippet", "contentDetails"],
            id=",".join(ch_ids[i]))

        b = vid_request.execute()

        for si in range(len(b["items"])):

            bstats = b["items"][si]["statistics"]
            binfo = b["items"][si]["snippet"]
            bid = b["items"][si]["id"]

            try:
                b["items"][si]["contentDetails"]["relatedPlaylists"]["uploads"]
            except KeyError:
                upload_playlist = ""

            if upload_playlist != "":
                upload_playlist = b["items"][si]["contentDetails"]["relatedPlaylists"]["uploads"]

            stats_dict.append({
                "channel_title": binfo["title"],
                "number_of_views": bstats["viewCount"],
                "published_videos": bstats["videoCount"],
                "channel_subs": bstats.get("subscriberCount"),
                "birth_of_channel": binfo["publishedAt"],
                "country_of_the_channel": binfo.get("country"),
                "channel_id": bid,
                "channel_thumbnail": binfo["thumbnails"]["medium"]["url"],
                "upload_playlist": upload_playlist,
            })

    dataframe_output = pd.DataFrame.from_dict(
        stats_dict).drop_duplicates(subset=["channel_id"])

    upload_id = dataframe_output["upload_playlist"].tolist()

    return dataframe_output, upload_id


def get_categories_names(country, youtube):
    empty_list = []
    category_id = youtube.videoCategories().list(
        part=["snippet"],
        regionCode=country).execute()

    count = len(category_id["items"])

    for i in range(count):
        ctitle = category_id["items"][i]["snippet"]["title"]
        cid = category_id["items"][i]["id"]

        empty_list.append({
            "categoryId": cid,
            "category_title": ctitle
        })

    df = pd.DataFrame.from_dict(empty_list)

    return df



if __name__ == "__main__":
    build_df()