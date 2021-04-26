import pandas as pd
import json
from googleapiclient.discovery import build
from datetime import datetime, date
import time


def main():
    api_key = ""
    list_of_countries = ['AR', 'AU', 'BO', 'BR', 'CA', 'CL', 'CO', 'CR',
                         'DE', 'EC', 'ES', 'FR', 'GB', 'IN', 'IT', 'JP', 'KR', 'MX', 'PE', 'PT', 'US', 'UY']

    # Function to retrieve the current month, day and year.
    date_new, hour, day, month, year = get_dates()

    #  Call the Youtube API using the API Key
    youtube = get_youtube(api_key)

    # This function returns 2 dataframe.
    # "df_videos" is a list of all the video ids and the countries were that video was trending.
    # "pivot_table" is a table that shows in which countries the video id was trending
    df_videos, pivot_table, df_videos_III = get_videolist(
        youtube=youtube, countries=list_of_countries, date=date_new)
    categories = get_categories_names("US", youtube).head()

    list_of_vid_ids = get_list_of_videos(dataframe=df_videos_III)

    list_with_videos = getting_fifty_elements_lists(
        ids_from_videos=list_of_vid_ids)

    vid_stats = stats_from_videos(
        videos_list=list_with_videos, youtube=youtube)

    channel__id = vid_stats["channel_id"].tolist()

    full_video_stats = vid_stats.merge(df_videos_III, on="video_id", how="inner").merge(
        categories, on="categoryId", how="inner")

    list_with_channels_ids = getting_fifty_elements_lists(
        ids_from_videos=channel__id)

    channels_data, upload_id = data_from_channels(
        ch_ids=list_with_channels_ids, youtube=youtube)

    ids_of_last_5_videos = getting_the_ids_of_videos(
        ids_of_playlists=upload_id, youtube=youtube)

    list_with_videos = getting_fifty_elements_lists(ids_of_last_5_videos)

    last_5_videos_data, count_v = stats_from_last_5_videos(
        videos_list=list_with_videos, youtube=youtube)

    dataframe_w_5videos = building_df(count=count_v, data=last_5_videos_data)

    full_dataframe = full_df(stats_from_video=df_videos_III, stats_from_channels=channels_data,
                             stats_from_5_videos=dataframe_w_5videos, trending_countries=pivot_table, cat=categories, countries=list_of_countries)

    full_dataframe.to_csv(f"full_df_{day}_{month}{year}_{hour}.csv")

    build_the_json_file(full_dataframe=full_dataframe,
                        countries_n=list_of_countries)


def get_dates():
    date_new = date(datetime.now().year, datetime.now().month,
                    datetime.now().day).isoformat()
    hour = int(datetime.now().hour)
    day = datetime.now().day
    month = datetime.now().month
    year = datetime.now().year

    if month < 10:
        month = f"0{month}"

    if day < 10:
        day = f"0{day}"

    return date_new, hour, day, month, year


def get_youtube(api_key):
    youtube = build("youtube", "v3", developerKey=api_key)
    return youtube


def get_videolist(countries, date, youtube):
    empty_list = []
    nextPageToken = None

    while True:
        c = countries

        for cont in range(len(c)):
            chart_mx = youtube.videos().list(
                part=["id", "snippet", "statistics", "contentDetails"],
                chart="mostPopular",
                regionCode=c[cont],
                pageToken=nextPageToken,
                maxResults=50)

            response = chart_mx.execute()
            count = len(response["items"])

            for i in range(count):
                id_v = response["items"][i]
                empty_list.append({
                    "video_id": id_v["id"],
                    "country_trending": c[cont],
                    "day_trending": date
                })

        nextPageToken = response.get("nextPageToken")

        if not nextPageToken:
            break

    df_videos = pd.DataFrame.from_dict(empty_list)
    df_videos["values"] = 1

    pivot_table = df_videos.pivot(
        index='video_id', columns='country_trending', values="values").fillna(0)

    df_videos_III = df_videos.drop_duplicates("video_id")

    return df_videos, pivot_table, df_videos_III


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


def get_list_of_videos(dataframe):
    list_of_vid_ids = dataframe["video_id"].tolist()

    return list_of_vid_ids


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

    return vid_stats


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


def getting_the_ids_of_videos(ids_of_playlists, youtube):
    list_of_video_ids = []
    for i in range(len(ids_of_playlists)):
        pl_request = youtube.playlistItems().list(
            part=["contentDetails"],
            playlistId=ids_of_playlists[i],
            maxResults=10).execute()

        for si in range(len(pl_request["items"])):
            video_ids = pl_request["items"][si]['contentDetails']["videoId"]
            upload_id = ids_of_playlists[i]
            list_of_video_ids.append(video_ids)

    return list_of_video_ids


def stats_from_last_5_videos(videos_list, youtube):
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
                "publishedAt": binfo["publishedAt"],
                "categoryId": binfo["categoryId"]
            })

    last_5_videos_data = pd.DataFrame.from_dict(stats_dict).sort_values(
        by=["channel_id", "publishedAt"], ascending=False).reset_index()

    count_v = last_5_videos_data[["channel_id"]].groupby("channel_id").size(
    ).reset_index().sort_values(by="channel_id", ascending=False)[0].tolist()

    return last_5_videos_data, count_v


def building_df(count, data):
    lista_vacia = []
    val1 = 0

    for i in range(len(count)):
        for si in range(count[i]):
            lista_vacia.append({
                "channel_id": data["channel_id"][val1],
                f"categoryId": data["categoryId"][val1],
                f"title{si}": data["title"][val1],
                f"viewCount{si}": data["viewCount"][val1],
                f"likeCount{si}": data["likeCount"][val1],
                f"dislikeCount{si}": data["dislikeCount"][val1],
                f"commentCount{si}": data["commentCount"][val1],
                f"duration{si}": data["duration"][val1],
                f"publishedAt{si}": data["publishedAt"][val1]
            })

            val1 += 1
    ger = pd.DataFrame.from_dict(lista_vacia)
    df_final = pd.DataFrame.from_dict(lista_vacia).groupby(
        "channel_id").first().reset_index()

    return df_final


def full_df(stats_from_video, stats_from_channels, stats_from_5_videos, trending_countries, cat, countries):
    full_datapt1 = vid_stats.merge(df_videos_III, on="video_id", how="inner")
    full_datapt2 = full_datapt1.merge(
        channels_data, on="channel_id", how="inner")
    full_datapt3 = full_datapt2.merge(
        dataframe_w_5videos, on="channel_id", how="inner")
    full_datapt4 = full_datapt3.merge(pivot_table, on="video_id", how="inner").rename(
        columns={"categoryId_x": "categoryId"})
    full_dataframe = full_datapt4.merge(cat, on="categoryId", how="left")
    full_dataframe["countries_trending"] = full_dataframe["AU"] + full_dataframe["BO"] + full_dataframe["BR"] + full_dataframe["CA"] + full_dataframe["CL"] + full_dataframe["CO"] + full_dataframe["CR"] + full_dataframe["DE"] + full_dataframe["EC"] + full_dataframe["ES"] + \
        full_dataframe["FR"] + full_dataframe["GB"] + full_dataframe["IN"] + full_dataframe["IT"] + full_dataframe["JP"] + \
        full_dataframe["KR"] + full_dataframe["MX"] + full_dataframe["PE"] + \
        full_dataframe["PT"] + full_dataframe["US"] + full_dataframe["UY"]

    for i in range(len(countries)):
        full_dataframe[f"mrkt{countries[i]}"] = full_dataframe[f'{countries[i]}'].apply(
            lambda x: f'{countries[i]}' if x == 1 else '')

    new = full_dataframe.iloc[:, -21:]
    full_dataframe["markets_trending"] = full_dataframe["mrktAR"].str.cat(
        new, sep=" ")

    return full_dataframe


def full_df(stats_from_video, stats_from_channels, stats_from_5_videos, trending_countries):
    full_df = stats_from_video.merge(stats_from_channels, on="channel_id", how="inner").merge(
        stats_from_5_videos, on="channel_id", how="inner").merge(trending_countries, on="video_id", how="inner").rename(columns={"category_title_y": "category_title", "categoryId_y": "category_id"})

    return full_df


def test_df(full_df, market):
    test = full_df[['channel_id', 'channel_title', 'number_of_views', 'published_videos',
                    'channel_subs', 'birth_of_channel', 'country_of_the_channel',
                    'channel_thumbnail', 'upload_playlist', 'video_id', 'publishedAt',
                    'title', 'viewCount', 'likeCount', 'dislikeCount', 'commentCount', 'duration',
                    'thumbnail', "link", "video_lang", "description", "country_trending", "day_trending",
                    "category_id", "category_title",
                    'title0', 'viewCount0', "publishedAt0",
                    'title1', 'viewCount1', "publishedAt1",
                    'title2', 'viewCount2', "publishedAt2",
                    'title3', 'viewCount3', "publishedAt3",
                    'title4', 'viewCount4', "publishedAt4",
                    f'{market}', 'countries_trending', 'markets_trending'
                    ]].sort_values(by="channel_title", ascending=True)

    runn_this = test[["channel_title"]].groupby(
        "channel_title").size().tolist()

    return test, runn_this


def json_df(test, runn_this, market):
    list_empty_II = []
    accounting = 0

    for am in range(len(runn_this)):
        #print(am, end =" ")
        for si in range(int(runn_this[am])):
            list_empty_II.append({
                "channel_id": test.iloc[accounting].tolist()[1],
                "channel_title": test.iloc[accounting].tolist()[1],
                "number_of_views": test.iloc[accounting].tolist()[2],
                "published_videos": test.iloc[accounting].tolist()[3],
                'channel_subs': test.iloc[accounting].tolist()[4],
                'birth_of_channel': test.iloc[accounting].tolist()[5],
                'country_of_the_channel': test.iloc[accounting].tolist()[6],
                'channel_thumbnail': test.iloc[accounting].tolist()[7],
                'upload_playlist': test.iloc[accounting].tolist()[8],
                f'video_id{si}':  test.iloc[accounting].tolist()[9],
                f'publishedAt{si}': test.iloc[accounting].tolist()[10],
                f'title_{si}': test.iloc[accounting].tolist()[11],
                f'viewCount{si}': test.iloc[accounting].tolist()[12],
                f'likeCount{si}': test.iloc[accounting].tolist()[13],
                f'dislikeCount{si}': test.iloc[accounting].tolist()[14],
                f'commentCount{si}': test.iloc[accounting].tolist()[15],
                f'duration{si}': test.iloc[accounting].tolist()[16],
                f'thumbnail{si}': test.iloc[accounting].tolist()[17],
                f"link{si}": test.iloc[accounting].tolist()[18],
                f"video_lang{si}": test.iloc[accounting].tolist()[19],
                f"description{si}": test.iloc[accounting].tolist()[20],
                f"day_trending{si}": test.iloc[accounting].tolist()[22],
                f"category_id{si}": test.iloc[accounting].tolist()[23],
                f"category_title{si}": test.iloc[accounting].tolist()[24],
                'title0': test.iloc[accounting].tolist()[25],
                'viewCount0': test.iloc[accounting].tolist()[26],
                "publishedAt0": test.iloc[accounting].tolist()[27],
                'title1': test.iloc[accounting].tolist()[28],
                'viewCount1': test.iloc[accounting].tolist()[29],
                "publishedAt1": test.iloc[accounting].tolist()[30],
                'title2': test.iloc[accounting].tolist()[31],
                'viewCount2': test.iloc[accounting].tolist()[32],
                "publishedAt2": test.iloc[accounting].tolist()[33],
                'title3': test.iloc[accounting].tolist()[34],
                'viewCount3': test.iloc[accounting].tolist()[35],
                "publishedAt3": test.iloc[accounting].tolist()[36],
                'title4': test.iloc[accounting].tolist()[37],
                'viewCount4': test.iloc[accounting].tolist()[38],
                "publishedAt4": test.iloc[accounting].tolist()[39],
                f'{market}': test.iloc[accounting].tolist()[40],
                "countries_trending": test.iloc[accounting].tolist()[41],
                "markets_trending": test.iloc[accounting].tolist()[42]

            })
            accounting += 1

    json_dataframe = pd.DataFrame.from_dict(list_empty_II).groupby("channel_title").first(
    ).reset_index().set_index("channel_title").transpose().fillna(0)

    return json_dataframe


def json_dataframe_II(helloV1, runn_this, market):
    empty_list_11 = []

    for i in range(len(helloV1.columns)):
        #print(i, end =" ")
        empty_list_11.append({
            "channel": helloV1.columns[i],
            "id": helloV1.iloc[:, i].to_frame().loc[f"channel_id"][0],
            "total_views": helloV1.iloc[:, i].to_frame().loc[f"number_of_views"][0],
            "published_videos": helloV1.iloc[:, i].to_frame().loc[f"published_videos"][0],
            "total_subs": helloV1.iloc[:, i].to_frame().loc[f"channel_subs"][0],
            "created_on": helloV1.iloc[:, i].to_frame().loc[f"birth_of_channel"][0],
            "country_of_origin": helloV1.iloc[:, i].to_frame().loc[f"country_of_the_channel"][0],
            "thumbnail": helloV1.iloc[:, i].to_frame().loc[f"channel_thumbnail"][0],
            "trending_videos": runn_this[i],
            "last_5_stats": [],
            "items": [],
            "country": f"{market}",
            "total_market_trending": helloV1.iloc[:, i].to_frame().loc["countries_trending"][0],
            "markets_trending": helloV1.iloc[:, i].to_frame().loc["markets_trending"][0]
        })

        empty_list_11[i]["last_5_stats"].append({
            "title0": helloV1.iloc[:, i].to_frame().loc[f"title0"][0],
            "viewCount0": helloV1.iloc[:, i].to_frame().loc[f"viewCount0"][0],
            "publishedAt0": helloV1.iloc[:, i].to_frame().loc[f"publishedAt0"][0],
            "title1": helloV1.iloc[:, i].to_frame().loc[f"title1"][0],
            "viewCount1": helloV1.iloc[:, i].to_frame().loc[f"viewCount1"][0],
            "publishedAt1": helloV1.iloc[:, i].to_frame().loc[f"publishedAt1"][0],
            "title2": helloV1.iloc[:, i].to_frame().loc[f"title2"][0],
            "viewCount2": helloV1.iloc[:, i].to_frame().loc[f"viewCount2"][0],
            "publishedAt2": helloV1.iloc[:, i].to_frame().loc[f"publishedAt2"][0],
            "title3": helloV1.iloc[:, i].to_frame().loc[f"title3"][0],
            "viewCount3": helloV1.iloc[:, i].to_frame().loc[f"viewCount3"][0],
            "publishedAt3": helloV1.iloc[:, i].to_frame().loc[f"publishedAt3"][0],
            "title4": helloV1.iloc[:, i].to_frame().loc[f"title4"][0],
            "viewCount4": helloV1.iloc[:, i].to_frame().loc[f"viewCount4"][0],
            "publishedAt4": helloV1.iloc[:, i].to_frame().loc[f"publishedAt4"][0],
        })

        for si in range(runn_this[i]):
            empty_list_11[i]["items"].append(
                {
                    "item": si,
                    "published_date": helloV1.iloc[:, i].to_frame().loc[f"publishedAt{si}"][0],
                    "trending_date": helloV1.iloc[:, i].to_frame().loc[f"day_trending{si}"][0],
                    "video_title": helloV1.iloc[:, i].to_frame().loc[f"title_{si}"][0],
                    "category_title": helloV1.iloc[:, i].to_frame().loc[f"category_title{si}"][0],
                    "statistics": {
                        "views": helloV1.iloc[:, i].to_frame().loc[f"viewCount{si}"][0],
                        "likes": helloV1.iloc[:, i].to_frame().loc[f"likeCount{si}"][0],
                        "dislikes": helloV1.iloc[:, i].to_frame().loc[f"dislikeCount{si}"][0],
                        "comments": helloV1.iloc[:, i].to_frame().loc[f"commentCount{si}"][0],
                        "duration": helloV1.iloc[:, i].to_frame().loc[f"duration{si}"][0]
                    },
                    "description": helloV1.iloc[:, i].to_frame().loc[f"description{si}"][0],
                    "duration": helloV1.iloc[:, i].to_frame().loc[f"duration{si}"][0],
                    "link": helloV1.iloc[:, i].to_frame().loc[f"link{si}"][0],
                    "video_lang": helloV1.iloc[:, i].to_frame().loc[f"video_lang{si}"][0],
                    "thumbnail": helloV1.iloc[:, i].to_frame().loc[f"thumbnail{si}"][0],
                }
            )

    return empty_list_11


def json_dataframe_III(list_of_elements, market):
    json_object = json.dumps(list_of_elements)

    loaded_r = json.loads(json_object)

    with open(f"{market}.json", "w") as outfile:
        json.dump(list_of_elements, outfile)

    return loaded_r


def design_json(full_df, market):
    df = full_df.query(f'{market} == 1')
    test, runn_this = test_df(df, market=market)
    json_dataframe = json_df(test=test, runn_this=runn_this, market=market)
    empty_list_11 = json_dataframe_II(
        helloV1=json_dataframe, runn_this=runn_this, market=market)
    json_dataframe_III(list_of_elements=empty_list_11, market=market)


def build_the_json_file(full_dataframe, countries_n):

    for i in range(22):
        design_json(full_df=full_dataframe.rename(
            columns={"categoryId": "category_id"}), market=f"{countries_n[i]}")


if __name__ == "__main__":
    main()