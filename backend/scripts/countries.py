from helper import build_df
import pandas as pd

# ['AR', 'AU', 'BO', 'BR', 'CA', 'CL', 'CO', 'CR', 'DE', 'EC', 
# 'ES', 'FR', 'GB', 'IN', 'IT', 'JP', 'KR', 'MX', 'PE', 'PT', 'US', 'UY']

def create_df(list_of_countries):
    countries = list_of_countries

    empty_list = []

    for i in range(len(countries)):
        empty_list.append(build_df(listOfCountries = [countries[i]]))
    
    all_frames = pd.concat(empty_list)
    all_framesV2 = all_frames.copy().drop_duplicates(subset=['video_id'])
    all_frames["count"] = 1

    countries_df= all_frames[["video_id","country","count"]]
    countries_pivot = countries_df.pivot(index="video_id", columns="country", values="count").reset_index()

    f_df = all_framesV2.merge(countries_pivot, on="video_id", how="inner")
    f_df["sum_of_countries"] = f_df[list_of_countries].sum(axis=1)

    f_df.to_csv("../data/test_df_28.csv")

    return  all_frames, f_df



if __name__ == "__main__":
    create_df(list_of_countries= ['AR', 'AU', 'BO', 'BR', 'CA', 'CL', 'CO', 'CR', 'DE', 'EC','ES', 'FR', 'GB', 'IN', 'IT', 'JP', 'KR', 'MX', 'PE', 'PT', 'US', 'UY'])