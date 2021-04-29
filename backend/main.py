from fastapi import FastAPI
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins=[
    "http://localhost:3000",
    "http://localhost:5500"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = pd.read_csv("./data/test_df_28.csv", index_col=0).fillna(0)

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/countries")
def get_country(country:str="MX", values:int=5):
    single_country = db.loc[db[f"{country}"] == 1].reset_index().head(values).transpose().to_dict()
    
    empty_list= []

    for i in range(len(single_country)):
        empty_list.append(single_country[i])  
  
    flag_list = []

    for i in range(values):
        valueOne = i
        valueTwo = i+1
        flags = db.loc[db[f"{country}"] == 1].iloc[valueOne:valueTwo,24:-1].reset_index().iloc[:,1:].transpose().reset_index().rename(columns={0:"counts"}).query('counts >0').set_index("index").transpose().columns
        flag_list.append([])
        
        for si in range(len(flags)):
            flag_list[i].append(flags[si])
    
    
    return {
        "items":empty_list,
        "flags":flag_list
    }

@app.get("/topfivetest")
def read_top(videos:int=5):
    trending = db.sort_values(by="sum_of_countries", ascending=False).iloc[0:videos,:].reset_index().transpose().to_dict()
    list_of_countries = db.country.drop_duplicates().reset_index()["country"].to_list()
    empty_list= []
    
    for i in range(len(trending)):
        empty_list.append(trending[i])
  
    flag_list = []

    for i in range(videos):
        valueOne = i
        valueTwo = i+1
        flags = db.sort_values(by="sum_of_countries", ascending=False).iloc[valueOne:valueTwo,24:-1].reset_index().iloc[:,1:].transpose().reset_index().rename(columns={0:"counts"}).query('counts >0').set_index("index").transpose().columns
        flag_list.append([])
        
        for si in range(len(flags)):
            flag_list[i].append(flags[si])

    

    return {
        "items":empty_list,
        "flags":flag_list,
        "list_of_countries":list_of_countries}

    