import os
import pandas as pd
import re
import json
import requests
from helpers.db import DB

db = DB()
url='https://public.opendatasoft.com/api/records/1.0/search/?dataset=geonames-all-cities-with-a-population-1000&q=&rows=10000'
r = requests.get(url)
response=r.json()
df = pd.DataFrame(response["records"])
#engine=create_engine(db.uri)
df['fields'] = df['fields'].apply(json.dumps)
df['geometry'] = df['geometry'].apply(json.dumps)
 #TRANSFORMATIONS
def extract(data):
    data=data.rename(columns={0:"datasetid",1:'recordid',2:'fields',3:'geometry',4:'record_timestamp'})
    return data
def transform(df):
    #create dic relate index and recordid
    df.fields=df.fields.apply(json.loads)
    df.geometry=df.geometry.apply(json.loads)
    #preporccesion for fields
    for column in df.fields[0].keys():
        values=[]
        for i in range(df.shape[0]):
            if (column not in df.fields[i].keys()):
                df.fields[i][column]=None
                values.append(df.fields[i][column])
            else:
                values.append(df.fields[i][column])
        df[column]=values
    #preprocessing for geometry 
    for column in df.geometry[0].keys():
        values=[]
        for i in range(df.shape[0]):
            if type(df.geometry[i])!=dict:
                values.append(None)
            else:
                values.append(df.geometry[i][column])
        df[column]=values
    df.drop(columns=["recordid","datasetid","geometry","fields","record_timestamp","type"],inplace=True)
    return  df 
    

df1=transform(extract(df))
df1.to_sql(name='db_name3',con=db.engine,if_exists='replace',index=False) 

