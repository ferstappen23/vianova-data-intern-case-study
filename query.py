from helpers.db import DB
import pandas as pd
import json
import numpy as np
import requests  


db=DB()
df=pd.read_sql("SELECT DISTINCT cou_name_en AS CountryName , country_code  AS CountryCode FROM db_name3 WHERE cou_name_en NOT IN (SELECT DISTINCT cou_name_en FROM db_name3 WHERE population > 10000000) ORDER BY cou_name_en ",con=db.engine)
df.to_csv('NoMegapolis.tsv',index=False,sep="\t")
