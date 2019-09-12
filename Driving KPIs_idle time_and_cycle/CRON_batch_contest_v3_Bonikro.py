# coding: utf-8


# TO DO
## App4mob: 
    
## eleven:

# In[87]:

import json
import numpy as np
import pandas as pd
#import MySQLdb
#from sqlalchemy import create_engine
from datetime import datetime, timedelta
import requests,json,sys,os,gc,time
from retrying import retry
# from libs.datastore import __mysql
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from scipy.spatial import procrustes
from scipy import stats
import math
from sklearn.linear_model import LinearRegression, Lasso


params = {
    #"day_date":time.strftime('%Y-%m-%d',time.localtime()),
    "day_date":"2018-01-15",
    "message":"Sucess",
    "client":"bonikro_drt_ecodriving_v4",
}
print(params)

try: 
    begin = time.time()

    # Action depending on BDD batch_contest
    engine = create_engine("mysql+mysqldb://test:test@164.132.195.254/"+params["client"])
    batch_contest= pd.read_sql('select * from ecodriving_batch_contest;', con=engine)
    batch_contest["datetime"] = batch_contest["datetime"].apply(lambda x: str(x.date()))
    print(batch_contest)

    contest =[]
    for i,v in batch_contest.iterrows():
        if v["datetime"]== params["day_date"]:
            contest.append({
                "contest_id":v["contest_id"],
                "batch_number":v["batch_number"],
                "id":v["id"]
                })            

            print(contest)
            engine = create_engine("mysql+mysqldb://test:test@164.132.195.254/"+params["client"])
            query = "SELECT v.batch_duration, v.score_type FROM `ecodriving_contest`as v WHERE `id`="+str(v["contest_id"])
            result = pd.read_sql(query, con=engine)
            print(result)

            batch_duration = result["batch_duration"].iloc[0]
            score_type = result["score_type"].iloc[0]
            print(v["batch_number"])
            print(str(score_type)) 
            print(v["contest_id"])

            # CRON EXECUTION

            ## EXECUTE MAIN SCORING with inputs contest_id et batch_number
            os.system("python3 Main_scoring_v10_Bonikro.py "+str(int(v["contest_id"]))+" "+str(int(v["batch_number"]))+" "+str(batch_duration)+" "+str(score_type))

            ## EXCECUTE MAIN SCORING CONTINUOUS with inputs contest_id et batch_number
            #os.system("python3 Main_continuous_scoring_v2_Bonikro.py "+str(int(v["contest_id"]))+" "+str(int(v["batch_number"])))

            ## EXECUTE SEND RESULTS with inputs contest_id, batch_number and batch_duration
            #os.system("python3 Send_results_scoring_v0.py "+str(int(v["contest_id"]))+" "+str(int(v["batch_number"]))+" "+str(int(batch_duration)))
            #os.system("python3 Print_hello.py "+str(v["contest_id"])+" "+str(v["batch_number"])+" "+str(batch_duration))

            query= "UPDATE `ecodriving_batch_contest` set `is_validated` = 1 where contest_id ="+str(v["contest_id"])+" and batch_number = "+str(v["batch_number"])
            result = engine.execute(query)
        
        else:
            continue


except Exception as e: 
    exc_type, exc_obj, exc_tb = sys.exc_info()
    msg = 'Error: ' +str(e) +' L:'+str(exc_tb.tb_lineno)
    print(msg)
    params["message"] = msg
    pass

logs = []
logs.append({
        "date_begin": params["day_date"],
        "date_end": time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()),
        "message": params["message"],
        "log_type":"CRON_batch_contest"
                })
logs = pd.DataFrame(logs)
engine = create_engine("mysql+mysqldb://test:test@164.132.195.254/"+params["client"])
logs.to_sql(con=engine,name='ecodriving_logs_CRON', if_exists='append', index=False)
print("duration:"+str(time.time()-begin)+"s")
   

