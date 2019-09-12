import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import requests,json,sys,os,gc,time
from retrying import retry
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from scipy.spatial import procrustes
from scipy import stats
import math
from sklearn.linear_model import LinearRegression, Lasso

params = {
    # "day_date":"2018-04-30",
    "day_date":time.strftime('%Y-%m-%d',time.localtime()),
    "message":"Sucess",
    "client":"bonikro_drt_v5",
}

print(params)

# Action depending on BDD batch_contest
engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
batch_contest = pd.read_sql('select * from ecodriving_batch_contest;', con=engine)
batch_contest["datetime"] = batch_contest["datetime"].apply(lambda x: str(x.date()))
print(batch_contest.head(5))
ecodriving_contest = pd.read_sql('select * from ecodriving_contest;', con=engine)

contest =[]
for i,v in batch_contest.iterrows():
    if (v["datetime"] == params["day_date"] and v["contest_id"] in set(ecodriving_contest["id"])):
        contest.append({
            "contest_id":v["contest_id"],
            "batch_number":v["batch_number"],
            "id":v["id"]
            })            

        result = ecodriving_contest[ecodriving_contest["id"] == v["contest_id"]]
        print("Scoring will be done for contest with the following characteristics:")
        print(result)

        batch_duration = result["batch_duration"].iloc[0]
        batch_number = int(v["batch_number"])
        score_type = result["score_type"].iloc[0]
        contest_id = int(v["contest_id"])


        # CRON EXECUTION

        ## EXECUTE MAIN SCORING with inputs contest_id, batch_number et score_type
        os.system("python3 Main_scoring_v10_Bonikro_LL.py "+str(contest_id)+" "+str(batch_number)+" "+str(batch_duration)+" "+str(score_type))

        # ## EXECUTE MAIN SCORING CONTINUOUS with inputs contest_id et batch_number
        os.system("python3 Main_continuous_scoring_v2_Bonikro_LL.py "+str(int(v["contest_id"]))+" "+str(int(v["batch_number"])))

        ## EXECUTE SEND RESULTS with inputs contest_id, batch_number and batch_duration
        os.system("python3 Send_results_scoring_v2_Bonikro_LL.py "+str(int(v["contest_id"]))+" "+str(int(v["batch_number"]))+" "+str(int(batch_duration)))

        # updates the MySQL table "ecodriving_batch_contest" by setting "is_validated" to 1
        query= "UPDATE `ecodriving_batch_contest` set `is_validated` = 1 where contest_id ="+str(v["contest_id"])+" and batch_number = "+str(v["batch_number"])
        result = engine.execute(query)

    else:
        continue