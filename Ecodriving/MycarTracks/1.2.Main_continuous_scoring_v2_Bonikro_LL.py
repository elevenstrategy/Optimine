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
from sqlalchemy import create_engine


params = {
    "day_date":time.strftime('%Y-%m-%d',time.localtime()),
    "contest_id": int(sys.argv[1]), # Get contest_id from external argument
    "batch_number":int(sys.argv[2]), # Get batch_number from external argument
    "message":"Success",
    "client_drt":"bonikro", #APP4MOB API
    "client":"bonikro_drt_v5",
}

def main_continuous_scoring(params):
    try:
        begin = time.time()
        data = transform(params)
        load(params,data)

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
        "log_type":"CRON_Main_continuous_scoring"
        })
    logs = pd.DataFrame(logs)
    engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
    logs.to_sql(con=engine,name='ecodriving_logs_CRON', if_exists='append', index=False)

    print("duration:"+str(time.time()-begin)+"s")
    return True


def transform(params):
    print(params)
    try:

        def score_on_base(base,serie,value):
            rebase = 100/base
            score = base - int(round(stats.percentileofscore(serie, value,'rank')/rebase, 0))
            if score < base/10:
                score = base/10
            return score

        continuous_scores = pd.DataFrame()
        continuous_move_scores = pd.DataFrame()

        # COMPUTE MOVE_SCORES
        ## Extract from `ecodriving_move_scores`
        engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
        query = "SELECT * FROM `ecodriving_move_scores` WHERE `id_contest`="+str(params["contest_id"])+" and `id_batch`<="+str(params["batch_number"])
        move_scores = pd.read_sql_query(query,con=engine) 
        move_scores = move_scores.drop_duplicates()

        print("move_scores: ------------------------------")
        print(move_scores.head())
        
        ## Weight the different scores with the number of moves
        def grouped_weighted_avg(values, weights, by):
            return (values.dropna() * weights.dropna()).groupby(by).sum() / weights.dropna().groupby(by).sum()

        continuous_move_scores ["score_acceleration"]= grouped_weighted_avg(move_scores["score_acceleration"],move_scores["nb_of_moves_acceleration"],move_scores["driver_id"])
        continuous_move_scores ["score_braking"] = grouped_weighted_avg(move_scores["score_braking"],move_scores["nb_of_moves_braking"],move_scores["driver_id"])
        continuous_move_scores ["score_cruise_speed"]= grouped_weighted_avg(move_scores["score_cruise_speed"],move_scores["number_of_moves_cruise_speed"],move_scores["driver_id"])

        ## On the whole contest, compute the continuous score
        continuous_move_scores["continuous_score_acceleration"] = continuous_move_scores["score_acceleration"].dropna().apply(lambda x: score_on_base(10, continuous_move_scores["score_acceleration"].dropna(),x))
        continuous_move_scores["continuous_score_braking"] = continuous_move_scores["score_braking"].dropna().apply(lambda x: score_on_base(10, continuous_move_scores["score_braking"].dropna(),x))
        continuous_move_scores["continuous_score_cruise_speed"] = continuous_move_scores["score_cruise_speed"].dropna().apply(lambda x: score_on_base(10, continuous_move_scores["score_cruise_speed"].dropna(),x))
        continuous_move_scores = continuous_move_scores.reset_index(level=["driver_id"])[["driver_id","continuous_score_acceleration","continuous_score_braking","continuous_score_cruise_speed"]]

        reg1 = LinearRegression(fit_intercept=False)

        # MERGE THE TWO DATAFRAMES and COMPUTE THE FINAL SCORE
        final_scores = pd.DataFrame()
        final_scores = continuous_move_scores
        final_scores = final_scores.fillna(0)
        final_scores["global_scores"] = final_scores[["continuous_score_acceleration","continuous_score_braking","continuous_score_cruise_speed"]].sum(axis=1)


        # Add the driver name in the final_scores dataframe
        query = "SELECT d.id,d.surname, d.name FROM `drt_driver` d WHERE d.id IN"+str(tuple(list(set(final_scores["driver_id"]))))
        results = pd.read_sql_query(query,con=engine) 
        results["driver_name"] = results["surname"]+" "+results["name"]
        results =  results.rename(columns ={"id":"driver_id"})

        final_scores = pd.merge(final_scores,results, how= 'left',on ="driver_id")
        final_scores["id_contest"]= [params["contest_id"]]*len(final_scores)
        final_scores["batch_number"]= [params["batch_number"]]*len(final_scores)
        final_scores["ranking"] = final_scores["global_scores"].rank(ascending = False)
        final_scores = final_scores.drop(["surname","name"], axis=1)
        final_scores = final_scores.rename(columns={"driver_name_y":"driver_name"})
        print("Final continuous score : ------------------")
        print(final_scores) 

        DataOut =[]
        DataOut = final_scores

        return DataOut

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        msg = 'Error: ' +str(e) +' L:'+str(exc_tb.tb_lineno)
        print(msg)
        gc.collect()
        raise Exception(msg) 

def load(params,data):
    continuous_scores = data

    engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
    continuous_scores.to_sql(con=engine,name='ecodriving_continuous_results', if_exists='append', index=False)
    print(continuous_scores)

    return True

main_continuous_scoring(params)
