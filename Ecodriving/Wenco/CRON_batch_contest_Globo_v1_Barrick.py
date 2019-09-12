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
import MySQLdb # INSTALL 'pip install mysqlclient'
import nexmo
from random import randint

params = {
    #"day_date":"2018-06-08", # FOR TEST ONLY
    "day_date":time.strftime('%Y-%m-%d',time.localtime()),
    "message":"Sucess",
    "client":"barrick_drt_v5",
    "domaine": "http://total-optimizer-ws.globoconnect.com/TotalOptimizerWs/service/anomalie", # TO UPDTAE: GLOBO domain to access the data
    "api_key":"F6C6BQZXC87SCQGB", # TO UPDATE: GLOBO API to access the data
}
print(params)

# Action depending on BDD batch_contest
engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
batch_contest = pd.read_sql('select * from ecodriving_batch_contest;', con=engine)
batch_contest["datetime"] = batch_contest["datetime"].apply(lambda x: str(x.date()))
#print(batch_contest.head(5))
ecodriving_contest = pd.read_sql('select * from ecodriving_contest;', con=engine)
#print(ecodriving_contest)


contest =[]
for i,v in batch_contest.iterrows():
    if (v["datetime"] == params["day_date"]):
        contest.append({
            "contest_id":v["contest_id"],
            "batch_number":v["batch_number"],
            "id":v["id"],
            "datetime":v["datetime"],
            })  
        print("Contest to be considered:---------------------------------------------------------------")
        print(contest)
        print("")
        engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
        query = "SELECT v.batch_duration, v.score_type FROM `ecodriving_contest`as v WHERE `id`="+str(v["contest_id"])
        result = pd.read_sql(query, con=engine)

        batch_duration = result["batch_duration"].iloc[0]
        batch_number = int(v["batch_number"])
        score_type = result["score_type"].iloc[0]
        contest_id = int(v["contest_id"])
        date_end = v["datetime"]
        
        ## PREPARE GLOBO REQUEST
        # Get perimeter of vehicle to consider for the selected contest
        engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])

        # the query is written to ensure the selection of vehicles that correspond only to the desired contest 
        query = "SELECT v.id, v.equipment_plant_number, v.equipment_name_mycartracks FROM `ecodriving_contests_has_vehicles` as chv, `drt_vehicle` as v WHERE contest_id ="+str(contest_id)+" and v.id = chv.vehicle_id"
        vehicle_to_consider = pd.read_sql_query(query,con=engine) # Dataframe with "id" and "equipment_plant_number"

        #vehicle_to_consider = vehicle_to_consider.iloc[0:4] 
        print("Vehicle to consider: ------------------------------------------------------------------")
        print(vehicle_to_consider)
        print("")

        # Get clean id_query without None value
        id_query =[]
        for i,v in vehicle_to_consider.iterrows():
            t = v["equipment_name_mycartracks"]
            if t is None:
                continue
            else:
                id_query.append(int(t))
        id_query = str(id_query)
        print(id_query)

        # Params
        domaine = params["domaine"]
        api_key = params["api_key"]

        #date_end = contest[0]["datetime"]
        
     ##############################################################################   
        #nb_week = contest[0]["batch_number"]
        nb_week = batch_number
        #nb_week = 20 # FOR TEST ONLY in order to get enough data                  #

    ###############################################################################

        #date_begin = date_end - timedelta(weeks=nb_week)
        date_begin = str(datetime.strptime(date_end,"%Y-%m-%d") - timedelta(weeks=nb_week))[0:10] 
        
        #date_end = date_end.strftime('%d/%m/%Y 00:00:00')
        date_end = datetime.strptime(date_end,"%Y-%m-%d").strftime('%d/%m/%Y 00:00:00')
        #date_begin = date_begin.strftime('%d/%m/%Y 00:00:00')
        date_begin = datetime.strptime(date_begin,"%Y-%m-%d").strftime('%d/%m/%Y 00:00:00')
        
        print("Date begin: ------------")
        print(date_begin)
        print("Date end: --------------")
        print(date_end)

        ## IMPLEMENT GLOBO REQUEST
        # Globo request: Demande de rapport des anomalies

        globo_request= {
            "apikey": api_key,
            "qry": {
                "idx": 1, #IdentifiantDeVotreChoixPourLierLaReponseAVotreDemande
                "mod": 1, # ModeDemandeVehiculeOuChauffeur: 1 vehicle, 2 driver
                "ids": id_query, # UnTableauDesIdentifiantsVehiculesOuChauffeursEnFonctionDeMod
                "deb": date_begin, # DateHeureFuseauClientDebutPeriodeDemandee
                "fin":date_end #DateHeureFuseauClientFinPeriodeDemandee
                }
            }

        headers = {"Content-type": "application/json"}
        r = requests.post(domaine, headers = headers, json = globo_request)
        result = json.loads(r.text)

        operations = []
        for t in result["res"]:
            operations.append({
                'id_mod':t['ent']['id'],
                'name_mod':t['ent']['lib'],
                'distance':t['ent']['odo'],
                'events':{
                    'harsh_braking':t['dec'],
                    'harsh_acceleration':t['acc'],
                    'over_speed':t['vit'],
                }
            })
        operations

        ## COMPUTE CONTINUOUS SCORING
        # Compute continous scoring
        scores = []
        for data in operations:
            
            distance = data['distance']
            harsh_acc = data['events']['harsh_acceleration']
            harsh_braking = data['events']['harsh_braking']
            over_speed = data['events']['over_speed']
            
            score_acc = (len(harsh_acc) * 0.3)*1000/(distance/1000*0.6214)
            score_decc = (len(harsh_braking) * 0.75)*1000/(distance/1000*0.6214)
            score_over_speed = (len(over_speed) * 1.8)*1000/(distance/1000*0.6214)
            
            scores.append({
                'id_mod':data["id_mod"],
                'score_acc':score_acc,
                'score_decc':score_decc,
                'score_over_speed': score_over_speed,
                'distance':distance,
                'vehicle':vehicle_to_consider[vehicle_to_consider["equipment_name_mycartracks"]==data["id_mod"]]["equipment_plant_number"].iloc[0]
            })
        scores= pd.DataFrame(scores)
        print(scores.head())

        ##Load into continuous scoring table
        continuous_scores =[]
        for i,v in scores.iterrows():
            continuous_scores.append({
                'driver_name': v["vehicle"], # since only vehicle request at the moment
                'driver_id':randint(1,2), #My phone number
                'hours_driven':v["distance"], # distance minimum vs. hours minimum
                'continuous_score_acceleration':v["score_acc"],
                'continuous_score_braking':v["score_decc"],
                'continuous_score_cruise_speed':v["score_over_speed"],
                'global_scores':(1/3*v["score_acc"])+(1/3*v["score_decc"])+(1/3*v["score_over_speed"]),
                'id_contest':contest_id,
                'batch_number':batch_number,
                'datetime':datetime.now(),
                'ranking':0
                
            })
        continuous_scores = pd.DataFrame(continuous_scores)
        print(continuous_scores.head())

        def score_on_base(base,serie,value):
            rebase = 100/base
            score = base - int(round(stats.percentileofscore(serie, value,'rank')/rebase, 0))
            if score < base/10:
                score = base/10
            return score

        continuous_scores["continuous_score_acceleration"] = continuous_scores["continuous_score_acceleration"].dropna().apply(lambda x: score_on_base(10, continuous_scores["continuous_score_acceleration"].dropna(),x))
        continuous_scores["continuous_score_braking"] = continuous_scores["continuous_score_braking"].dropna().apply(lambda x: score_on_base(10, continuous_scores["continuous_score_braking"].dropna(),x))
        continuous_scores["continuous_score_cruise_speed"] = continuous_scores["continuous_score_cruise_speed"].dropna().apply(lambda x: score_on_base(10, continuous_scores["continuous_score_cruise_speed"].dropna(),x))

        continuous_scores ["ranking"] = continuous_scores ["ranking"].rank(ascending = False)
        engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
        continuous_scores.to_sql(con=engine,name='ecodriving_continuous_results', if_exists='replace', index=False)
        

        ## EXECUTE SEND RESULTS with inputs contest_id, batch_number and batch_duration
        
        client = nexmo.Client(key="36112ed4", secret="50363491001c8b58")
        
        # Get continuous score
        engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
        query = "SELECT * FROM `ecodriving_continuous_results` WHERE `id_contest`="+str(contest_id)+" and `batch_number`="+str(batch_number)
        scores = pd.read_sql_query(query,con=engine)
        scores = scores.fillna(0)

        # Get phone numbers
        #drivers_id = str(tuple(list(set(scores["driver_id"]))))
        drivers_id = str(tuple(set(scores["driver_id"])))
        drivers_id

        engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
        query = "SELECT * FROM `drt_driver` WHERE `id` IN"+drivers_id
        phone_numbers = pd.read_sql_query(query,con=engine) 
        phone_numbers = phone_numbers [["id","tel","name","surname","pin"]]
        phone_numbers = phone_numbers[phone_numbers["tel"]!= ""]
        phone_numbers = phone_numbers.rename(columns={"id":"driver_id"})

        final_scores = pd.merge(scores, phone_numbers, how ="left", on ="driver_id")
        final_scores = final_scores.drop(["ranking","name","surname","id_contest","batch_number","driver_id","datetime"],1)

        final_scores = final_scores.dropna()
        final_scores["tel"] = final_scores["tel"]
        final_scores = final_scores[["driver_name","continuous_score_acceleration","continuous_score_braking","continuous_score_cruise_speed","tel"]]
        print("The final scores are:")
        print(final_scores.head())

        text_template = """
        Concours Ecoconduite Optimizer:

        Vos résultats à date sont :
        Accélération : %s sur 10
        Freinage : %s sur 10
        Maintien de la vitesse : %s sur 10

        Pour rappel, ce concours a débuté le XX et prendra fin le XX.

        Bonne route !
        """

        for t in final_scores.itertuples():
            text_sms = text_template % (t[2],t[3],t[4]) # For Ivory Coast
            print(text_sms)
            response = client.send_message({'from': 'Optimine', 'to': '0033' + t[5], 'text': text_sms}) 
            response = response['messages'][0]

            if response['status'] == '0':
                print('Sent message', response['message-id'])
                remaining_balance = response['remaining-balance']
            else:
                print('Error:', response['error-text'])

        print('Remaining balance is', remaining_balance)


        # updates the MySQL table "ecodriving_batch_contest" by setting "is_validated" to 1
        #query= "UPDATE `ecodriving_batch_contest` set `is_validated` = 1 where contest_id ="+int(v["contest_id"])+" and batch_number = "+int(v["batch_number"])
        #result = engine.execute(query)

    else:
        continue