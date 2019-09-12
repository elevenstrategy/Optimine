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
import pytz 
from dateutil import tz

params = {
    "day_date":time.strftime('%Y-%m-%d',time.localtime()),
    "segmentation":None,
    "queriesPerMin": 33, # 500 requests per 15 minutes vs. 150 previously
    "message":"Sucess",
    "client_id":"ZH3KCrjWFcxj4preBB3nZBR7VZrv4MUxvV48jS4fzc5656SSXf.mycartracks.com", # bonikro
    "client_secret":"k4MVPYAxqvP8rdN5nJ7tZpfb3EHkJRpR",#bonikro
    "client":"bonikro_drt_v5", # Bonikro
    "apiKey":"qaTZVySF4zQ3sUBM6rJ8NApVuXPw7jeA", # Bonikro
}



#to get the complete list of timezones to change the timezone, please do the following:
#for tz in pytz.all_timezones:
#    print(tz)

def trackpoints_import(params):
    try:
        begin = time.time()
    
        data = extract(params)
        #data = transform(params,data)

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
        "log_type":"CRON_mycartracks_data_import"
        })
    logs = pd.DataFrame(logs)
    engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
    logs.to_sql(con=engine,name='ecodriving_logs_CRON', if_exists='append', index=False)

    print("duration:"+str(time.time()-begin)+"s")
    return True




#to get the complete list of timezones to change the params['timezone'] parameter, please do the following:
#for tz in pytz.all_timezones:
#    print(tz)

def trackpoints_import(params):
    try:
        begin = time.time()
    
        data = extract(params)
        #data = transform(params,data)

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
        "log_type":"CRON_mycartracks_data_import"
        })
    logs = pd.DataFrame(logs)
    engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
    logs.to_sql(con=engine,name='ecodriving_logs_CRON', if_exists='append', index=False)

    print("duration:"+str(time.time()-begin)+"s")
    return True


def extract(params):
    @retry(stop_max_attempt_number=3)
    def doGetRequest(url, params_request):
        #  Get a new token available for one hour
        client_id = params["client_id"]
        client_secret = params["client_secret"]
        OAUTH_TOKEN_URL = "https://www.mycartracks.com/oauth/token"
        data = {"client_id": client_id,"client_secret": client_secret,"grant_type": "client_credentials"}
        headers_token = {}
        r = requests.post(OAUTH_TOKEN_URL, data=data, headers=headers_token)
        result = json.loads(r.text)
        access_token = result["access_token"]
        headers = {"Content-type": "application/json","Authorization": "Bearer " + access_token}
        
        # Process the request
        res=requests.get(url, params = params_request,headers = headers)
        if res.status_code==200: 
            return res
        else:
            r = requests.post(OAUTH_TOKEN_URL, data=data, headers=headers_token)
            result = json.loads(r.text)
            access_token = result["access_token"]
            headers = {"Content-type": "application/json","Authorization": "Bearer " + access_token}
            time.sleep(1)

    try:
        timer = 1
        if "queriesPerMin" in params : 
            timer = 60/params["queriesPerMin"]

        urlApi = "https://www.mycartracks.com"

        minimalTrackDistanceKmToConsider = 10
        previous_i = 0
        # maxtracks=  5 : why to limit the number of tracks to 5 ?

        # Requests of 3 days so that there is an overlapp if the datas don't synchronize well
        date_from = params["day_date"]
        date_to = params["day_date"]
        date_format = "%Y-%m-%d %H:%M:%S"

        date_from_elargi = str(datetime.strptime(params["day_date"],"%Y-%m-%d") + timedelta(days=-1))[0:10] # normalement on met - 1 ici
        date_to_elargi = str(datetime.strptime(params["day_date"],"%Y-%m-%d") + timedelta(days=+0))[0:10]
        print(date_from_elargi)
        print(date_to_elargi)

        # INPUT 4: list of vehicles in the API
        #Establish connection to MyCarTracks API

        #Establish connection to MyCarTracks API
        params_request = {"paid": True}
        url = urlApi+"/services/rest/v2/vehicles"
        res = doGetRequest(url,params_request)
        time.sleep(timer) # timer to slow down queries
        if res.status_code!=200: 
            print("error accessing API")
        else:
            print("sucess accesing API")  
        cars = res.json()
        print(len((cars["data"])),"cars found in API")

        # Associate for each sensor id its name
        vehicle_Ids = {} # id_number : sensor_number
        sensor_list = []
        sensor_list_id =[]
        for c in cars["data"]:    
            vehicle_Ids[c["id"]] = c["name"]
            sensor_list.append(c["name"])
            sensor_list_id.append(c["id"])

        # Associate for each its sensor id its vehicle name
        vehicle_Ids_1 = {} #id_number : equipment_name_mycartracks
        vehicle_Ids_2 ={} # equipment_name_mycartracks:id_number

        for c in cars["data"]:
            # Get the respective name of the sensors
            sensor = c["name"]
            engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
            query = "SELECT `equipment_plant_number`,`equipment_name_mycartracks` FROM `drt_vehicle` WHERE `equipment_name_mycartracks`='"+sensor+"'"
            vehicle = pd.read_sql_query(query,con=engine)
            if vehicle.empty ==True:
                vehicle_Ids_1[c["id"]] = c["name"]
                vehicle_Ids_2[c["name"]] = c["id"]
            else:
                vehicle_Ids_1[c["id"]] = vehicle["equipment_plant_number"][0]
                vehicle_Ids_2[vehicle["equipment_plant_number"][0]] = c["id"]

        # Get all the equipped vehicles
        engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])

        trucks = []
        for sensor in sensor_list :
            engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
            query = "SELECT `owner`,`equipment_plant_number`,`id`,`equipment_name_mycartracks` FROM `drt_vehicle` WHERE `equipment_name_mycartracks`='"+sensor+"'"
            vehicle = pd.read_sql_query(query,con=engine)
            if vehicle.empty ==True:
                continue
            else:
                trucks.append({
                    "owner": vehicle["owner"].iloc[0],
                    "equipment_plant_number":vehicle["equipment_plant_number"].iloc[0],
                    "equipment_name_mycartracks":vehicle["equipment_name_mycartracks"].iloc[0],
                    "vehicle_id":vehicle["id"].iloc[0]
                    })       

        vehicle = pd.DataFrame(trucks)
        print(vehicle)

        carId = None
        vehicle_to_consider = None

        if "segmentation" in params and params["segmentation"] is not None: 
            try:
                vehicle_to_consider = vehicle["equipment_plant_number"].iloc[0]
                carId = vehicle_Ids_2[vehicle_to_consider]
                print(carId)
            except IndexError:
                print(carId)
                pass

        #Loading all tracks in selected date range
        params_request = {
            #'page': 0,
            "limit":1000,
            'omitPage': True,
            #'measurementUnit': "KILOMETER",
            #'humanReadable': "false",
            'startTime': int(time.mktime(datetime.strptime(date_from_elargi + ' 00:00:00', date_format).timetuple())),
            'endTime': int(time.mktime(datetime.strptime(date_to_elargi + ' 23:59:59', date_format).timetuple())),
        }

        if carId:
            params_request["carId"] = carId

        res = doGetRequest(urlApi+"/services/rest/v2/tracks", params_request)
        if res.status_code!=200: 
            print(res.status_code)
            print(res.text)
            print("error accessing API")

        tracks = res.json()
        # tracks = json.loads(res.decode())
        print(len(tracks['data']),"tracks found in API for those dates") 

        #Remove tracks that are smaller than [10] kms because of bugs on one vehicule T56, which counts small tracks 
        clean_tracks = []
        for t in tracks["data"]:
            #distance = t["totalDistance"].split(" ")
            if t['carId'] in(sensor_list_id):
                t['carName'] = vehicle_Ids_1[t['carId']] 
                if t["totalDistance"] >= (minimalTrackDistanceKmToConsider):
                    clean_tracks.append(t)
            else:
                pass

        print(len(clean_tracks),"tracks after cleaning tiny tracks")
        if len(clean_tracks)==0:
            print("no clean tracks found")
            DataOut =[]

        df_clean_tracks = pd.DataFrame(clean_tracks)
        df_clean_tracks.head(2)
        #clean_tracks


        #Import trackpoints from each consistent track (more than 10 km and less than 24 hours)  
        # VERY LONG
        begin_trackpoints = time.time()
        
        # VERY LONG
        i=0
        previous_i=0
        all_trackpoints = []

        for i,t in df_clean_tracks.iterrows():
            if i < previous_i: #To start where it previously stopped
                continue
            print("get trackpoints of track ",i)
            params_request = {
                'omitPage': True,
                'limit':86400 #24 hours maximum
                #'measurementUnit': "KILOMETER",
                #'humanReadable': "false",
            }
            res = doGetRequest(urlApi+"/services/rest/v2/trackpoints/"+str(t["id"]), params_request)
            time.sleep(timer) #timer to slow down queries 

            if res.status_code!=200: 
                print(res.status_code,res.text)
                print("error accessing API")
                exit()
            trackpoints = res.json()
            print(len(trackpoints['data']),"trackpoints found")
            for v in trackpoints['data']:
                v['trackId'] = t["id"]
                v['carName'] = vehicle_Ids_1[t["carId"]]
            all_trackpoints += trackpoints['data']

        df = pd.DataFrame(all_trackpoints)

        df_trackpoints = df

        # Turn RAW trackpoints into CLEAN trackpoints
        #df_trackpoints_original = df.drop("source", axis=1)
        df_trackpoints_original = df
        df_trackpoints_original ["time"] = df_trackpoints_original["time"].apply(lambda x :datetime.fromtimestamp(x/1000))
        df_trackpoints_original = df_trackpoints_original.rename(columns = {'time':'datetime', 'carName': 'vehicleName','id':'id_mycartracks'})

        #df_trackpoints_original = df_trackpoints_original.sort_values("datetime", ascending = True)
        print(df_trackpoints_original.head())
        print(df_trackpoints_original.tail())
        
        duration_trackpoints = time.time()-begin_trackpoints

        # Import goefnces data
        @retry(stop_max_attempt_number=3)
        def doGetRequest_APIv0(url):
            res=requests.get(url)
            if res.status_code==200: 
                return res
            else:
                time.sleep(1)
                raise Exception("cant access "+url+" / "+res.text)
        
        begin_geofences = time.time()
        # Import gofences data with API v1
        urlApi = "https://www.mycartracks.com"
        apiKey = params["apiKey"]
        params = {
            'pageSize':100000,
            'dateFrom': int(time.mktime(datetime.strptime(date_from_elargi + ' 00:00:00', date_format).timetuple())),
            'dateTo': int(time.mktime(datetime.strptime(date_to_elargi + ' 23:59:59', date_format).timetuple())),
            'type':'vehicleGeofenceDetailed'            
        }

        if carId:
            params["carId"] = carId

        paramsString = []
        for k in params:
            paramsString.append( str(k)+"="+str(params[k]) )
        paramsString=  "&".join(paramsString)

        url = "/services/rest/v1/timeReport/vehicles.json?apiKey="+ apiKey + "&" + paramsString
        print(url)
        res = doGetRequest_APIv0(urlApi+url)
        time.sleep(timer) #timer to slow down queries 
        if res.status_code!=200: 
            print(res.status_code)
            print(res.text)
            print("error accessing API")

        geofence = res.json()
        print(len(geofence['geofenceAccess']),"geofences found in API for those dates")
        if len(geofence['geofenceAccess'])==0:
            print("no clean geofences found")
            geofence_df = pd.DataFrame(geofence['geofenceAccess'])
            geofence_df["vehicleName"]=[]
        else:
            geofence_df = pd.DataFrame(geofence['geofenceAccess'])
            geofence_df = geofence_df.sort_values(["entryTime"], ascending = True)
            geofence_df["exitTime"] = pd.to_datetime(geofence_df["exitTime"])
            geofence_df["entryTime"] = pd.to_datetime(geofence_df["entryTime"])
            # Fonction qui change le nom du véhicule de, par exemple, "sensor12" à  "DT05" - elle doit matcher le nom initial du DF geofence_df avec la ligne correspondante dans le DF vehicle qui contient aussi l'autre nom
            def g(x):
                val = vehicle.loc[vehicle["equipment_name_mycartracks"] == x]["equipment_plant_number"]
                if len(val) == 0:
                    return x
                else:
                    return val.iloc[0]
            geofence_df["vehicleName"] = geofence_df["vehicleName"].apply(g)

            #geofence_df["vehicleName"] = geofence_df["vehicleName"].apply(lambda x: vehicle.loc[vehicle["equipment_name_mycartracks"]==x]["equipment_plant_number"].iloc[0])
            geofence_df = geofence_df[["entryTime","exitTime","vehicleName","geofenceName","averageSpeed","distanceDriven","maximumSpeed","movingTime","serviceTime","stoppedInside"]]                        
        
        duration_geofences = time.time()-begin_geofences

        # Print OUTPUT
        DataOut1 =[]
        DataOut2 =[]
        DataOut3 =[]
        DataOut4 =[]
        DataOut5 = []
        
        
        DataOut1 = df_trackpoints_original
        if len(df_trackpoints_original)>0:
            DataOut1.datetime = DataOut1.datetime.apply(lambda x: np.datetime64(pd.to_datetime(x.tz_localize('UTC').tz_convert('Africa/Abidjan'))).astype(datetime))
        DataOut2 = geofence_df
        if len(geofence_df)>0:
            DataOut2.entryTime=geofence_df.entryTime.apply(lambda x: np.datetime64(pd.to_datetime(x.tz_localize('UTC').tz_convert('Africa/Abidjan'))).astype(datetime))
            DataOut2.exitTime=geofence_df.exitTime.apply(lambda x: np.datetime64(pd.to_datetime(x.tz_localize('UTC').tz_convert('Africa/Abidjan'))).astype(datetime))
        DataOut3 = duration_trackpoints
        DataOut4 = duration_geofences
        DataOut5 = clean_tracks
        
        
        
        DataOut =[DataOut1,DataOut2,DataOut3,DataOut4, DataOut5]
        gc.collect()
        return DataOut

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        msg = 'Error: ' +str(e) +' L:'+str(exc_tb.tb_lineno)
        print(msg)
        gc.collect()
        raise Exception(msg)

        
def load(params,data):
    logs = []

    trackpoints = pd.DataFrame(data[0])
    print(trackpoints.head())
    engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
    
    #trackpoints=clean_df_db_dups(trackpoints, 'ecodriving_trackpoints', engine, dup_cols=list(trackpoints.columns), filter_continuous_col='datetime', filter_categorical_col=None)
                            
    trackpoints.to_sql(con=engine,name='ecodriving_trackpoints', if_exists='append', index=False)

    logs.append({
        "date_begin": params["day_date"],
        "date_end": time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()),
        "number_of_points":len(trackpoints),
        "number_of_tracks":len(data[4]),
        "number_of_vehicles":len(set(list(trackpoints["vehicleName"]))),
        "log_type":"ecodriving_trackpoints",
        "request_duration":data[2]
        })

    geofences = pd.DataFrame(data[1])
    print(geofences.head())
    engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
    
    #geofences=clean_df_db_dups(geofences, 'ecodriving_geofences', engine, dup_cols=list(geofences.columns), filter_continuous_col=['entryTime','exitTime'], filter_categorical_col=None)
    
    geofences.to_sql(con=engine,name='ecodriving_geofences', if_exists='append', index=False)
    logs.append({
        "date_begin": params["day_date"],
        "date_end": time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()),
        "number_of_points":len(geofences),
        "number_of_tracks":"n.a.",
        "number_of_vehicles":len(set(list(geofences["vehicleName"]))),
        "log_type":"ecodriving_geofences",
        "request_duration":data[3]
        })
    logs = pd.DataFrame(logs)
    engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
    logs.to_sql(con=engine,name='ecodriving_logs_trackpoints_and_geofences', if_exists='append', index=False)


trackpoints_import(params)


