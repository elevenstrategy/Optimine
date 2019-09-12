
# coding: utf-8

# In[3]:

import requests,json,sys,os,gc,time #, urllib.parse
from urllib.request import Request
import urllib.request
import numpy as np
import pandas as pd
from datetime import datetime,timedelta
from retrying import retry
from sqlalchemy import create_engine

"""
#NOTE WORKFLOW 
a deux fois par jour a 1h et 13h de J-7 a J-2

## TO DO:
@Julien : Input du script = equipment_plant_number d'un véhicule ayant un sensor i.e. ayant le chammp equipment_name_mycartracks non vide

"""

params = {
    "dateMin":"2018-03-13",
    "dateMax":"2018-03-13",
    "segmentation":None, # Input of the script: equipment_plant_number of a vehicle with a sensor i.e. with an equipment_name_mycartracks
    "minimum_time":5*60,
    "maximum_time":30*60,
    "minimum_speed":3,
    "queriesPerMin": 33, # 500 requests per 15 minutes vs. 150 previously
    "client_id":"ZH3KCrjWFcxj4preBB3nZBR7VZrv4MUxvV48jS4fzc5656SSXf.mycartracks.com", # bonikro
    #"client_id":"MMBthuZ9ycNRQ3pF51ReAZqzCrTz7h9tAtT9J8892JCNq6aEBJ.mycartracks.com", # sadiola
    "client_secret":"k4MVPYAxqvP8rdN5nJ7tZpfb3EHkJRpR",#bonikro
    #"client_secret":"UDxZSDcBEWF9eETktMTrJd9ybk1TzXHV",#Sadiola
    "client":"bonikro_drt_ecodriving_v4", # Bonikro
    #"client":"sadiola_drt_ecodriving_v4", #Sadiola
    #'url':"http://sadiola.optimizer-drt.com/app.php/oauth/v2/token?client_id=9_qn8px3goj80ck8ksco0gksc4gs0cosssckws0og48o0o04ok4&client_secret=2lqbngod8k84c08ok04o4wwsc8k4ocgwkw8ko8kswo44osswwo&grant_type=password&username=dashboard&password=elFdrt11", #sadiola,
    #'urlApiOptim':"http://sadiola.optimizer-drt.com/app.php/api/v2/vehicles", #sadiola
    'url':"http://bonikro.optimizer-drt.com/app.php/oauth/v2/token?client_id=9_qn8px3goj80ck8ksco0gksc4gs0cosssckws0og48o0o04ok4&client_secret=2lqbngod8k84c08ok04o4wwsc8k4ocgwkw8ko8kswo44osswwo&grant_type=password&username=dashboard&password=elFdrt11", #bonikro,
    'urlApiOptim':"http://bonikro.optimizer-drt.com/app.php/api/v2/vehicles" #bonikro
}


def idleTime(params): 

    try:
        begin = time.time()

        data = extract(params)
        #data = transform(params,data)

        load(params,data)

    except Exception as e: 
        exc_type, exc_obj, exc_tb = sys.exc_info()
        msg = 'Error: ' +str(e) +' L:'+str(exc_tb.tb_lineno)
        print(msg)
        pass


    print("duration:"+str(time.time()-begin)+"s")
    return True

#fonction qui sera éechangée entre Forepaas et Eleven
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
        date_from = params["dateMin"]
        date_to = params["dateMax"]
        date_format = "%Y-%m-%d %H:%M:%S"

        date_from_elargi = str(datetime.strptime(params["dateMin"],"%Y-%m-%d") + timedelta(days=-1))[0:10]
        print(date_from_elargi)
        date_to_elargi = str(datetime.strptime(params["dateMax"],"%Y-%m-%d") + timedelta(days=+1))[0:10]
        print("")
        print(date_to_elargi)

        # INPUT n°1 :
        minimum_time = params["minimum_time"] # minimum time in seconds : to be validated by the client

        # INPUT n°2 :
        maximum_time = params["maximum_time"] # maximum time in seconds : to be validated by the client

        # INPUT 3:
        minimum_speed = params["minimum_speed"] # unit = km/h. Goal: take into the fact that mycartracks is not 100% accurate on the speed


        # INPUT 4: list of vehicles in the API
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
        vehicle_Ids_1 ={} # sensor_number : id_number
        sensor_list = []
        for c in cars["data"]:    
            vehicle_Ids[c["id"]] = c["name"]
            vehicle_Ids_1[c["name"]]=c["id"]
            sensor_list.append(c["name"])

        # Get all the equipped vehicles

        trucks = []
        
        url=params['url']
 
        data = requests.get(url).text
        data = json.loads(data)
        access=data['access_token']

        #r=Request("http://bonikro.optimizer-drt.com/app.php/api/v2/vehicles",None,{"Authorization": "Bearer %s" %access})
        r=Request(params['urlApiOptim'],None,{"Authorization": "Bearer %s" %access})

        rep=urllib.request.urlopen(r)
        html=rep.read().decode('utf8')
        json_obj=json.loads(html)

        vehicle = []
        for t in json_obj :
            vehicle.append({
                    'owner' : t['owner'],
                    'equipment_plant_number' : t['equipment_plant_number'],
                    'equipment_name_mycartracks' : t['equipment_name_mycartracks']
                    }) 
        vehicle = pd.DataFrame(vehicle)
        vehicle=vehicle.loc[vehicle.equipment_name_mycartracks.isin(sensor_list)]
        vehicle_liste = vehicle
        if len(vehicle_liste)==0:
            DataOut=[]
            return DataOut       

        ## Output initialization
        def get_idle_time(vehicle_to_consider,sensor_to_consider,carId):
            Ouput = {}
            print("---------------------------The vehicle considered is")
            print(vehicle_to_consider)
            # # Import tracks and trackpoints of the selected period from mycartracks for the concerned vehicle
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


            # In[16]:

            #Remove tracks that are smaller than [10] kms because of bugs on some vehicles  
            clean_tracks = []
            for t in tracks['data']:  
                t['carName'] = vehicle_to_consider
                t['startDate'] = datetime.fromtimestamp(t["startTime"]/1000).strftime('%Y-%m-%d %H:%M:%S')
                t['endDate'] = datetime.fromtimestamp(t["endTime"]/1000).strftime('%Y-%m-%d %H:%M:%S')
                if t["totalDistance"] >= (minimalTrackDistanceKmToConsider):
                    clean_tracks.append(t)

            print(len(clean_tracks),"tracks after cleaning tiny tracks")
            if len(clean_tracks)==0:
                print("no clean tracks found")
                Output={}
                print("----------------------Idle Time for the below vehicle")
                print(Output)
                return Output

            df_clean_tracks = pd.DataFrame(clean_tracks)


            
            ##  Compute totalTime and movingTime: in hour
            df_clean_tracks = df_clean_tracks[["carName","totalTime","movingTime","endTime","startTime","startDate","endDate","id","carId"]]


            df_clean_tracks["date_start"] = pd.to_datetime(df_clean_tracks["startDate"]).apply(lambda x: x.date())
            df_clean_tracks["date_end"] = pd.to_datetime(df_clean_tracks["endDate"]).apply(lambda x: x.date())
            
             # Get TotalTime et MovingTime in hours
            df_clean_tracks.loc[:,"totalTime"] = (df_clean_tracks.loc[:,"totalTime"]/1000)/3600
            df_clean_tracks.loc[:,"movingTime"] = (df_clean_tracks.loc[:,"movingTime"]/1000)/3600

            df_clean_tracks.date_start=df_clean_tracks.date_start.apply(lambda x: np.datetime64(pd.to_datetime(pd.to_datetime(x).tz_localize('UTC').tz_convert('Africa/Abidjan'))).astype(datetime))
            df_clean_tracks.date_end=df_clean_tracks.date_end.apply(lambda x: np.datetime64(pd.to_datetime(pd.to_datetime(x).tz_localize('UTC').tz_convert('Africa/Abidjan'))).astype(datetime))

            df_clean_tracks["startTime"]= df_clean_tracks["startTime"].apply(lambda x :datetime.fromtimestamp(x/1000))
            df_clean_tracks["endTime"]= df_clean_tracks["endTime"].apply(lambda x :datetime.fromtimestamp(x/1000))

            df_clean_tracks.startTime=df_clean_tracks.startTime.apply(lambda x: np.datetime64(pd.to_datetime(pd.to_datetime(x).tz_localize('UTC').tz_convert('Africa/Abidjan'))).astype(datetime))
            df_clean_tracks.endTime=df_clean_tracks.endTime.apply(lambda x: np.datetime64(pd.to_datetime(pd.to_datetime(x).tz_localize('UTC').tz_convert('Africa/Abidjan'))).astype(datetime))

            
            df_clean_tracks = df_clean_tracks[df_clean_tracks ["totalTime"]<24]
            if len(df_clean_tracks)==0:
                print("no clean tracks found")
                Output={}
                print("----------------------Idle Time for the below vehicle")
                print(Output)
                return Output

            # Consider the case where the track goes from the day J-1 to the day J0
            df_clean_tracks_corrected = []
            for i,v in df_clean_tracks.iterrows():
                if v["date_start"] != v["date_end"]:
                    # Divide the track into two tracks (one for each day)
                    date_start_track_duration = float(24 -(v["startTime"].hour + v["startTime"].minute/60 + v["startTime"].second/3600))   
                    date_end_track_duration = float(v["endTime"].hour + v["endTime"].minute/60 + v["endTime"].second/3600) 
                    ratio = float(date_start_track_duration/(date_start_track_duration+date_end_track_duration))

                    # Register the part of the track of date_start
                    df_clean_tracks_corrected.append({
                            "carName":v["carName"],
                            "totalTime": ratio*v["totalTime"],
                            "movingTime": ratio*v["movingTime"],
                            "date":v["date_start"]
                        })

                    # Register the part of the track of date_end


                    df_clean_tracks_corrected.append({
                                    "carName":v["carName"],
                                    "totalTime": (1-ratio)*v["totalTime"],
                                    "movingTime":(1-ratio)*v["movingTime"],
                                    "date":v["date_end"]
                                })        
                else:
                    df_clean_tracks_corrected.append({
                            "carName":v["carName"],
                            "totalTime":v["totalTime"],
                            "movingTime": v["movingTime"],
                            "date":v["date_start"]
                        })
            df_clean_tracks_corrected = pd.DataFrame(df_clean_tracks_corrected)

            track_per_vehicle = df_clean_tracks_corrected.groupby(["carName","date"]).sum()
            track_per_vehicle["nospeedTime"] = track_per_vehicle["totalTime"] - track_per_vehicle["movingTime"]
            track_per_vehicle = track_per_vehicle.reset_index()
            

            #Import trackpoints from each consistent track (more than 10 km and less than 24 hours)  
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
                    v['carName'] = vehicle_to_consider
                all_trackpoints += trackpoints['data']

            df = pd.DataFrame(all_trackpoints)
            df_trackpoints = df
            df_trackpoints ["time"] = df_trackpoints["time"].apply(lambda x :datetime.fromtimestamp(x/1000))


            # # Compute idle time from trackpoints data
            dict_nospeed_time_per_vehicle ={}

            car = df_trackpoints.sort_values(["time"],ascending = [True]).reset_index(drop=True)
            # Compute ide time
            idle_time_per_vehicle = []
            time_per_vehicle = []
            last_car = ""
            last_track_ID = ""
            count = 0

            for idx, df in car.iterrows():
                vehicle = df["carName"] 
                time2 = df["time"]
                speed = df["speed"]
                trackID = df["trackId"]

                # Consider all the points of the same track where speed is "close to zero"    
                if (speed <= minimum_speed) & (trackID == last_track_ID) & (idx<(len(car)-1)):
                    count = count + 1
                    time_per_vehicle.append(time2)        

                else : 
                    if count > 1 :
                        idle_time = (time_per_vehicle[-1] - time_per_vehicle[0]).seconds

                        idle_time_per_vehicle.append({
                            "carName" : last_car,
                            "track_ID": last_track_ID,    
                            "idle_time" : idle_time,
                            "time_start" : time_per_vehicle[0],
                            "time_end" : time_per_vehicle[-1]    
                            })

                        count = 0
                        time_per_vehicle = []

                    count = 0
                    time_per_vehicle = []

                    if speed <= minimum_speed :
                        count = count +1                 
                        time_per_vehicle.append(time2)
                    else : 
                        continue

                last_car = vehicle
                last_track_ID = trackID

            nospeed_time_per_vehicle  = pd.DataFrame(idle_time_per_vehicle) #Idle time in seconds

            # Split Idle time betwwen the different date
            nospeed_time_per_vehicle_corrected = []
            for i,v in nospeed_time_per_vehicle.iterrows():
                if v["time_end"].date() != v["time_start"].date():
                    # Divide the idle time in two
                    date_start_idle_duration =  float(24 -(v["time_start"].hour + v["time_start"].minute/60 + v["time_start"].second/3600))   
                    date_end_idle_duration = float((v["time_end"].hour + v["time_end"].minute/60 + v["time_end"].second/3600))
                    ratio = float(date_start_idle_duration/(date_start_idle_duration+date_end_idle_duration))

                    # Register the part of the idle of date_start
                    nospeed_time_per_vehicle_corrected.append({
                            "carName":v["carName"],
                            "idle_time":ratio*v["idle_time"],
                            "time_end":pd.to_datetime(datetime(v["time_start"].year,v["time_start"].month,v["time_start"].day,23,59,59)),
                            "time_start": v["time_start"],
                            "track_ID":v["track_ID"]
                        })

                    # Register the part of the track of date_end
                    nospeed_time_per_vehicle_corrected.append({
                            "carName":v["carName"],
                            "idle_time":(1-ratio)*v["idle_time"],
                            "time_end":v["time_end"],
                            "time_start":pd.to_datetime(datetime(v["time_end"].year,v["time_end"].month,v["time_end"].day,0,0,0)),
                            "track_ID":v["track_ID"]
                        })

                else:
                    nospeed_time_per_vehicle_corrected.append({
                            "carName":v["carName"],
                            "idle_time":v["idle_time"],
                            "time_end":v["time_end"],
                            "time_start": v["time_start"],
                            "track_ID":v["track_ID"]
                        })
            nospeed_time_per_vehicle_corrected = pd.DataFrame(nospeed_time_per_vehicle_corrected)
            nospeed_time_per_vehicle = nospeed_time_per_vehicle_corrected

            # Get the day of the different tracks on the periods considered
            nospeed_time_per_vehicle ["date"] = nospeed_time_per_vehicle["time_end"].apply(lambda x: x.date()) 
            nospeed_time_per_vehicle = nospeed_time_per_vehicle[["carName","idle_time","track_ID","date","time_start","time_end"]]
            if "idle_time" in nospeed_time_per_vehicle : 
                
                # Get idle time per track
                for i,v in nospeed_time_per_vehicle.iterrows():
                    if ((v["idle_time"]>minimum_time) & (v["idle_time"]<maximum_time)) == True:
                        continue
                    else:
                        nospeed_time_per_vehicle["idle_time"].iloc[i] = 0
                
                # Delete the last idle time of each track:
                nospeed_time_per_vehicle_last_idle = nospeed_time_per_vehicle.groupby(["track_ID"]).last()
                nospeed_time_per_vehicle_last_idle = nospeed_time_per_vehicle_last_idle.groupby("date").sum().rename(columns = {"idle_time" :"last_idle_time" })/3600 # last idle time in hours
                nospeed_time_per_vehicle_last_idle = nospeed_time_per_vehicle_last_idle.reset_index()
                
                nospeed_time = nospeed_time_per_vehicle.groupby(["date"]).sum()/3600 # idle time in hours
                nospeed_time = nospeed_time.drop("track_ID", axis = 1)
                nospeed_time = nospeed_time.reset_index()
                    
                nospeed_time = pd.merge(nospeed_time,nospeed_time_per_vehicle_last_idle, how = "left", on =["date"])
                nospeed_time = nospeed_time.fillna(0)
                nospeed_time ["true_idle"] = nospeed_time ["idle_time"] - nospeed_time ["last_idle_time"]
                #nospeed_time= nospeed_time.reset_index()
                nospeed_time = nospeed_time[["date","true_idle"]]

                #print("Idle time per vehicle ----------------------")
                #print(nospeed_time)
                track_per_vehicle.date=track_per_vehicle.date.apply(lambda x: str(x)[0:10])
                nospeed_time.date=nospeed_time.date.apply(lambda x: str(x))

                # Join the informations from tracks (track_per_vehicle) and trackpoints (nospeed_time)
                output_per_vehicle = pd.merge(track_per_vehicle,nospeed_time) 
                print("output per vehicle: ---------------------")
                print(output_per_vehicle)

                ## Return Output 
                for idx,g in output_per_vehicle.iterrows():
                    g["date"] = str(g["date"])
                    if g["date"] >= date_from and g["date"] <= date_to: 
                        Output= {
                           "vehicle":g["carName"],
                           "date": g["date"],
                           "idletime_hours":g["true_idle"],
                           "totaltime_hours":g["totalTime"]
                           #"nospeedTime":g["nospeedTime"]
                           }
                        print("----------------------Idle Time for the below vehicle")
                        print(Output)
                        return Output
                    else:
                        Ouput ={}

            else:
                print("no idle time detected")
                Output={}
                return Output
            
            Output={}
            return Output

        DataOut=[]
        idle_time =[]
        for i,v in vehicle_liste.iterrows():
            sensor_to_consider = v["equipment_name_mycartracks"]
            vehicle_to_consider = v["equipment_plant_number"]
            carId = vehicle_Ids_1[sensor_to_consider]
            idle_time = get_idle_time(vehicle_to_consider,sensor_to_consider,carId)
            if len(idle_time)>0:
                DataOut.append(idle_time)
        
        print("--------------------------Final results for Idle time")
        print(DataOut)
        gc.collect()        
        return DataOut

    except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            msg = 'Error: ' +str(e) +' L:'+str(exc_tb.tb_lineno)
            print(msg)
            gc.collect()
            raise Exception(msg)


def load(params,data):

    data = pd.DataFrame(data)
    data.to_csv("Idle Time Bonikro vagrant.csv",sep=";",index=False,header=True)
    print(data)



idleTime(params)

