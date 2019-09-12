# coding: utf-8

# In[87]:

import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import requests,json,sys,os,gc,time
from retrying import retry
from sqlalchemy import create_engine
from scipy.spatial import procrustes
from scipy import stats
import math
from sklearn.linear_model import LinearRegression, Lasso
import peakutils
import matplotlib
import matplotlib.pyplot as plt


params = {
    "day_date": str(datetime.strptime(time.strftime('%Y-%m-%d',time.localtime()),"%Y-%m-%d") + timedelta(days=-1))[0:10], # the end date for the score calculation, which should be one day before (Sunday) scoring is done (Monday)
    "segmentation":None,
    "id_logs":"App4mob", 
    "id_fms": "App4mob",
    "score_type":str(sys.argv[4]), # 'Haulage', "DT"
    "contest_id": int(sys.argv[1]), # Get contest_id from external argument
    "batch_number":int(sys.argv[2]), # Get batch_number from external argument
    "batch_duration":int(sys.argv[3]), # Get batch_duration from external argument
    # "score_type":"DT", # 'Haulage', "DT"
    # "contest_id": 17, 
    # "batch_number":1,
    # "batch_duration":7,
    "message":"Test sucessful",
    "client_drt":"bonikro", #APP4MOB API
    "client":"bonikro_drt_v5",
    "Speed_maintenace_geofence": "Cruise speed"
}

print(params)

def main_scoring(params):
    try:
        begin = time.time()
        data = extract(params)
        
        data = transform(params,data)
        load(params,data) # comment to ensure you don't upload results to the database

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
        "log_type":"CRON_batch_contest_main_scoring"
        })
    logs = pd.DataFrame(logs)
    engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
    logs.to_sql(con=engine,name='ecodriving_logs_CRON', if_exists='append', index=False)

    print("duration:"+str(time.time()-begin)+"s")
    return True

def extract(params):
    try:
        timer = 1
        if "queriesPerMin" in params : 
            timer = 60/params["queriesPerMin"]

        date_from = params["day_date"]
        date_to = params["day_date"]
        date_format = "%Y-%m-%d %H:%M:%S"
        
        date_from_elargi = str(datetime.strptime(params["day_date"],"%Y-%m-%d") + timedelta(days=-params["batch_duration"]))[0:10]
        date_to_elargi = str(datetime.strptime(params["day_date"],"%Y-%m-%d"))[0:10]

        # Transform date into datetime
        startDate = pd.to_datetime(date_from_elargi)
        endDate= pd.to_datetime(date_to_elargi) + timedelta(hours = +23, minutes =+59, seconds=+59)

        print("Start date", startDate)
        print("End date", endDate)

        @retry(stop_max_attempt_number=3)
        def doGetRequest(url):
            res=requests.get(url)
            if res.status_code==200: 
                return res
            else:
                time.sleep(1)
                raise Exception("cant access "+url+" / "+res.text)

 

        # Get perimeter of vehicle to consider for the selected contest
        engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
        # the query is written to ensure the selection of vehicles that correspond only to the desired contest 
        query = "SELECT v.id, v.equipment_plant_number FROM `ecodriving_contests_has_vehicles` as chv, `drt_vehicle` as v WHERE contest_id ="+str(params["contest_id"])+" and v.id = chv.vehicle_id"
        vehicle_to_consider = pd.read_sql_query(query,con=engine) # Dataframe with "id" and "equipment_plant_number"
        
        vehicle_to_consider_id = str(tuple(list(set(vehicle_to_consider["id"]))))
        vehicle_to_consider_name =  str(tuple(list(set(vehicle_to_consider["equipment_plant_number"]))))
        
        print("Vehicles to consider for the contest "+str(params["contest_id"])+" in the batch "+str(params["batch_number"]))
        print(set(vehicle_to_consider["equipment_plant_number"]))
       

        # Import DRIVERS LOGS from BDD logs with INPUT = (vehicle_to_consider, startDate, endDate)
        if params["id_logs"] =="App4mob": 
            engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
            # this query is written to get data on drivers when they connected and when they disconnected 
            query= "SELECT log.id, log.driver_id,d.name, d.surname,v.equipment_plant_number, log.action, log.date FROM `appid_operation` as log LEFT JOIN `drt_vehicle` as v ON v.id = log.vehicle_id LEFT JOIN `drt_driver` as d ON d.id = log.driver_id WHERE date BETWEEN '"+ str(startDate)+"' and '"+str(endDate)+"'ORDER by log.date asc"
            drivers_data = pd.read_sql_query(query,con=engine) # DataFrame with "id", "equipment_plant_number", "action", "date","driver_name"
            drivers_data["action"] = drivers_data["action"].apply(lambda x: int(x))
            
            # we consider data only for the vehicles that ought to be considered
            drivers_data = drivers_data[drivers_data["equipment_plant_number"].isin(list(vehicle_to_consider["equipment_plant_number"]))]
            drivers_data = drivers_data[(drivers_data["date"] > startDate) & (drivers_data["date"] <= endDate)]
            drivers_data["driver_name"]= drivers_data["surname"] +" "+drivers_data["name"]

            # we get the required data on names of drivers
            id_to_consider =  str(tuple(list(set(drivers_data["driver_id"]))))
            query= "SELECT `name`,`surname`,`id` FROM `drt_driver` WHERE `id` IN "+id_to_consider
            drivers_names = pd.read_sql_query(query,con=engine) # DataFrame with "id", "equipment_plant_number", "action", "date","driver_name"
            drivers_names["driver_name"] = drivers_names["surname"] +" "+drivers_names["name"]

            drivers_data =  drivers_data[["action","date","id","equipment_plant_number","driver_id","driver_name"]]
            print("drivers_data: DataFrame with drivers log data")
            print("----- Number of rows: " + str(drivers_data.shape[0]))
            print(drivers_data.head())
            print(drivers_data.shape)
            print(set(drivers_data["equipment_plant_number"]))
            print(str(len(set(drivers_data["equipment_plant_number"])))+" vehicles")
            print(set(drivers_data["driver_name"]))
            print(str(len(set(drivers_data["driver_name"])))+" names")

        # Filters out vehicle with LOGS only
        vehicle_to_consider_name = str(tuple(list(set(drivers_data["equipment_plant_number"]))))

        # Import GEOFENCES data from BDD Geofences with vehicle in vehicle perimeter and date in the time period
        if True:
            all_geofence_data= pd.read_sql("select * from `ecodriving_geofences` where `entryTime` BETWEEN'"+str(startDate)+"' and '"+str(endDate)+"' AND `vehicleName` IN"+ vehicle_to_consider_name+";", con=engine)
            all_geofence_data = all_geofence_data.drop_duplicates()
            print("all_geofence_data: DataFrame with all Geofence data")
            print("----- Number of rows: " + str(all_geofence_data.shape[0]))
            print(all_geofence_data.head())
            print("We have geofence data for the following vehicles:", set(all_geofence_data['vehicleName']))

    
        # Import TRACKPOINTS data BDD Trackpoints with vehicle in vehicle perimeter and date in the time period 
        if True:
            df_trackpoints = pd.read_sql("select * from `ecodriving_trackpoints` WHERE `datetime` BETWEEN'"+str(startDate)+"' and '"+str(endDate)+"' AND `vehicleName` IN"+ vehicle_to_consider_name+";", con=engine)
            df_trackpoints = df_trackpoints.sort_values(["datetime","vehicleName"], ascending = [True, True])
            df_trackpoints = df_trackpoints.drop_duplicates()
            print("trackpoints_data: DataFrame with trackpoints data")
            print("----- Number of rows: " + str(df_trackpoints.shape[0]))
            print(df_trackpoints.head())

        DataOut1 = []
        DataOut2 = []
        DataOut3 = []
        DataOut4 = []
        DataOut5 = []
        DataOut6= []
        DataOut1 = drivers_data
        DataOut2 = drivers_names
        # DataOut3 = df_fms_data
        DataOut4 = df_trackpoints
        DataOut5 = all_geofence_data
        DataOut6 = vehicle_to_consider
        DataOut= []
        DataOut =[DataOut1,DataOut2,DataOut3, DataOut4, DataOut5, DataOut6]
        gc.collect()
        return DataOut

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        msg = 'Error: ' +str(e) +' L:'+str(exc_tb.tb_lineno)
        print(msg)
        gc.collect()
        raise Exception(msg)  

def transform(params, data):
    try:
        drivers_data =  data[0]  
        drivers_names = data[1]
        df_trackpoints = data[3]
        all_geofence_data = data[4]
        vehicle_to_consider = data[5]
    
        def harmonize_vehicle_name(name):
            if name[0] == "T":
                return name+"-SFTP"if "SFTP" not in name else name
            if name[0] =="A":
                return name+"-SFTP"if "SFTP" not in name else name
            else:
                return name

        def get_day_from_str_date(x):
            try:
                day = x.split(" ")[0]
            except AttributeError:
                day = x 
            return day

        def get_time_from_str_date(x):
            try:
                time = x.split(" ")[1]
            except AttributeError:
                time = x 
            return time

        # Association of a driver to every trackpoints:  
        def add_drivers(df_to_complete, time_key):
            
            completed_df = pd.DataFrame()
            unknown_name = "No_driver"
            
            vehicle_perimeter = list(set(drivers_data["equipment_plant_number"]) & set(df_to_complete["vehicleName"]))
            print("Vehicle perimeter considered in add_drivers function : ")
            print(vehicle_perimeter)
            
            for v in vehicle_perimeter:

                # Prepare data, with dataframes containing only the vehicle concerned
                df = df_to_complete.loc[df_to_complete["vehicleName"] == v]
                drivers_connections = drivers_data.loc[(drivers_data["equipment_plant_number"] == v) & 
                                                       (drivers_data["action"] == 1)].sort_values("date", ascending=True)

                # Find indexes where there is a new driver connecting
                df["driver_name"] = unknown_name
                for i,t in drivers_connections.iterrows():
                    try:
                        df.ix[df[time_key] >= t["date"], "driver_name"] = t["driver_name"]
                    except IndexError:
                        pass #Case when there was a connection after all points

                completed_df = pd.concat([completed_df,df])
                
            return completed_df.reset_index(drop=True)

        trackpoints_with_drivers = add_drivers(df_trackpoints,"datetime")
        trackpoints_with_drivers = trackpoints_with_drivers.drop('id', 1)
        print("trackpoints_with_drivers: DataFrame with trackpoints with drivers")
        print("----- Number of rows: " + str(trackpoints_with_drivers.shape[0]))
        print(trackpoints_with_drivers.head())  
        
        def speed_maintenance(): # for Haulage vehicles

            try:
                # Speed maintenance at a specified geofence
                vehicles_to_consider = list(vehicle_to_consider["equipment_plant_number"])
                relevant_trackpoints = trackpoints_with_drivers[trackpoints_with_drivers["vehicleName"].isin(vehicles_to_consider)]
                relevant_trackpoints = relevant_trackpoints.copy().sort_values(["vehicleName","datetime"], ascending=True).reset_index(drop=True)

                # ## Cruise speed
                df_cruise_speed = all_geofence_data[
                    (all_geofence_data["geofenceName"] == params["Speed_maintenace_geofence"]) &
                    (all_geofence_data["vehicleName"].isin(vehicles_to_consider))
                ]
                df_cruise_speed_with_drivers = add_drivers(df_cruise_speed, "entryTime")

                def str_to_timestamp(x):
                    return int(time.mktime(x.timetuple()))

                df_cruise_speed_with_drivers["entryTime"] = df_cruise_speed_with_drivers["entryTime"].apply(str_to_timestamp)
                df_cruise_speed_with_drivers["exitTime"] = df_cruise_speed_with_drivers["exitTime"].apply(str_to_timestamp)

                # A bit long to compute
                # This is to have the timestamp of the trackpoint as index so that computations below go faster
                df_trackpoints_time_indexed = relevant_trackpoints.copy().sort_values("datetime", ascending=True)
                df_trackpoints_time_indexed["datetime"] = df_trackpoints_time_indexed["datetime"].apply(str_to_timestamp)

                df_trackpoints_time_indexed = df_trackpoints_time_indexed.set_index("datetime")

                df_trackpoints_time_indexed_per_vehicle = {}
                for v in set(df_trackpoints_time_indexed["vehicleName"]):
                    df_trackpoints_time_indexed_per_vehicle[v] = df_trackpoints_time_indexed.loc[df_trackpoints_time_indexed["vehicleName"] == v]

                # !!! Very long to compute !!!
                def long_func_v_2():
                    cruise_speed_per_driver = {}
                    for driver in list(set(df_cruise_speed_with_drivers["driver_name"].dropna())):
                        df = df_cruise_speed_with_drivers.loc[df_cruise_speed_with_drivers["driver_name"] == driver]
                        cruise_speeds_stats = []

                        for i,t in df.iterrows():
                            try:
                                speeds = df_trackpoints_time_indexed_per_vehicle[t["vehicleName"]].loc[t["entryTime"]:t["exitTime"],"speed"]
                                mean = np.mean(speeds)
                                std = np.std(speeds)
                                cruise_speeds_stats.append((mean,std))
                            except KeyError:
                                continue #Case when some track points are missing

                        cruise_speed_per_driver[driver] = cruise_speeds_stats
                    return cruise_speed_per_driver

                cruise_speed_per_driver = long_func_v_2()
                
                scores_cruise_speed_per_driver = []
                for d,v in cruise_speed_per_driver.items():
                    mean_avg = []
                    std_avg = []
                    for tup in v:
                        mean = tup[0]
                        std = tup[1]
                        if not np.isnan(mean) and not np.isnan(std): 
                            mean_avg.append(mean)
                            std_avg.append(std)
                    scores_cruise_speed_per_driver.append({
                            "driver_name": d,
                            "average_cruise_speed": stats.trim_mean(mean_avg, 0.1) if len(mean_avg) > 0 else np.nan,
                            "score_cruise_speed": stats.trim_mean(std_avg, 0.1) if len(std_avg) > 0 else np.nan,
                            "number_of_moves_cruise_speed": len(v)
                        })

                cruise_speed_stats = pd.DataFrame(scores_cruise_speed_per_driver)
                print("Speed stability: --------------------------------------------------")
                print(cruise_speed_stats)
                
                move_score = cruise_speed_stats
                move_score["id_contest"] = [params["contest_id"]]*len(move_score)
                move_score["id_batch"] = [params["batch_number"]]*len(move_score)
                return move_score

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                msg = 'Error: ' +str(e) +' L:'+str(exc_tb.tb_lineno)
                print(msg)
                gc.collect()
                raise Exception(msg)

        def move_score_specific_stop_points():
            try:
                # Acceleration, braking at a specified stop points, and speed maintenance at a specified geofence
                vehicles_to_consider = list(vehicle_to_consider["equipment_plant_number"])
                relevant_trackpoints = trackpoints_with_drivers[trackpoints_with_drivers["vehicleName"].isin(vehicles_to_consider)]
                relevant_trackpoints = relevant_trackpoints.copy().sort_values(["vehicleName","datetime"], ascending=True).reset_index(drop=True)

                # Get acceleration, braking and cruise speed marks (Astra trucks only)
                #Setp 1:  Find all stop points

                def find_stop_points(lat,lng,df):
                    #This function will find all trackpoints that:
                    #- Are close to the 'lat' and 'lng' given
                    #- Has 0 speed
                    #In this case, we only consider stop points at the crossing with the main road
                    stop_lat = lat
                    stop_lng = lng
                    stop_points = df.loc[((df["latitude"].isin(stop_lat)) | (df["longitude"].isin(stop_lng))) & (df["speed"] == 0)]

                    #Find all stop points
                    previous_row_nb = 0
                    while stop_points.shape[0] > previous_row_nb:
                        previous_row_nb = stop_points.shape[0]
                        stop_lat = list(set(stop_points["latitude"]))
                        stop_lng = list(set(stop_points["longitude"]))
                        stop_points = df.loc[((df["latitude"].isin(stop_lat)) | (df["longitude"].isin(stop_lng))) & (df["speed"] == 0)]
                    return stop_points.sort_values(["vehicleName","datetime"], ascending=[True,True])

                ## Choose latitude and longitude for the area where you are looking for stop points
                stop_points = find_stop_points([6.208243,6.208317],[-5.304669,-5.304205], relevant_trackpoints)
                stop_points = stop_points.drop_duplicates()
                print("Number of stop points found: " + str(stop_points.shape[0]))
                stop_points = stop_points.reset_index(drop=False)

                ## Get clean stop points, associated with path (geofences data)
                #Get clean stop points for each vehicle
                # Détermine avec la géofence si c'est un stop point ou il accèlère et freine et si c'est vrs le pit ou vers le rom pad
                geofences_data = all_geofence_data[
                    (all_geofence_data["geofenceName"].isin(['Akissi-so', 'ROM pad'])) &
                    (all_geofence_data["vehicleName"].isin(vehicles_to_consider))
                ]

                final_stop_points = pd.DataFrame()

                for vehicle in list(set(stop_points["vehicleName"])):
                    
                    stop_points_vehicle = stop_points[stop_points["vehicleName"] == vehicle].sort_values("datetime", ascending=True)
                    geofences_data_vehicle = geofences_data[geofences_data["vehicleName"] == vehicle].sort_values("entryTime", ascending=True)
                    
                    if geofences_data_vehicle.shape[0] == 0:
                        #print("Vehicle with no geofence data: " + vehicle)
                        continue
                        
                    stop_points_vehicle = stop_points_vehicle[
                        (stop_points_vehicle["datetime"] < geofences_data_vehicle["entryTime"].iloc[-1]) & #Attribute a path to each stop point
                        (stop_points_vehicle["datetime"] >= geofences_data_vehicle["exitTime"].iloc[0]) #Avoid error due to stop points that exist before first geofence data
                    ]
                    
                    #Determine path_end time and path_destination
                    mask_index = [np.where(geofences_data_vehicle["entryTime"] > time)[0][0] for time in stop_points_vehicle["datetime"]]
                    #The mask_index determines the first row index of geofences data where entryTime is after the stop points
                    #Put differently, it determines the next geofence where the vehicle entered after the stop point ("path_destination")
                    #and at what time ("path_end")
                    
                    path_destination = []
                    path_end = []
                    for idx in mask_index:
                        row = geofences_data_vehicle.iloc[idx]
                        path_destination.append(row["geofenceName"])
                        path_end.append(row["entryTime"])

                    stop_points_vehicle["path_destination"] = path_destination
                    stop_points_vehicle["path_end"] = path_end
                    
                    #Delete unnecessary stop points (if more than one for the path) and qualify each stop point (braking or acceleration)
                    stop_points_vehicle_braking = stop_points_vehicle.groupby("path_end").first().reset_index()
                    stop_points_vehicle_braking["stop_type"] = "braking"

                    stop_points_vehicle_acceleration = stop_points_vehicle.groupby("path_end").last().reset_index()
                    stop_points_vehicle_acceleration["stop_type"] = "acceleration"
                    
                    #Record dataframe
                    final_stop_points = pd.concat([
                                    final_stop_points,
                                    stop_points_vehicle_braking,
                                    stop_points_vehicle_acceleration]).sort_values(["vehicleName","datetime"], ascending=[True,True])
                    
                    #http://stackoverflow.com/questions/29370057/select-dataframe-rows-between-two-dates
                    
                # # Calculate scores
                # ## Cruise speed
                df_cruise_speed = all_geofence_data[
                    (all_geofence_data["geofenceName"] == "Cruise speed") &
                    (all_geofence_data["vehicleName"].isin(vehicles_to_consider))
                ]
                df_cruise_speed_with_drivers = add_drivers(df_cruise_speed, "entryTime")

                def str_to_timestamp(x):
                    return int(time.mktime(x.timetuple()))

                df_cruise_speed_with_drivers["entryTime"] = df_cruise_speed_with_drivers["entryTime"].apply(str_to_timestamp)
                df_cruise_speed_with_drivers["exitTime"] = df_cruise_speed_with_drivers["exitTime"].apply(str_to_timestamp)

                # A bit long to compute
                # This is to have the timestamp of the trackpoint as index so that computations below go faster
                df_trackpoints_time_indexed = relevant_trackpoints.copy().sort_values("datetime", ascending=True)
                df_trackpoints_time_indexed["datetime"] = df_trackpoints_time_indexed["datetime"].apply(str_to_timestamp)

                df_trackpoints_time_indexed = df_trackpoints_time_indexed.set_index("datetime")

                df_trackpoints_time_indexed_per_vehicle = {}
                for v in set(df_trackpoints_time_indexed["vehicleName"]):
                    df_trackpoints_time_indexed_per_vehicle[v] = df_trackpoints_time_indexed.loc[df_trackpoints_time_indexed["vehicleName"] == v]

                # !!! Very long to compute !!!

                def long_func_v_1():
                    
                    cruise_speed_per_driver = {}

                    for driver in list(set(final_stop_points["driver_name"])):

                        df = df_cruise_speed_with_drivers.loc[df_cruise_speed_with_drivers["driver_name"] == driver]
                        cruise_speeds_stats = []

                        for vehicle, df_track in df_trackpoints_time_indexed_per_vehicle.items():

                            df = df[df["vehicleName"] == vehicle]

                            for t in df.itertuples():
                                try:       
                                    speeds = df_track.loc[t[5]:t[6],"speed"]
                                    mean = np.mean(speeds)
                                    std = np.std(speeds)
                                    cruise_speeds_stats.append((mean,std))
                                except KeyError:
                                    continue #Case when some track points are missing

                        cruise_speed_per_driver[driver] = cruise_speeds_stats
                        
                    return cruise_speed_per_driver

                # !!! Very long to compute !!!
                def long_func_v_2():
                    cruise_speed_per_driver = {}
                    for driver in list(set(final_stop_points["driver_name"])):
                        df = df_cruise_speed_with_drivers.loc[df_cruise_speed_with_drivers["driver_name"] == driver]
                        cruise_speeds_stats = []

                        # for t in df.itertuples():
                        #     try:
                        #         speeds = df_trackpoints_time_indexed_per_vehicle[t[20]].loc[t[5]:t[6],"speed"]
                        #         mean = np.mean(speeds)
                        #         std = np.std(speeds)
                        #         cruise_speeds_stats.append((mean,std))
                        #     except KeyError:
                        #         continue #Case when some track points are missing

                        for i,t in df.iterrows():
                            try:
                                speeds = df_trackpoints_time_indexed_per_vehicle[t["vehicleName"]].loc[t["entryTime"]:t["exitTime"],"speed"]
                                mean = np.mean(speeds)
                                std = np.std(speeds)
                                cruise_speeds_stats.append((mean,std))
                            except KeyError:
                                continue #Case when some track points are missing

                        cruise_speed_per_driver[driver] = cruise_speeds_stats
                    return cruise_speed_per_driver

                #%time cruise_speed_per_driver = long_func_v_1()
                cruise_speed_per_driver = long_func_v_2()
                print("test: --------------------------")
                print(cruise_speed_per_driver.keys())

                scores_cruise_speed_per_driver = []

                for d,v in cruise_speed_per_driver.items():
                    mean_avg = []
                    std_avg = []
                    for tup in v:
                        mean = tup[0]
                        std = tup[1]
                        if not np.isnan(mean) and not np.isnan(std): 
                            mean_avg.append(mean)
                            std_avg.append(std)
                    scores_cruise_speed_per_driver.append({
                            "driver_name": d,
                            "average_cruise_speed": stats.trim_mean(mean_avg, 0.1) if len(mean_avg) > 0 else np.nan,
                            "score_cruise_speed": stats.trim_mean(std_avg, 0.1) if len(std_avg) > 0 else np.nan,
                            "number_of_moves_cruise_speed": len(v)
                        })

                cruise_speed_stats = pd.DataFrame(scores_cruise_speed_per_driver)
                print("Test: --------------------------------------------------")
                print(cruise_speed_stats)
                #stats["score"] = stats["average_deviation"].apply(lambda x: )


                # ## Acceleration and braking
                final_stop_points_per_driver = {}
                final_stop_points["datetime"] = final_stop_points["datetime"].apply(str_to_timestamp)

                for driver in list(set(final_stop_points["driver_name"])):
                    final_stop_points_per_driver[driver] = final_stop_points[(final_stop_points["driver_name"] == driver)]

                def plot_movement_curves(final_stop_points_per_driver, seconds, movement, maximum_speed):

                    movements_per_driver = {}
                    reference_per_destination = {}
                    
                    for pos, path_destination in enumerate(['Akissi-so', 'ROM pad']):

                        movements_per_driver[path_destination] = {}
                        all_speeds =  [[] for x in range(0, seconds+1)]

                        for driver, df_stop_points in final_stop_points_per_driver.items():
                            
                            df_stop_points = df_stop_points[
                                (df_stop_points["stop_type"] == movement) & 
                                (df_stop_points["path_destination"] == path_destination)
                            ]
                            
                            movements_per_driver[path_destination][driver] = []

                            # print(df_stop_points["id_mycartracks"])
                            for idx in df_stop_points["id_mycartracks"]:
                                
                                if movement == "acceleration":
                                    beg = relevant_trackpoints[relevant_trackpoints["id_mycartracks"]==idx].index[0]
                                    end = relevant_trackpoints[relevant_trackpoints["id_mycartracks"]==idx].index[0] + seconds + 1
                                else:
                                    beg = relevant_trackpoints[relevant_trackpoints["id_mycartracks"]==idx].index[0] - seconds
                                    end = relevant_trackpoints[relevant_trackpoints["id_mycartracks"]==idx].index[0] + 1

                                speeds = []                
                                # for p,s in enumerate(relevant_trackpoints[beg:end]["speed"]):
                                #     speeds.append(s)
                                #     all_speeds[p].append(s)

                                # movements_per_driver[path_destination][driver].append(speeds)
                                for ix in range(beg, end): #enumerate(relevant_trackpoints[beg:end, "speed"]):
                                    speeds.append(relevant_trackpoints.loc[ix, "speed"])

                
                                if movement == "acceleration":
                                    if (any(0 == v for v in speeds[1:end]) == False) & (speeds[-1] > maximum_speed) :
                                        all_speeds.append(speeds)

                                        movements_per_driver[path_destination][driver].append(speeds)
                                        # ax_lst[pos][0].plot(speeds)

                                else:
                                    if (any(0 == v for v in speeds[0:-1]) == False) & (speeds[0] > maximum_speed) :
                                        all_speeds.append(speeds)

                                        movements_per_driver[path_destination][driver].append(speeds)
                                        # ax_lst[pos][0].plot(speeds)


                        print("Number of paths to " + path_destination + ": ", len(all_speeds[0]))

                        average_speeds = np.mean(all_speeds, axis=0)

                        # average_speeds = [np.average(values) for values in all_speeds]
                        # median_speeds = [np.median(values) for values in all_speeds]
                        #quartile_speeds = [np.percentile(values, 25) for values in all_speeds]
                        
                        reference_per_destination[path_destination] = average_speeds
                    
                    return movements_per_driver, reference_per_destination

                acceleration_per_driver, acceleration_reference = plot_movement_curves(final_stop_points_per_driver, 60, "acceleration",10)
                braking_per_driver, braking_reference = plot_movement_curves(final_stop_points_per_driver, 60, "braking",10)

                def get_movement_score(mvt_per_driver, mvt_reference,move_type):
                    
                    scores = []
                    score_per_driver = {}
                    
                    for path_destination in ['Akissi-so', 'ROM pad']:
                        
                        ref_move = mvt_reference[path_destination]
                        
                        for driver in mvt_per_driver[path_destination]:
                            
                            all_disp = []
                            c = 0
                            
                            for move in mvt_per_driver[path_destination][driver]:
                                
                                c += 1
                                try:
                                    ref_2D, mov_2D = np.array(ref_move).reshape(-1,1), np.array(move).reshape(-1,1) 
                                    mtx1, mtx2, disparity = procrustes(ref_2D, mov_2D)
                                    all_disp.append(disparity)
                                except ValueError:
                                    continue #If move only contains Zeros
                            
                            try:
                                agg_disp = score_per_driver[driver]["score"] + all_disp
                                scores.append({
                                        'driver_name': driver,
                                        'nb_of_moves_' + move_type: score_per_driver[driver]["nb_of_moves"] + c,
                                        'score_' + move_type: stats.trim_mean(agg_disp, 0.1) if len(agg_disp) > 0 else np.nan 
                                    })
                            except KeyError:
                                score_per_driver[driver] = {"score": all_disp, "nb_of_moves": c}
                            
                    return pd.DataFrame(scores)

                acc_score_per_driver = get_movement_score(acceleration_per_driver, acceleration_reference,'acceleration')
                brake_score_per_driver = get_movement_score(braking_per_driver, braking_reference,'braking')
                move_score = acc_score_per_driver.merge(brake_score_per_driver, on='driver_name')

                move_score = move_score.merge(cruise_speed_stats,on='driver_name')
                move_score = move_score[move_score["driver_name"] != "DEMO"]

                move_score["id_contest"] = [params["contest_id"]]*len(move_score)
                move_score["id_batch"] = [params["batch_number"]]*len(move_score)
                return move_score

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                msg = 'Error: ' +str(e) +' L:'+str(exc_tb.tb_lineno)
                print(msg)
                gc.collect()
                raise Exception(msg)

        def move_score_all_stop_points():
            try:
                vehicles_to_consider = list(vehicle_to_consider["equipment_plant_number"])
                # Acceleration and braking for every "clean" stop points

                # # Get acceleration and braking scores after a clear stop points

                # ## Identification of clear stop points
                # Clear stop points are defined as stops points of more than 30 seconds

                # INPUT n°1:
                minimum_time = 1*60 # Minimum time in seconds to be considered a clear stop
                minimum_speed = 2 # minimum_speed to be considered to be stopped
                maximum_speed = 20 # speed to exceed at least at the end of the time considered

                # INPUT n°2:
                relevant_trackpoints = trackpoints_with_drivers[trackpoints_with_drivers["vehicleName"].isin(list( vehicles_to_consider))==True]
                vehicles_to_consider =list(set(list(relevant_trackpoints["vehicleName"])))

                # In[36]:
                relevant_trackpoints = trackpoints_with_drivers[trackpoints_with_drivers["vehicleName"].isin(vehicles_to_consider)]
                relevant_trackpoints = relevant_trackpoints.copy().sort_values(["vehicleName","datetime"], ascending=True).reset_index(drop=True)

                # For one driver, compute the stop points of this driver for a specific vehicle 
                def get_stop_points(df_trackpoints_to_consider, vehicle, minimum_time, minimum_speed):
                    # df_trackpoints_to_consider: trackpoints with driver
                    df = df_trackpoints_to_consider[df_trackpoints_to_consider["vehicleName"] == vehicle] 
                    df = df.sort_values(["vehicleName","datetime"],ascending = [True, True]).reset_index(drop=True)
                    df = df[["vehicleName","speed","datetime","trackId","id_mycartracks","driver_name"]]
                    
                    # Detect clear stop points
                    stop_path_per_vehicle = []
                    stop_id =[]

                    time_per_vehicle = []

                    last_car = ""
                    last_track_ID = ""
                    count = 0

                    for idx, df in df.iterrows():
                        vehicle = df["vehicleName"] 
                        time = df["datetime"]
                        speed = df["speed"]
                        trackID = df["trackId"]
                        trackpointsID = df["id_mycartracks"]
                        driver_name = df["driver_name"]

                        if (speed <minimum_speed) & (trackID == last_track_ID) :
                            count = count + 1
                            time_per_vehicle.append(time) 
                            stop_id.append(trackpointsID)
                        else : 
                            if count > 1 :
                                stop_time = (time_per_vehicle[-1] - time_per_vehicle[0]).total_seconds()
                                if (stop_time > minimum_time):
                                    stop_path_per_vehicle.append({
                                            "vehicleName":vehicle,
                                            "trackId": trackID,
                                            "acceleration_id": stop_id[-1],
                                            "braking_id": stop_id[0],
                                            "driver_name":driver_name
                                            })

                                count = 0
                                time_per_vehicle = []
                                stop_id =[]

                            count = 0
                            time_per_vehicle = []
                            stop_id =[]

                            if speed < minimum_speed :
                                count = count +1                 
                                time_per_vehicle.append(time)
                                stop_id.append(trackpointsID)
                            else : 
                                continue

                        last_car = vehicle
                        last_track_ID = trackID

                    stop_path_per_vehicle = pd.DataFrame(stop_path_per_vehicle)
                    return stop_path_per_vehicle

                stop_points_per_driver ={}
                stop_points= pd.DataFrame()
                for driver in list(set(relevant_trackpoints['driver_name'])):
                    for vehicle in list(set(relevant_trackpoints.ix[relevant_trackpoints["driver_name"]==driver,"vehicleName"])):
                        df_trackpoints = relevant_trackpoints[(relevant_trackpoints["driver_name"]==driver) & (relevant_trackpoints["vehicleName"] ==vehicle)]
                        stop_path_per_vehicle = get_stop_points (df_trackpoints, vehicle, minimum_time, minimum_speed)
                        #print(stop_path_per_vehicle.head())
                        stop_points = pd.concat([stop_points,stop_path_per_vehicle])
                    
                    stop_points_per_driver[driver] =stop_points
                    stop_points= pd.DataFrame()

                # Get the movement path after and before each stop points for a fixed time range equal to SECONDS parameter 

                def plot_movement_curves (stop_path_per_vehicle, df_trackpoints_to_consider, seconds, movement, maximum_speed):
                    # movement can take two values: "acceleration" and "braking"
                  
                    # Datafram with the different path in columns and their value from 0 to 60 seconds in line
                    df_all_speeds = pd.DataFrame(np.nan, index = np.arange(seconds+1), columns = np.arange(stop_path_per_vehicle.shape[0]))
                    denominator = df_all_speeds.shape[1] # Number of paths 

                    # Dictionnary containing the different classifications dataframes
                    dict_all_speed_clean = {}
                    dict_all_speed_clean[maximum_speed] = pd.DataFrame()
                    dict_all_speed_clean[maximum_speed+5] = pd.DataFrame()
                    dict_all_speed_clean[maximum_speed+10] = pd.DataFrame()
                    dict_all_speed_clean[maximum_speed+15] = pd.DataFrame()
                    
                    # Dictionnary containing the different average_speeds ndarray
                    average_speed = {}
                    average_speed [maximum_speed] = []
                    average_speed [maximum_speed+5] = []
                    average_speed [maximum_speed+10] = []
                    average_speed [maximum_speed+15] = []
                    
                    for t,v in stop_path_per_vehicle.iterrows():
                        try:
                            if movement == "acceleration":
                                idx = df_trackpoints_to_consider[df_trackpoints_to_consider["id_mycartracks"] == v["acceleration_id"]].index[0]
                                beg = idx
                                end = beg + seconds +1
                            if movement == "braking" :
                                idx = df_trackpoints_to_consider[df_trackpoints_to_consider["id_mycartracks"] == v["braking_id"]].index[0]
                                beg = idx - seconds
                                end = beg +1 +seconds

                            # Reset index to zero so that it corresponds to the index of the path i.e. from 0 to 60
                            try:
                                df_all_speeds[t] = df_trackpoints_to_consider[beg:end]["speed"].reset_index(drop=True)
                                #df_all_speeds[t]= df_all_speeds[t].fillna(0)
                            except IndexError:
                                pass

                            # Filter out tracks that come back to speed ==0 after a short time
                            if movement == "acceleration":
                                if (any(0 == v for v in df_all_speeds[t][1:end]) == False) & (df_all_speeds[t].iloc[-1] > maximum_speed) :

                                    if ((maximum_speed+5) >= df_all_speeds[t].iloc[-1] > maximum_speed):
                                        #ax_lst[0,0].plot(df_all_speeds[t])
                                        dict_all_speed_clean[maximum_speed] = pd.concat([dict_all_speed_clean[maximum_speed],df_all_speeds[t]], axis = 1)
                                    if ((maximum_speed+10) >= df_all_speeds[t].iloc[-1] > (maximum_speed +5)):
                                        #ax_lst[1,0].plot(df_all_speeds[t])
                                        dict_all_speed_clean[maximum_speed +5] = pd.concat([dict_all_speed_clean[maximum_speed +5],df_all_speeds[t]], axis = 1)        
                                    if ((maximum_speed+15) >= df_all_speeds[t].iloc[-1] > (maximum_speed +10)):
                                        #ax_lst[2,0].plot(df_all_speeds[t])
                                        dict_all_speed_clean[maximum_speed +10] = pd.concat([dict_all_speed_clean[maximum_speed +10],df_all_speeds[t]], axis = 1)        

                                    if (df_all_speeds[t].iloc[-1] > (maximum_speed +15)):
                                        #ax_lst[3,0].plot(df_all_speeds[t])
                                        dict_all_speed_clean[maximum_speed +15] = pd.concat([dict_all_speed_clean[maximum_speed +15],df_all_speeds[t]], axis = 1)                    
                                    
                            if movement == "braking":
                                if (any(0 == v for v in df_all_speeds[t][0:-1]) == False) & (df_all_speeds[t].iloc[0] > maximum_speed) :

                                    if ((maximum_speed+5) >= df_all_speeds[t].iloc[0] > maximum_speed):
                                        #ax_lst[0,0].plot(df_all_speeds[t])
                                        dict_all_speed_clean[maximum_speed] = pd.concat([dict_all_speed_clean[maximum_speed],df_all_speeds[t]], axis = 1)
                                    if ((maximum_speed+10) >= df_all_speeds[t].iloc[0] > (maximum_speed +5)):
                                        #ax_lst[1,0].plot(df_all_speeds[t])
                                        dict_all_speed_clean[maximum_speed +5] = pd.concat([dict_all_speed_clean[maximum_speed +5],df_all_speeds[t]], axis = 1)        
                                    if ((maximum_speed+15) >= df_all_speeds[t].iloc[0] > (maximum_speed +10)):
                                        #ax_lst[2,0].plot(df_all_speeds[t])
                                        dict_all_speed_clean[maximum_speed +10] = pd.concat([dict_all_speed_clean[maximum_speed +10],df_all_speeds[t]], axis = 1)        

                                    if (df_all_speeds[t].iloc[0] > (maximum_speed +15)):
                                        #ax_lst[3,0].plot(df_all_speeds[t])
                                        dict_all_speed_clean[maximum_speed +15] = pd.concat([dict_all_speed_clean[maximum_speed +15],df_all_speeds[t]], axis = 1)                    

                        except IndexError:
                            pass
                    df_all_speeds_clean = pd.concat([df for df in dict_all_speed_clean.values()],axis = 1)
                    numerator = df_all_speeds_clean.shape[1] 
                    
                    average_speed [maximum_speed] = dict_all_speed_clean[maximum_speed].mean(axis = 1)
                    average_speed [maximum_speed +5] = dict_all_speed_clean[maximum_speed+5].mean(axis = 1)
                    average_speed [maximum_speed+10] = dict_all_speed_clean[maximum_speed+10].mean(axis = 1)
                    average_speed [maximum_speed+15] = dict_all_speed_clean[maximum_speed+15].mean(axis = 1)
                    
                    # ax_lst[0,1].plot(average_speed [maximum_speed])
                    # ax_lst[1,1].plot(average_speed [maximum_speed+5])
                    # ax_lst[2,1].plot(average_speed [maximum_speed+10])
                    # ax_lst[3,1].plot(average_speed [maximum_speed+15])
                    
                    if denominator!=0:
                        ratio = numerator/denominator
                    else:
                        ratio = 0
                    
                    return dict_all_speed_clean, df_all_speeds_clean, average_speed, ratio

                # mvt = "acceleration"
                def get_acceleration_braking_scores(seconds,mvt,maximum_speed):
                    try:    
                        dict_all_speed_clean_vehicles ={}
                        df_all_speeds_clean_vehicles ={}
                        average_speed_vehicles ={}
                        ratio_vehicles ={}

                        for driver in list(stop_points_per_driver.keys()):
                            if len(stop_points_per_driver[driver])>0:
                                try:
                                    dict_all_speed_clean_vehicles[driver] ={}
                                    df_all_speeds_clean_vehicles[driver] ={}
                                    average_speed_vehicles[driver] ={}
                                    ratio_vehicles[driver] ={}

                                    for vehicle in list(set(stop_points_per_driver[driver]["vehicleName"])):
                                        try:
                                            stop_path_per_vehicle = stop_points_per_driver[driver][stop_points_per_driver[driver]["vehicleName"]==vehicle]   
                                            df_trackpoints_to_consider = relevant_trackpoints[(relevant_trackpoints["driver_name"]==driver) & (relevant_trackpoints["vehicleName"] ==vehicle)]
                                            df_trackpoints_to_consider = df_trackpoints_to_consider.sort_values("datetime",ascending=True)
                                            df_trackpoints_to_consider= df_trackpoints_to_consider.reset_index(drop = True)
                                            #print(df_trackpoints_to_consider.head(1))
                                            dict_all_speed_clean_vehicles[driver][vehicle], df_all_speeds_clean_vehicles[driver][vehicle], average_speed_vehicles[driver][vehicle], ratio_vehicles[driver][vehicle] = plot_movement_curves(stop_path_per_vehicle, df_trackpoints_to_consider, 60, mvt,maximum_speed)
                                        except IndexError:
                                            pass

                                except IndexError:
                                    pass


                        # compute the mean over all vehicles 
                        speeds = [maximum_speed, maximum_speed+5,maximum_speed+10,maximum_speed+15]
                        average_speeds = {}
                        average = pd.Series()
                        for s in speeds:
                            for driver in list(stop_points_per_driver.keys()):
                                if len(stop_points_per_driver[driver])>0:
                                    for vehicle in list(set(stop_points_per_driver[driver]["vehicleName"])):
                                        average =pd.concat([average_speed_vehicles[driver][vehicle][s],average], axis =1)
                                else:
                                    continue
                            average_speeds[s]= average.mean(axis = 1)

                        # Compute for a specified vehicle and classification a score
                        def get_movement_scores (dict_all_speed_vehicle,average_speeds,classifier):
                            scores= []
                            all_speed_clean = dict_all_speed_vehicle[classifier]
                            average = average_speeds [classifier]
                            for i in all_speed_clean.columns.values:
                                ref_2D, mov_2D = np.array(average).reshape(-1,1), np.array(all_speed_clean[i]).reshape(-1,1) 
                                mtx1, mtx2, disparity = procrustes(ref_2D, mov_2D)
                                scores.append(disparity)
                            scores = np.mean(scores)
                            return scores

                        driver_scores ={}
                        for driver in list(stop_points_per_driver.keys()):
                            if len(stop_points_per_driver[driver])>0:
                                for vehicle in list(set(stop_points_per_driver[driver]["vehicleName"])):
                                    driver_scores[driver] ={}
                                    driver_scores[driver][vehicle]={}
                                    for s in speeds:
                                        driver_scores[driver][vehicle][s]=np.nan_to_num(get_movement_scores(dict_all_speed_clean_vehicles[driver][vehicle],average_speeds,s))
                      

                        # Weight the scores of the drivers and vehicles for different classification by the number of path per calssification
                        scores_weighted_drivers ={}
                        for driver in list(driver_scores.keys()):
                            scores_weighted_drivers[driver] ={}
                            scores_weighted_drivers[driver]["scores"] =[] 
                            for vehicle in list(set(driver_scores[driver].keys())):
                                for s in speeds:
                                    v = driver_scores[driver][vehicle][s]*dict_all_speed_clean_vehicles[driver][vehicle][s].columns.shape[0]/df_all_speeds_clean_vehicles[driver][vehicle].shape[1]
                                    if (v!=0):
                                        scores_weighted_drivers[driver]["scores"].append(driver_scores[driver][vehicle][s]*dict_all_speed_clean_vehicles[driver][vehicle][s].columns.shape[0]/df_all_speeds_clean_vehicles[driver][vehicle].shape[1])
                            scores_weighted_drivers[driver]["scores"]= np.mean(scores_weighted_drivers[driver]["scores"])
                            scores_weighted_drivers[driver]["nb_path"] = df_all_speeds_clean_vehicles[driver][vehicle].shape[1]
                        #scores_weighted = pd.DataFrame(scores_weighted)
                        scores_weighted_drivers

                        move_score = []
                        for driver in scores_weighted_drivers.keys():
                            move_score.append({
                                "driver_name":driver,
                                "nb_of_moves_"+mvt:scores_weighted_drivers[driver]['nb_path'],
                                "score_"+mvt: scores_weighted_drivers[driver]["scores"]
                                })
                        move_score = pd.DataFrame(move_score)
                        move_score = move_score.dropna()

                        # Plot best and worst curves
                        move_score = move_score.sort_values("score_"+mvt, ascending = True).reset_index(drop=True)
                        
                        """
                        ## Best drivers
                        fig, ax_lst = plt.subplots(4,2)
                        best_driver = move_score["driver"].iloc[0]
                        df_all_speeds_clean_best_driver = df_all_speeds_clean_vehicles[best_driver]
                        for vehicle in list(set(driver_scores[best_driver].keys())):
                            for i in range(0,3):
                                for path in list(dict_all_speed_clean_vehicles[best_driver][vehicle][speeds[i]].columns): 
                                    ax_lst[i,0].plot(dict_all_speed_clean_vehicles[best_driver][vehicle][speeds[i]][path])

                        ax_lst[0,1].plot(average_speeds[maximum_speed])
                        ax_lst[1,1].plot(average_speeds[maximum_speed+5])
                        ax_lst[2,1].plot(average_speeds[maximum_speed+10])
                        ax_lst[3,1].plot(average_speeds[maximum_speed+15])
                        plt.show()
                        fig.savefig("images/Best_drivers for "+mvt+".png")
                        
                        # Worst drivers
                        fig, ax_lst = plt.subplots(4,2)
                        best_driver = move_score["driver"].iloc[0]
                        df_all_speeds_clean_best_driver = df_all_speeds_clean_vehicles[best_driver]
                        for vehicle in list(set(driver_scores[best_driver].keys())):
                            for i in range(0,3):
                                for path in list(dict_all_speed_clean_vehicles[best_driver][vehicle][speeds[i]].columns): 
                                    ax_lst[i,0].plot(dict_all_speed_clean_vehicles[best_driver][vehicle][speeds[i]][path])

                        ax_lst[0,1].plot(average_speeds[maximum_speed])
                        ax_lst[1,1].plot(average_speeds[maximum_speed+5])
                        ax_lst[2,1].plot(average_speeds[maximum_speed+10])
                        ax_lst[3,1].plot(average_speeds[maximum_speed+15])
                        plt.show()
                        fig.savefig("images/Worst_drivers for "+mvt+".png")
                        """
   
                        return move_score

                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        msg = 'Error: ' +str(e) +' L:'+str(exc_tb.tb_lineno)
                        print(msg)
                        gc.collect()
                        raise Exception(msg)

                acceleration_score = get_acceleration_braking_scores(60, "acceleration",maximum_speed)
                braking_score = get_acceleration_braking_scores(60, "braking",maximum_speed)
                move_score = pd.merge(acceleration_score,braking_score, how='outer', on = "driver_name")
                print("move_score:---------------------------------------------------------------------")
                print(move_score)
                return move_score 

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                msg = 'Error: ' +str(e) +' L:'+str(exc_tb.tb_lineno)
                print(msg)
                gc.collect()
                raise Exception(msg)

        def climb_and_descent(): 
            # Speed maintenance in montée and descente
            try:

                param_descent=40 # length of the plotted curves (20 before middle of descent, 20 after)
                param_climb=60 # length of the plotted curves (30 before middle of descent, 30 after)

                vehicles_to_consider = list(vehicle_to_consider["equipment_plant_number"])
                relevant_trackpoints = trackpoints_with_drivers[trackpoints_with_drivers["vehicleName"].isin(list( vehicles_to_consider))==True]
                
                vehicles_to_consider =list(set(list(relevant_trackpoints["vehicleName"])))
                relevant_trackpoints = trackpoints_with_drivers[trackpoints_with_drivers["vehicleName"].isin(vehicles_to_consider)]

                def get_middle_per_vhl(df_trackpoints_to_consider, vehicle_to_consider):
                   
                    nbre_milieu_montee = 0
                    nbre_milieu_descente = 0
                    
                    df = df_trackpoints_to_consider[df_trackpoints_to_consider['vehicleName'].isin(vehicle_to_consider)==True]
                    df['altitude'] = df['altitude'].apply(lambda s: float(str(s).split(" ")[0]))
                    trackId = list(set(df['trackId']))
                       

                    df_middle_montee=pd.DataFrame(columns={'index','altitude','trackId','vehicleName'})
                    df_middle_descente=pd.DataFrame(columns={'index','altitude','trackId','vehicleName'})

                    j=0 # indices
                    k=0 # indices
                             
                    for track in trackId:
                        
                        p = df[df['trackId'] == track].reset_index(drop=True) # trackpoints of the vehicle considered for the track ocnsidered
                        vhl=p.loc[0,'vehicleName'] #name of the vehicle
                        
                        
                        if (max(p['altitude'])-min(p['altitude'])>100): # To be determined manually in order to filter out flat tracks
                            altitude = np.array(p['altitude'])
                            inv_altitude = -altitude  #pour trouver les minima

                            # paramètres de peakutils choisis à la main afin de garder uniquement le sommet le plus haut
                            ###############################################################################################
                            ## https://plot.ly/python/peak-finding/                                                      ##
                            ## https://pythonhosted.org/PeakUtils/reference.html : features of peakutiles.indexes()      ##
                            ###############################################################################################
                            
                            ## treshold: minimum variation for an extremum to be considered as such
                            ## min_dist: chosen manually (minimum distance between two maxima)
                            indices_max = peakutils.indexes(altitude, thres=0.1, min_dist=500)  
                            indices_min = peakutils.indexes(inv_altitude, thres=0.1, min_dist=500)

                            df_max = pd.DataFrame(indices_max, columns={'index'})
                            df_max['type']="max"

                            df_min = pd.DataFrame(indices_min, columns={'index'})
                            df_min['type']="min"

                            df_extremum = pd.concat([df_max , df_min])
                            df_extremum = df_extremum.sort_values("index", ascending=True).reset_index(drop=True)

                        
                            for i in range(len(df_extremum)-1):
                                try:              
                                    if (df_extremum.loc[i, 'type']=='max'):

                                        if ((df_extremum.loc[i+1, 'type']=='min')):
                                            altitude_middle = int((p.loc[df_extremum.loc[i, 'index'],'altitude']+ p.loc[df_extremum.loc[i+1, 'index'],'altitude'])/2)
                                            df_middle_descente.loc[j,'altitude']=altitude_middle
                                            df_middle_descente.loc[j,'index']= p[((p['altitude']-altitude_middle <0) & (p['altitude']-altitude_middle> -5) & (p.index>df_extremum.loc[i, 'index']) & (p.index<df_extremum.loc[i+1, 'index']))].dropna().index.tolist()[0] # First points between [altitude middle, altitude middle -5], -5 if defined mannually 
                                            df_middle_descente.loc[j,'trackId']=track
                                            df_middle_descente.loc[j,'vehicleName']=vhl
                                            nbre_milieu_descente+=1
                                            j+=1

                                    else:

                                         if ((df_extremum.loc[i+1, 'type']=='max')):
                                            altitude_middle = int((p.loc[df_extremum.loc[i, 'index'],'altitude']+ p.loc[df_extremum.loc[i+1, 'index'],'altitude'])/2)
                                            df_middle_montee.loc[k,'altitude']=altitude_middle
                                            df_middle_montee.loc[k,'index']= p[ (p['altitude']-altitude_middle< 5) & (p['altitude']-altitude_middle>0) & (p.index>df_extremum.loc[i, 'index'])&(p.index< df_extremum.loc[i+1, 'index'])].dropna().index.tolist()[0] # First points between [altitude middle, altitude middle + 5], +5 if defined mannually
                                            df_middle_montee.loc[k,'trackId']=track
                                            df_middle_montee.loc[k,'vehicleName']=vhl
                                            nbre_milieu_montee+=1
                                            k+=1

                                except IndexError:
                                    pass
                                
                    # Cleaning des milieux : On ne garde que les points situés entre 155 et 175m : cela correspond au milieu du pit 1
                    
                    df_middle_montee = df_middle_montee[(df_middle_montee['altitude']>155)&(df_middle_montee['altitude']<178)].reset_index(drop=True) 
                    df_middle_descente = df_middle_descente[(df_middle_descente['altitude']>155)&(df_middle_descente['altitude']<178)].reset_index(drop=True)
                    
                    return df_middle_montee, df_middle_descente, nbre_milieu_montee, nbre_milieu_descente


                def plot_mvt_curves(df_trackpoints_to_consider, param_montee, param_descente):
                    
                    dict_all_curves={'montee':{}, 'descente':{}}
                    reference_per_movement={'montee':{},'descente':{}}
                    df_all_montee=pd.DataFrame()
                    df_all_descente=pd.DataFrame()
                    nbre_montee=0
                    nbre_descente=0
                    nbre_descente_final =0
                    nbre_montee_final=0

                        
                    drivers = list(set(df_trackpoints_to_consider['driver_name']))
                    
                    for driver in drivers:
                        
                        df = df_trackpoints_to_consider[(df_trackpoints_to_consider['driver_name'] == driver)]
                        vehicles = list(set(df['vehicleName']))
                        df_middle_montee, df_middle_descente, nbre_milieu_montee, nbre_milieu_descente = get_middle_per_vhl(df, vehicles)
                        
                        dict_all_curves['montee'][driver]=pd.DataFrame()
                        dict_all_curves['descente'][driver]=pd.DataFrame()
                        
                        nbre_montee+=nbre_milieu_montee
                        nbre_descente+=nbre_milieu_descente
                        
                            
                        #plot_montee
                        for track in list(set(df_middle_montee['trackId'])):

                            p=df[df['trackId']==track].reset_index(drop=True)
                            j=0 

                            for index, points in df_middle_montee[(df_middle_montee>(param_montee/2))&(df_middle_montee<len(p)-(param_montee/2))].dropna().iterrows():

                                try:
                                    temp = pd.DataFrame(columns={'courbe'})
                                    temp.loc[0, 'courbe'] = p.loc[points['index']-(param_montee/2), 'speed']

                                    for i in range(1, param_montee):
                                        temp.loc[i, 'courbe'] = p.loc[points['index']-(param_montee/2)+i, 'speed']


                                    #Cleaning of tracks with a filter on speed (speed between [6,16]) in order to have "clean" plots
                                    if ((min(temp['courbe'])>6) &(max(temp['courbe'])<16)):
                                        dict_all_curves['montee'][driver][j]=temp
                                        df_all_montee[nbre_montee_final]=temp
                                        j+=1
                                        nbre_montee_final+=1

                                except KeyError:
                                    pass
                        
                                    
                        #plot descente
                        for track in list(set(df_middle_descente['trackId'])):

                            p=df[df['trackId']==track].reset_index(drop=True)
                            k=0

                            for index, points in df_middle_descente[(df_middle_descente>(param_descente/2))&(df_middle_descente<len(p)-(param_descente/2))].dropna().iterrows():

                                try:
                                    temp = pd.DataFrame(columns={'courbe'})
                                    temp.loc[0, 'courbe'] = p.loc[points['index']-(param_descente/2), 'speed']

                                    for i in range(1, param_descente):
                                        temp.loc[i, 'courbe'] = p.loc[points['index']-(param_descente/2)+i, 'speed']

                                    #Cleaning of tracks with a filter on speed (min speed >15 and max speed > 20). Max speed is to be sure we deal with a descent. Min speed to be sure we are right in the middle of the descent
                                    if ((min(temp['courbe'])>15) & (max(temp['courbe'])>20)):  
                                        dict_all_curves['descente'][driver][k]= temp
                                        df_all_descente[nbre_descente_final]=temp
                                        nbre_descente_final+=1
                                        k+=1
                                    
                                except KeyError:
                                    pass

                    
                    reference_per_movement['descente']=df_all_descente.dropna(axis=1).mean(axis=1, skipna=True)
                    reference_per_movement['montee']=df_all_montee.dropna(axis=1).mean(axis=1, skipna=True)
                    
                    return dict_all_curves,reference_per_movement, nbre_montee, nbre_montee_final, nbre_descente, nbre_descente_final

                dict_all_curves, reference_per_movement, nbre_montee, nbre_montee_final, nbre_descente, nbre_descente_final = plot_mvt_curves(relevant_trackpoints, param_climb, param_descent)

                def get_movement_scores(dict_all_curves):
                    
                    scores={'montee':{}, 'descente':{}}
                    
                    for driver in list(dict_all_curves['montee']):
                        score = []
                        
                        try:
                            score = []
                            for column in dict_all_curves['montee'][driver]:
                                
                                std = np.std(dict_all_curves['montee'][driver][column].values) 
                                # on note les chauffeurs sur la base de la déviation standard (pour voir le maintien de la vitesse)
                                score.append(std)

                            scores['montee'][driver]=np.mean(score)
                        
                        except (KeyError, ValueError):
                            pass
                        
                    for driver in list(dict_all_curves['descente']):
                        score = []

                        try:
                            score = []
                            for column in dict_all_curves['descente'][driver]:

                                std=np.std(dict_all_curves['descente'][driver][column].values)
                                score.append(std)
                                
                            scores['descente'][driver]=np.mean(score)

                        except (KeyError, ValueError):
                            pass
                    return scores

                scores = get_movement_scores(dict_all_curves)

                # Get scores descente
                scores_descent = pd.DataFrame.from_dict(scores['descente'], orient='index')
                scores_descent = scores_descent.dropna().reset_index().rename(columns={'index':'name', 0:'score_descent'}).sort_values('score_descent', ascending=True)
                for ix, row in scores_descent.iterrows():
                    scores_descent.loc[ix, 'num_descent'] = len(dict_all_curves['descente'][row['name']].columns)
                scores_descent = scores_descent.reset_index(drop=True)

                # Get scores montee
                scores_montee = pd.DataFrame.from_dict(scores['montee'], orient='index')
                scores_montee = scores_montee.dropna().reset_index().rename(columns={'index':'name', 0:'score_climb'}).sort_values('score_climb', ascending=True)
                for ix, row in scores_montee.iterrows():
                    scores_montee.loc[ix, 'num_climb'] = len(dict_all_curves['montee'][row['name']].columns)

                scores_montee = scores_montee.reset_index(drop=True)

                # Merge descent and montee
                scores = pd.merge(scores_descent, scores_montee, on = "name",how = 'outer')
                scores = scores.fillna(0)
                scores ["number_of_moves_cruise_speed"] = scores["num_climb"] + scores["num_descent"]
                scores ["score_cruise_speed"] =[0]*len(scores)
                for i,v in scores.iterrows():
                    scores["score_descent"].iloc[i] = (v["score_descent"]-scores["score_descent"].min()) /(scores["score_descent"].max()-scores["score_descent"].min())
                    scores["score_climb"].iloc[i] = (v["score_climb"]-scores["score_climb"].min()) /(scores["score_climb"].max()-scores["score_climb"].min())
                    scores["score_cruise_speed"].iloc[i]= (v["num_descent"]*scores["score_descent"].iloc[i] + v["num_climb"]*scores["score_climb"].iloc[i])/(v["num_descent"]+v["num_climb"])
                move_score_bis =  scores[["name","number_of_moves_cruise_speed","score_cruise_speed"]]
                move_score_bis = move_score_bis.rename(columns ={"name":"driver_name"})

                print("move_score_bis: -------------------------------------------")
                print(move_score_bis)
                return move_score_bis

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                msg = 'Error: ' +str(e) +' L:'+str(exc_tb.tb_lineno)
                print(msg)
                gc.collect()
                raise Exception(msg)  

        if params["score_type"]=="Haulage":
            move_score = move_score_all_stop_points()
            print("Move score:", move_score)
            speed_maintenance_score = speed_maintenance()
            print("Speed stability score:", speed_maintenance_score)
            move_score = pd.merge(move_score,speed_maintenance_score, how= "outer", on = "driver_name")
            print("")
            print("--------------move scores:")
            print(move_score.head())

        if params["score_type"]=="DT":
            move_score = move_score_all_stop_points()
            speed_maintenance_score = climb_and_descent()
            move_score = pd.merge(move_score,speed_maintenance_score, how= "outer", on = "driver_name")
            move_score["id_contest"] = [params["contest_id"]]*len(move_score)
            move_score["id_batch"] = [params["batch_number"]]*len(move_score)
            move_score = move_score
            print("")
            print("--------------move scores:")
            print(move_score.head())
            print("")
            print("--------------speed maintenance scores")
            print(speed_maintenance_score.head())

        
        DataOut1 = move_score
        DataOut2 = drivers_names
        DataOut =[]
        DataOut =[DataOut1,DataOut2]
    
        return DataOut

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        msg = 'Error: ' +str(e) +' L:'+str(exc_tb.tb_lineno)
        print(msg)
        gc.collect()
        raise Exception(msg)  

def load(params,data):
    drivers_names = data[1] # driver_names
    # for_example_drivers = drivers_names.head(5)
    # for_example_drivers = pd.DataFrame(for_example_drivers)
    # print("This is an example of how the driver names data looks:", for_example_drivers)
    
    engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
   
    if params["score_type"]=="Haulage":
        move_score = data[0] # drivers_data
        for_example_data = move_score.head(5)
        for_example_data = pd.DataFrame(for_example_data)
        print("This is an example of how the first part to merge looks", for_example_data)
        print("The merging is done on the \"driver_name\" field")
        move_score = pd.merge(move_score,drivers_names,how = "inner", on = "driver_name")
        move_score = move_score.drop(["name","surname"],1)
        move_score = move_score.rename(columns ={"id":"driver_id"})
        print("-----------------------Final move score for Haulage")    
        print(move_score)

        engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
        move_score.to_sql(con=engine,name='ecodriving_move_scores', if_exists='append', index=False)    
    
    if params["score_type"]=="DT":
        move_score = data[0]
        # drivers_names = data[1]
        move_score = pd.merge(move_score,drivers_names,how = "inner", on = "driver_name")
        move_score = move_score.drop(["name","surname"],1)
        move_score = move_score.rename(columns ={"id":"driver_id"})
        print("-----------------------Final move score for DT")    
        print(move_score)
        engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
        move_score.to_sql(con=engine,name='ecodriving_move_scores', if_exists='append', index=False)
    

    return True


main_scoring(params)

