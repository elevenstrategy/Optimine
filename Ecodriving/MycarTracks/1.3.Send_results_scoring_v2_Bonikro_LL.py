import nexmo
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sys

params = {
    "contest_id": int(sys.argv[1]),
    "batch_number":int(sys.argv[2]),
    "message":"Sucess",
    "client_drt":"bonikro", #APP4MOB API
    "client":"bonikro_drt_v5",
}

client = nexmo.Client(key="36112ed4", secret="50363491001c8b58")

# Get continuous score
engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
query = "SELECT * FROM `ecodriving_continuous_results` WHERE `id_contest`="+str(params["contest_id"])+" and `batch_number`="+str(params["batch_number"])
scores = pd.read_sql_query(query,con=engine)
scores = scores.fillna(0)

# Obtain phone numbers
drivers_id = str(tuple(list(set(scores["driver_id"]))))

engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
query = "SELECT * FROM `drt_driver` WHERE `id` IN"+drivers_id
phone_numbers = pd.read_sql_query(query,con=engine) 
phone_numbers = phone_numbers [["id","tel","name","surname","pin"]]
phone_numbers = phone_numbers[phone_numbers["tel"]!= ""]
phone_numbers = phone_numbers.rename(columns={"id":"driver_id"})

final_scores = pd.merge(scores, phone_numbers, how ="left", on ="driver_id")
final_scores = final_scores.drop(["ranking","name","surname","id_contest","batch_number","driver_id","id","datetime"],1)

def number_to_str(x):
    new_x = str(x).split(".")[0]
    new_x = new_x if len(new_x) == 8 else '0' + new_x
    return new_x

final_scores = final_scores.dropna()
final_scores["tel"] = final_scores["tel"].apply(number_to_str)
final_scores = final_scores[["driver_name","continuous_score_acceleration","continuous_score_braking","continuous_score_cruise_speed","score_burn_rate","tel"]]
print("The final scores are:")
print(final_scores.head())

text_template = """
Concours Ecoconduite Optimizer :

Vos résultats du concours à ce jour sont:
Accélération : %s sur 10
Freinage : %s sur 10
Maintien de la vitesse : %s sur 10

Pour rappel, le concours a débuté le 23 avril et prendra fin le 23 juillet. 

Bonne route !
"""

# prepare and send the messages
for t in final_scores.itertuples():
    if type(t[6]) is not str:
        continue
    text_sms = text_template % (t[2],t[3],t[4]) # For Ivory Coast
    print(text_sms)
    response = client.send_message({'from': 'Optimine', 'to': '00225' + t[6], 'text': text_sms}) # For Ivory Coast
    response = response['messages'][0]

    if response['status'] == '0':
        print('Sent message', response['message-id'])
        remaining_balance = response['remaining-balance']
    else:
        print('Error:', response['error-text'])

print('Remaining balance is', remaining_balance)

