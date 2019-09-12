import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import time
import smtplib
import os.path as op
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders
import os
from pandas import ExcelWriter



params = {
    "day_date":time.strftime('%Y-%m-%d',time.localtime()),
    "client_drt":"bonikro", 
    "client":"bonikro_drt_ecodriving_v4",
    "client1":"MailingList"
}

#KPI drivers with logs 
engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
query= "SELECT DISTINCT id_contest FROM `ecodriving_move_scores`"
id_contest=pd.read_sql_query(query,con=engine) 
B=pd.DataFrame(columns=['Contest', 'Batch', 'Drivers_with_logs', 'Drivers_without_logs']) 
for i in range(len(id_contest)):
    if np.isnan((id_contest.iloc[i,0]))==False:
        engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
        query="SELECT DISTINCT id_batch FROM `ecodriving_move_scores` WHERE `id_contest`=" +  str(int(id_contest.iloc[i]))
        batch_id= pd.read_sql_query(query,con=engine)
        for j in range(len(batch_id)):
            query= "SELECT DISTINCT driver_id from `ecodriving_move_scores` WHERE driver_id IS NOT NULL AND id_contest=" + str(int(id_contest.iloc[i])) + " AND id_batch=" + str(int(batch_id.iloc[j]))
            Drivers=len(pd.read_sql_query(query,con=engine))
            query= "SELECT DISTINCT driver_id from `ecodriving_move_scores` WHERE driver_id IS NULL AND id_contest=" + str(int(id_contest.iloc[i])) + " AND id_batch=" + str(int(batch_id.iloc[j]))
            DriversNA=len(pd.read_sql_query(query,con=engine))
            B.loc[B.shape[0]]=[str(int(id_contest.iloc[i])), str(int(batch_id.iloc[j])), Drivers, DriversNA]


#KPI drivers with scores 
engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
query= "SELECT DISTINCT id_contest FROM `ecodriving_move_scores`"
id_contest=pd.read_sql_query(query,con=engine)
Acceleration=pd.DataFrame(columns=['Contest', 'Batch', 'Drivers_with_Acceleration_Score', 'Drivers_without_Acceleration_Score'])
Braking=pd.DataFrame(columns=['Contest', 'Batch', 'Drivers_with_Braking_Score', 'Drivers_without_Braking_Score'])
CruiseSpeed=pd.DataFrame(columns=['Contest', 'Batch', 'Drivers_with_CruiseSpeed_Score', 'Drivers_without_CruiseSpeed_Score']) 
ScoreGlobal=pd.DataFrame(columns=['Contest', 'Batch', 'Drivers_with_global_score', 'Drivers_without_global_score'])
for i in range(len(id_contest)):
    if np.isnan((id_contest.iloc[i,0]))==False:
        engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
        query="SELECT DISTINCT id_batch FROM `ecodriving_move_scores` WHERE `id_contest`=" +  str(int(id_contest.iloc[i]))
        batch_id= pd.read_sql_query(query,con=engine)
        for j in range(len(batch_id)):
#acceleration
            query= "SELECT DISTINCT score_acceleration from `ecodriving_move_scores` WHERE score_acceleration IS NOT NULL AND id_contest=" + str(int(id_contest.iloc[i])) + " AND id_batch=" + str(int(batch_id.iloc[j]))
            ScoreAcc=len(pd.read_sql_query(query,con=engine))
            query= "SELECT DISTINCT score_acceleration from `ecodriving_move_scores` WHERE score_acceleration IS NULL AND id_contest=" + str(int(id_contest.iloc[i])) + " AND id_batch=" + str(int(batch_id.iloc[j]))
            ScoreAccNA=len(pd.read_sql_query(query,con=engine))
            Acceleration.loc[Acceleration.shape[0]]=[str(int(id_contest.iloc[i])), str(int(batch_id.iloc[j])), ScoreAcc, ScoreAccNA]
#brakings
            query= "SELECT DISTINCT score_braking from `ecodriving_move_scores` WHERE score_braking IS NOT NULL AND id_contest=" + str(int(id_contest.iloc[i])) + " AND id_batch=" + str(int(batch_id.iloc[j]))
            ScoreBra=len(pd.read_sql_query(query,con=engine))
            query= "SELECT DISTINCT score_braking from `ecodriving_move_scores` WHERE score_braking IS NULL AND id_contest=" + str(int(id_contest.iloc[i])) + " AND id_batch=" + str(int(batch_id.iloc[j]))
            ScoreBraNA=len(pd.read_sql_query(query,con=engine))
            Braking.loc[Braking.shape[0]]=[str(int(id_contest.iloc[i])), str(int(batch_id.iloc[j])), ScoreBra, ScoreBraNA]
#Cruise_Speed
            query= "SELECT DISTINCT score_cruise_speed from `ecodriving_move_scores` WHERE score_cruise_speed IS NOT NULL AND id_contest=" + str(int(id_contest.iloc[i])) + " AND id_batch=" + str(int(batch_id.iloc[j]))
            ScoreCp=len(pd.read_sql_query(query,con=engine))
            query= "SELECT DISTINCT score_cruise_speed from `ecodriving_move_scores` WHERE score_cruise_speed IS NULL AND id_contest=" + str(int(id_contest.iloc[i])) + " AND id_batch=" + str(int(batch_id.iloc[j]))
            ScoreCpNA=len(pd.read_sql_query(query,con=engine))
            CruiseSpeed.loc[CruiseSpeed.shape[0]]=[str(int(id_contest.iloc[i])), str(int(batch_id.iloc[j])), ScoreCp, ScoreCpNA]
#global_scores
            query= "SELECT DISTINCT driver_id from `ecodriving_continuous_results` WHERE global_scores IS NOT NULL AND id_contest=" + str(int(id_contest.iloc[i])) + " AND batch_number=" + str(int(batch_id.iloc[j])) 
            ScoreG=len(pd.read_sql_query(query,con=engine))
            query= "SELECT DISTINCT driver_id from `ecodriving_continuous_results` WHERE global_scores IS  NULL AND id_contest=" + str(int(id_contest.iloc[i])) + " AND batch_number=" + str(int(batch_id.iloc[j])) 
            ScoreGNA=len(pd.read_sql_query(query,con=engine))
            ScoreGlobal.loc[ScoreGlobal.shape[0]]=[str(int(id_contest.iloc[i])), str(int(batch_id.iloc[j])), ScoreG, ScoreGNA]

            
ttt=pd.concat([ScoreGlobal.reset_index(drop=True), CruiseSpeed[['Drivers_with_CruiseSpeed_Score','Drivers_without_CruiseSpeed_Score']]], axis=1)
eee=pd.concat([Braking.reset_index(drop=True), Acceleration[['Drivers_with_Acceleration_Score','Drivers_without_Acceleration_Score']]],axis=1)
fff=pd.concat([ttt, eee[['Drivers_with_Braking_Score','Drivers_without_Braking_Score','Drivers_with_Acceleration_Score','Drivers_without_Acceleration_Score']]], axis=1)
F=pd.concat([fff,B[['Drivers_with_logs','Drivers_without_logs']]], axis=1)


def load(params,F):
    engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client"])
    F.to_sql(con=engine,name='Sanity_Checks', if_exists='replace', index=False)

load(params,F)
	
writer=pd.ExcelWriter('SanityChecks_'+str(params["client_drt"])+"_"+str(params['day_date'])[0:11]+".xlsx")
F.to_excel(writer,'Sheet1')
writer.save()

engine = create_engine("mysql+mysqldb://test:Xt6RZHQK5j5rb5cs@164.132.195.254/"+params["client1"])
query= "SELECT  Adresse_Mail from `Mailing_List_Sanity_Checks` WHERE Nom_Mine=\'" +  params["client_drt"] + "\'"
send_to=list(pd.read_sql_query(query,con=engine).iloc[:,0])


def send_mail(send_from, send_to, subject, message, files=[],
              server='smtp-mail.outlook.com', port=587, username='', password='',
              use_tls=True):
    """Compose and send email with provided info and attachments.

    Args:
        send_from (str): from name
        send_to (str): to name
        subject (str): message title
        message (str): message body
        files (list[str]): list of file paths to be attached to email
        server (str): mail server host name
        port (int): port number
        username (str): server auth username
        password (str): server auth password
        use_tls (bool): use TLS mode
    """
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(message))

    for path in files:
        part = MIMEBase('application', "octet-stream")
        with open(path, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename="{}"'.format(op.basename(path)))
        msg.attach(part)

    smtp = smtplib.SMTP(server, port)
    if use_tls:
        smtp.starttls()
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()

send_mail('ismail.alaoui@eleven-strategy.com',send_to,'Sanity Checks reports', 'Dear team please find enclosed the latest sanity check report\n\n Best regards, \n\n Ismail', [os.getcwd()+'/SanityChecks_'+str(params["client_drt"])+"_"+str(params['day_date'])[0:11]+".xlsx"],   server='smtp-mail.outlook.com', port=587, username='ismail.alaoui@eleven-strategy.com', password='lon1Alon2A!',use_tls=True)


os.remove(os.getcwd()+'/SanityChecks_'+str(params["client_drt"])+"_"+str(params['day_date'])[0:11]+".xlsx")