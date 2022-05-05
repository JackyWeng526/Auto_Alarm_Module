#%% Import pakages
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import pymssql
import os


# %% Setup the python scripts and file paths
BATH_PATH = os.path.dirname(os.path.abspath(__file__))
ETC_PATH = os.path.join(BATH_PATH, "..", "etc")
DATA_PATH = os.path.join(BATH_PATH, "..", "data")
BA_Point_filename = [f for f in os.listdir(ETC_PATH) if ("BA_Point_List" in f) & (f.endswith(".xlsx"))]
BA_Point_Table = BA_Point_filename[0]
Condition_Table_filename = [f for f in os.listdir(ETC_PATH) if ("Conditions" in f) & (f.endswith(".xlsx"))]
Condition_Table = Condition_Table_filename[0]

########################################################################################################
MSSQL_IP = "122.116.215.201:51433"
MSSQL_user = "EcoFirst_admin"
MSSQL_password = "1qaz@WSX5487"
MSSQL_database = "TaipeiPost"
MSSQL_table = "dbo.BA_point_Data"
# MSSQL_table = "dbo.raw_data"
time_lag = 0 # minutes
time_period = 10 # minutes
TEST_Point = "CHL400-Tco"
########################################################################################################


# %%
# Obtain BA points from TCP protocols
def get_TCP_point(Point_ID: list):
    point_list = pd.read_excel(os.path.join(ETC_PATH, BA_Point_Table), sheet_name="tcp") # Reading tcp page of the Point Table
    point_list = point_list[point_list["Protocol"]=="modbus_tcp"] # Check types of protocols
    try:
        DF = point_list[point_list["Data_Name"].isin(Point_ID)].iloc[:, 2:]
        return DF
    except:
        DF = point_list[point_list["Data_Name"].isin([Point_ID])].iloc[:, 2:]
        return DF


get_TCP_point([TEST_Point])
# %%
# Collect BA Data from MS SQL **Note: It's better to execute cursor directly with MSSQL rather than use pd.read_sql
def get_point_data(points: list):
    # Gather Point_ID
    point_list = points

    # Set Timestamp
    res = datetime.timedelta(minutes=1).total_seconds()
    end = int(datetime.datetime.now().timestamp() / res) * res
    end = datetime.datetime.fromtimestamp(end) - datetime.timedelta(minutes=time_lag) 
    start = end - datetime.timedelta(minutes=time_period) 

    # SQL connection
    select_index = "Time"
    table = MSSQL_table
    conn = pymssql.connect(server=MSSQL_IP, user=MSSQL_user, password=MSSQL_password, database=MSSQL_database)   
    cur = conn.cursor()
    sql = F"SELECT * FROM {table} WHERE {select_index} BETWEEN '{start}' AND '{end}'"
    cur.execute(sql)
    data = cur.fetchall()
    conn.close()

    # Data process
    DF = pd.DataFrame(data).copy()
    DF.columns = ["Time", "Point_ID", "Value"]
    DF = DF[DF["Point_ID"].isin(point_list)]
    DF = DF.sort_values(by="Time")
    DF.dropna(inplace=True)
    return DF

get_point_data([TEST_Point])
# %%
# Filter data with conditions
def filter_conditions(data):#, conditions: dict):
    BA_data = data.copy()
    Data_df = BA_data.pivot_table(index="Time", columns="Point_ID", values="Value").resample("5min").mean()
    
    return Data_df

filter_conditions(get_point_data([TEST_Point]))



# %%
# Threshold Conditions
def get_conditions():
    High_limit_Table = pd.read_excel(os.path.join(ETC_PATH, Condition_Table), sheet_name="High_limit")
    High_limit_Point_list = list(High_limit_Table["Point_ID"].unique())
    Low_limit_Table = pd.read_excel(os.path.join(ETC_PATH, Condition_Table), sheet_name="Low_limit")
    Low_limit_Point_list = list(Low_limit_Table["Point_ID"].unique())
    Alarm_list = {"High_limit": {}, "Low_limit": {}}

    # High limit determination
    High_limit_DF = get_point_data(High_limit_Point_list)
    for id in High_limit_Point_list:
        df_temp = High_limit_DF[High_limit_DF["Point_ID"]==id]
        resample_time = int(High_limit_Table[High_limit_Table["Point_ID"]==id]["Time_period_minutes"].values)
        thredhold = High_limit_Table[High_limit_Table["Point_ID"]==id]["Threshold"].values
        resample_data = df_temp.pivot_table(index="Time", columns="Point_ID", values="Value").resample(F"{resample_time}min").mean()
        data_value = resample_data.iloc[-1].values[0]
        if data_value > thredhold:
            print(F"{id}: High limit warning!")
            Alarm_list["High_limit"][id] = {
                "Time": resample_data.index[-1], 
                "Event": "High limit warning!", 
                "Threshold": thredhold, "Value": data_value
                }
            msg = Alarm_list["High_limit"][id]
            main(msg)
        else:
            pass
    
    # Low limit determination
    Low_limit_DF = get_point_data(Low_limit_Point_list)
    for id in Low_limit_Point_list:
        df_temp = Low_limit_DF[Low_limit_DF["Point_ID"]==id]
        resample_time = int(Low_limit_Table[Low_limit_Table["Point_ID"]==id]["Time_period_minutes"].values)
        thredhold = Low_limit_Table[Low_limit_Table["Point_ID"]==id]["Threshold"].values[0]
        resample_data = df_temp.pivot_table(index="Time", columns="Point_ID", values="Value").resample(F"{resample_time}min").mean()
        if data_value < thredhold:
            print(F"{id}: Low limit warning!")
            Alarm_list["Low_limit"][id] = {
                "Time": resample_data.index[-1].isoformat(), 
                "Event": "Low limit warning!", 
                "Threshold": thredhold, 
                "Value": data_value,
                "事件描述": Low_limit_Table[Low_limit_Table["Point_ID"]==id]["事件描述"].values[0], 
                "建議處理方式": Low_limit_Table[Low_limit_Table["Point_ID"]==id]["ACTION"].values[0], 
                }
            msg = json.dumps(Alarm_list["Low_limit"][id])
            main(msg)
        else:
            pass

    return resample_data, Alarm_list

test = get_conditions()
test


# %%
def lineNotifyMessage(token, msg):
    headers = {"Authorization": F"Bearer {token}", "Content-Type" : "application/x-www-form-urlencoded"}
    payload = {"message": msg}
    r = requests.post("https://notify-api.line.me/api/notify", headers=headers, params=payload)
    return r.status_code

def main(msg):
    message = msg
    # token_Jacky = "AJ5cbCcuyz4Podeok3TAgpSXHMrqKCJx4uIRjfaJqsx"
    # token_Weather_Warning = "sWYZ7LOam6YQdWQN8XZqUTM5N0YcESda1yNaZnts3WK"
    token_ecofirst_test = "nUpbTpujLAkd7V13tUAFsOc4oIm6AtYm21fpUtgOryd"
    line_token = token_ecofirst_test
    lineNotifyMessage(line_token, message)
    print("1")

# %%
# conditions = {
#     "Point_ID": TEST_Point,
#     "Condition_Description": "Temperature of returned cooling water is higher than 24.5 degree C.",
#     "High_limit": 24.5
# }

# %%
if __name__=="__main__":
    get_conditions()
    time.sleep(2)

# %%
