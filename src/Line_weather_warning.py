#%% Import pakages
import pandas as pd
import numpy as np
import sqlite3
import requests
import json
import datetime
import time
import os


BATH_PATH = os.path.dirname(os.path.abspath(__file__))


def get_special_warning():
    # update weather special warning
    location_special = "W-C0033-001"
    params_CWB = {
        "authorizationkey": "CWB-31C20651-3019-4C26-8B40-D1CAB757200B",
        "format": "json"}    

    url = F"https://opendata.cwb.gov.tw/api/v1/rest/datastore/{location_special}"
    res = requests.get(url, params=params_CWB)

    res_json = json.loads(res.content.decode('utf-8'))
    res_df = pd.DataFrame.from_dict(res_json["records"]["location"])
    res_df["警特報狀況"] = res_df.loc[:, ["hazardConditions"]].applymap(lambda x: F'{x["hazards"][0]["info"]["phenomena"]}/{x["hazards"][0]["info"]["significance"]}' if x["hazards"]!=[] else np.nan)
    res_df["startTime"] = res_df.loc[:, ["hazardConditions"]].applymap(lambda x: F'{x["hazards"][0]["validTime"]["startTime"]}' if x["hazards"]!=[] else np.nan)
    res_df["endTime"] = res_df.loc[:, ["hazardConditions"]].applymap(lambda x: F'{x["hazards"][0]["validTime"]["endTime"]}' if x["hazards"]!=[] else np.nan)
    
    res_df = res_df.drop(["geocode", "hazardConditions"], axis=1)
    res_df = res_df.rename(columns={"locationName": "縣市"})
    return res_df


def get_district_msg():
    # update weather special warning
    special_district = "W-C0033-002"
    params_CWB = {
        "authorizationkey": "CWB-31C20651-3019-4C26-8B40-D1CAB757200B",
        "format": "json"}    

    url = F"https://opendata.cwb.gov.tw/api/v1/rest/datastore/{special_district}"
    res = requests.get(url, params=params_CWB)

    res_json = json.loads(res.content.decode('utf-8'))
    if res_json["records"]["record"]!=[]:
        res_time = res_json["records"]["record"][0]["datasetInfo"]["validTime"]["startTime"]
        res_msg = res_json["records"]["record"][0]["contents"]["content"]["contentText"].rstrip().strip()
        df = pd.DataFrame(columns=["Time", "msg"])
        df.loc[0, :] = [res_time, res_msg]
        return df
    else:
        pass


def save_warning():
    df = get_special_warning()
    df = df.dropna()
    dist_list = list(df.loc[:, "縣市"])
    # if len(df)==0:
    try:
        msg_data = get_district_msg()
        table = "MSG"
        date_time = "Time"
        path = os.path.join(BATH_PATH, "Weather_Warning.db")
        conn = sqlite3.connect(path)    
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_list = [i[0] for i in cursor.fetchall()]
        if (len(msg_data) >= 0) & (table not in table_list):
            conn = sqlite3.connect(path)
            msg_data.to_sql(name=table, con=conn, if_exists="append")
            sql_max_date = F"SELECT * FROM {table} WHERE ({date_time}, 'msg') IN ( SELECT MAX({date_time}), 'msg' FROM {table} )"
            Max_date = pd.read_sql(sql=sql_max_date, con=conn)            
            conn.close()
            print(F"{table} is created!")
            Max_date = Max_date.drop(["index"], axis=1)
            Max_date = Max_date.reset_index(drop=True)
            msg_time = Max_date.loc[0, "Time"]
            msg_txt = Max_date.loc[0, "msg"]
            msg = F"[{msg_time}] {msg_txt}"
            main(msg)

        elif (len(msg_data) >= 0) & (table in table_list):
            sql_count_before = cursor.execute(F"SELECT COUNT(*) FROM {table}")
            count_b = sql_count_before.fetchone()

            for i in msg_data.index:
                cursor.execute(F"SELECT * FROM {table} WHERE {date_time} BETWEEN '{str(msg_data[date_time].loc[i])}' AND '{str(msg_data[date_time].loc[i])}'")
                if len(cursor.fetchall()) == 1:
                    cursor.execute(F"DELETE FROM {table} WHERE {date_time} BETWEEN '{str(msg_data[date_time].loc[i])}' AND '{str(msg_data[date_time].loc[i])}'")
            conn.commit()
            msg_data.to_sql(name=table, con=conn, if_exists="append")  
            print(F"{table} is updated!")

            sql_count_after = cursor.execute(F"SELECT COUNT(*) FROM {table}")
            count_a = sql_count_after.fetchone()
            conn.close()

            if count_a[0] > count_b[0]:
                # conn = sqlite3.connect(path)    
                # sql_max_date = F"SELECT * FROM {table} WHERE ({date_time}, 'msg') IN ( SELECT MAX({date_time}), 'msg' FROM {table} )"
                # Max_date = pd.read_sql(sql=sql_max_date, con=conn)
                # Max_date = Max_date.drop(["index"], axis=1)
                # Max_date = Max_date.reset_index(drop=True)
                msg_time = msg_data.loc[0, "Time"]
                msg_txt = msg_data.loc[0, "msg"]
                msg = F"[{msg_time}] {msg_txt}"
                main(msg)
                conn.close()
    except:
        pass

    # else:
    msg_df = [] ###
    for dist_name in dist_list:
        df_data = df[df["縣市"]==dist_name]
        table = dist_name
        date_time = "startTime"
        path = os.path.join(BATH_PATH, "Weather_Warning.db")
        conn = sqlite3.connect(path)    
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_list = [i[0] for i in cursor.fetchall()]

        if (len(df_data) >= 0) & (table not in table_list):
            conn = sqlite3.connect(path)
            df_data.to_sql(name=table, con=conn, if_exists="append")
            sql_max_date = F"SELECT * FROM {table} WHERE ('縣市', '警特報狀況', {date_time}) IN ( SELECT '縣市', '警特報狀況', MAX({date_time}) FROM {table} )"
            Max_date = pd.read_sql(sql=sql_max_date, con=conn)            
            Max_date = Max_date.drop(["index"], axis=1)
            Max_date = Max_date.reset_index(drop=True)
            # msg_time = Max_date.loc[0, "Time"]
            # msg_txt = Max_date.loc[0, "msg"]
            # msg = F"[{msg_time}] {msg_txt}"
            # main(msg)
            conn.close()
            print(F"{table} is created!")
    
        elif (len(df_data) >= 0) & (table in table_list):
            sql_count_before = cursor.execute(F"SELECT COUNT(*) FROM {table}")
            count_b = sql_count_before.fetchone()

            for i in df_data.index:
                cursor.execute(F"SELECT * FROM {table} WHERE {date_time} BETWEEN '{str(df_data[date_time].loc[i])}' AND '{str(df_data[date_time].loc[i])}'")
                if len(cursor.fetchall()) == 1:
                    cursor.execute(F"DELETE FROM {table} WHERE {date_time} BETWEEN '{str(df_data[date_time].loc[i])}' AND '{str(df_data[date_time].loc[i])}'")
            conn.commit()
            df_data.to_sql(name=table, con=conn, if_exists="append")  
            print(F"{table} is updated!")

            sql_count_after = cursor.execute(F"SELECT COUNT(*) FROM {table}")
            count_a = sql_count_after.fetchone()
            conn.close()

            if count_a[0] > count_b[0]:
                conn = sqlite3.connect(path)    
                sql_max_date = F"SELECT * FROM {table} WHERE ('縣市', '警特報狀況', {date_time}) IN ( SELECT '縣市', '警特報狀況', MAX({date_time}) FROM {table} )"
                Max_date = pd.read_sql(sql=sql_max_date, con=conn)
                Max_date = Max_date.drop(["index"], axis=1)
                Max_date = Max_date.reset_index(drop=True)
                # msg_time = Max_date.loc[0, "Time"]
                # msg_txt = Max_date.loc[0, "msg"]
                # msg = F"[{msg_time}] {msg_txt}"
                # main(msg)
                conn.close()
    print("Mission Finnished!")


def lineNotifyMessage(token, msg):
  headers = {"Authorization": F"Bearer {token}", "Content-Type" : "application/x-www-form-urlencoded"}
  payload = {"message": msg}
  r = requests.post("https://notify-api.line.me/api/notify", headers=headers, params=payload)
  return r.status_code

def main(msg):
  message = msg
  token_Jacky = "AJ5cbCcuyz4Podeok3TAgpSXHMrqKCJx4uIRjfaJqsx"
#   token_Weather_Warning = "sWYZ7LOam6YQdWQN8XZqUTM5N0YcESda1yNaZnts3WK"
  line_token = token_Jacky
  lineNotifyMessage(line_token, message)

if __name__=="__main__":
  save_warning()  
  time.sleep(2)

# %%
