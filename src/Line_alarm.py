"""
Service: Line BA Alarm & Notification
Version: 1.3.1
Developer: Jacky
Publisher: EcoFirst Tech. Ltd.
"""


# %% Import pakages & Setup the python scripts and file paths
# Packages
from apscheduler.schedulers.blocking import BlockingScheduler 
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR  
import pandas as pd
import numpy as np
import json
import datetime
import requests
import pymssql
import os
import uuid
import logging


# File path
BATH_PATH = os.path.dirname(os.path.abspath(__file__))
ETC_PATH = os.path.join(BATH_PATH, "..", "etc")
DATA_PATH = os.path.join(BATH_PATH, "..", "data")
TEST_PATH = os.path.join(BATH_PATH, "..", "TEST")


# Test data
TEST_Condition_Table_filename = [f for f in os.listdir(TEST_PATH) if ("TEST_Conditions" in f) & (f.endswith(".xlsx"))]
TEST_Condition_Table = TEST_Condition_Table_filename[0]
TEST_Data_Table_filename = [f for f in os.listdir(TEST_PATH) if ("TEST_data" in f) & (f.endswith(".xlsx"))]
TEST_Data_Table = TEST_Data_Table_filename[0]
TEST_Data_Table


# %%
# Function of error recording
def error_log(error_txt):
    txt = {"ErrorType": error_txt, "Time": datetime.datetime.now().isoformat()}
    print(txt)
    if "error_log.txt" not in  os.listdir(ETC_PATH):
        f = open(os.path.join(ETC_PATH, "error_log.txt"), "w")
        f.write(F"{txt}\n")
        f.close()
    else:
        f = open(os.path.join(ETC_PATH, "error_log.txt"), "a")
        f.write(F"{txt}\n")
        f.close() 


# Function of get meta config
def get_config():
    with open(os.path.join(ETC_PATH, "config.json")) as jsonfile:
        config = json.load(jsonfile)
    return config


########################################################################################################
meta = get_config()
# MSSQL
MSSQL_IP = meta["MSSQL"]["IP"]
MSSQL_user = meta["MSSQL"]["User"]
MSSQL_password = meta["MSSQL"]["Password"]
MSSQL_database = meta["MSSQL"]["Database"]
MSSQL_table_BA = meta["MSSQL"]["BA_Point_Data"]
MSSQL_table_Alarm_rule = meta["MSSQL"]["Alarm_Rule_Table"]
MSSQL_table_Events = meta["MSSQL"]["Alarm_Event_Table"]
time_lag = 0 # minutes
time_period = 20 # minutes
# Line notify-api
Line_api_url = meta["Line_API"]["Notify-api"]
# Line tokens
token_Jacky = meta["Line_API"]["Token"]["token_Jacky_test"]
token_ecofirst = meta["Line_API"]["Token"]["token_ecofirst_test"]
token_Alarm_Group = meta["Line_API"]["Token"]["token_Alarm_Group"]
Selected_Token = token_Alarm_Group

TEST_Point = ["CHL400_Chiller_ON_OFF"]
# TEST_Point = ["CHL120_Chiller_ON_OFF"]
########################################################################################################


# %% Line API
def lineNotifyMessage(token, msg):
    headers = {"Authorization": F"Bearer {token}", "Content-Type" : "application/x-www-form-urlencoded"}
    payload = {"message": msg}
    r = requests.post("https://notify-api.line.me/api/notify", headers=headers, params=payload)
    return r.status_code


def line_api_main(msg):
    message = msg
    line_token = Selected_Token
    lineNotifyMessage(line_token, message)


# %%
class Line_Notify_module:
    def __init__(self):
        self.Single_rule, self.Multi_rule, self.Point_list = self.get_condition_rules()
        # self.Single_rule, self.Multi_rule, self.Point_list = self.get_condition_rules_local()
        pass


    def get_condition_rules_local(self):
        # Get point rule from MSSQL
        rule_df = pd.read_excel(os.path.join(ETC_PATH, "BA_Point_Conditions_0505 .xlsx"))
        Single_rule = rule_df[rule_df["Related_Point_list"].isna()]
        Multi_rule = rule_df[~rule_df["Related_Point_list"].isna()]
        Point_list = list(rule_df.loc[:, "Point_ID"].unique())
        return Single_rule, Multi_rule, Point_list


    def get_condition_rules(self):
        # Get point rule from MSSQL
        rule_table = MSSQL_table_Alarm_rule
        conn = pymssql.connect(server=MSSQL_IP, user=MSSQL_user, password=MSSQL_password, database=MSSQL_database)   
        sql = F"SELECT * FROM {rule_table}"
        cur = conn.cursor(as_dict=True)
        cur.execute(sql)
        res = cur.fetchall()
        rule_df = pd.DataFrame(res)
        Single_rule = rule_df[rule_df["Related_Point_list"].isna()]
        Multi_rule = rule_df[~rule_df["Related_Point_list"].isna()]
        Point_list = list(rule_df.loc[:, "Point_ID"].unique())
        return Single_rule, Multi_rule, Point_list


    def get_point_data(self):
        # Gather Point_ID
        point_list = self.Point_list

        # Set Timestamp
        res = datetime.timedelta(minutes=1).total_seconds()
        # end = int(datetime.datetime(2022, 5, 5, 15, 53).timestamp() / res) * res
        end = int(datetime.datetime.now().timestamp() / res) * res
        end = datetime.datetime.fromtimestamp(end) - datetime.timedelta(minutes=time_lag) 
        start = end - datetime.timedelta(minutes=time_period) 

        # SQL connection
        select_index = "Time"
        table = MSSQL_table_BA
        conn = pymssql.connect(server=MSSQL_IP, user=MSSQL_user, password=MSSQL_password, database=MSSQL_database)   
        cur = conn.cursor(as_dict=True)
        sql = F"SELECT * FROM {table} WHERE {select_index} BETWEEN '{start}' AND '{end}'"
        cur.execute(sql)
        data = cur.fetchall()
        conn.close()

        # Data process
        DF = pd.DataFrame(data).copy()
        DF = DF[DF["Point_ID"].isin(point_list)]
        DF = DF.sort_values(by="Time")
        DF["Value"] = pd.to_numeric(DF["Value"])
        DF = DF.drop_duplicates(subset=["Time", "Point_ID"], keep="last")
        return DF


    def SingleRule_event(self):
        rule_table = self.Single_rule
        data_table = self.get_point_data()

        Alarm_list = {"Activate": [], "Deactivate": []}
        Method_list = list(rule_table.loc[:, "Method"].unique())

        for method in Method_list:
            select_method_df = rule_table[rule_table["Method"]==method]
            select_method_df.reset_index(drop=True, inplace=True)
            for idx in select_method_df.index:
                point_id = select_method_df.loc[idx, "Point_ID"]
                df_temp = data_table[data_table["Point_ID"]==point_id]
                resample_data = df_temp.pivot(index="Time", columns="Point_ID", values="Value").resample("1min").mean()
                resample_data = resample_data.resample(F"1min").fillna("pad")
                delay_time = int(select_method_df[select_method_df["Point_ID"]==point_id]["Time_period_minutes"].values)
                thredhold = float(select_method_df[select_method_df["Point_ID"]==point_id]["Threshold"].values[0])
                print(point_id, ": ", delay_time, "min")
                if method == "小於": 
                    resample_data.loc[:, "Status"] = np.where((resample_data[point_id] - thredhold)<0, 1, 0) 
                if method == "大於":
                    resample_data.loc[:, "Status"] = np.where((resample_data[point_id] - thredhold)>0, 1, 0)  ###
                resample_data.loc[:, "Delay_Status"] = resample_data["Status"].rolling(delay_time).sum()
                resample_data.loc[:, "Alarm"] = resample_data["Delay_Status"] - resample_data["Delay_Status"].shift(1) 
                resample_data = resample_data.dropna()
                Delay_status = resample_data.iloc[-1]["Delay_Status"]
                Alarm_status = resample_data.iloc[-1]["Alarm"]
                data_value = resample_data.iloc[-1][point_id]
                dtn = resample_data.index[-1].replace(second=0, microsecond=0).isoformat()
                if (Delay_status == delay_time) and (Alarm_status == 1):
                    print(F"{point_id}: Warning!")
                    Alarm_list["Activate"].append({
                        "Point_ID": point_id,
                        "Time": dtn, 
                        "Event": rule_table.iloc[idx]["Event_Trigger"], 
                        "Threshold": thredhold, 
                        "Value": data_value,
                        "Advice": rule_table.iloc[idx]["Advice"],
                        "Status": "事件追蹤中", 
                        "Finish_Time": None, 
                        "Memo": None,
                        "Event_ID": str(uuid.uuid1())})
                if (Delay_status == delay_time-1) and (Alarm_status == -1):
                    print(F"{point_id}: Warning!")
                    Alarm_list["Deactivate"].append({
                        "Point_ID": point_id,
                        "Time": dtn, 
                        "Event": rule_table.iloc[idx]["Event_Dismiss"], 
                        "Threshold": thredhold, 
                        "Value": data_value,
                        "Advice": "已恢復正常", 
                        "Status": "Event finished", 
                        "Finish_Time": dtn})         
        return {"Activate": pd.DataFrame(Alarm_list["Activate"]), "Deactivate": pd.DataFrame(Alarm_list["Deactivate"])}


    def MultiRule_event(self):
        rule_table = self.Multi_rule
        data_table = self.get_point_data()

        Alarm_list = {"Activate": [], "Deactivate": []}
        Method_list = list(rule_table.loc[:, "Method"].unique())

        for method in Method_list:
            select_method_df = rule_table[rule_table["Method"]==method]
            select_method_df.reset_index(drop=True, inplace=True)
            for idx in select_method_df.index:
                point_id = select_method_df.loc[idx, "Point_ID"]
                related_id = select_method_df.loc[idx, "Related_Point_list"]
                df_temp = data_table[data_table["Point_ID"].isin([point_id, related_id])]
                resample_data = df_temp.pivot(index="Time", columns="Point_ID", values="Value").resample("1min").mean()
                resample_data = resample_data.resample(F"1min").fillna("pad")
                data_temp = resample_data[[point_id, related_id]]
                delay_time = int(select_method_df[select_method_df["Point_ID"]==point_id]["Time_period_minutes"].values)
                print(F"{point_id}: {delay_time} min (Related_point: {related_id})")
                
                # First condition
                thredhold_0 = float(select_method_df[select_method_df["Point_ID"]==point_id]["Related_Threshold_list"].values[0])
                df_0 = data_temp[[related_id]].copy()
                if method == "小於": 
                    df_0.loc[:, "Status"] = np.where((df_0[related_id] - thredhold_0)<0, 1, 0) 
                if method == "大於":
                    df_0.loc[:, "Status"] = np.where((df_0[related_id] - thredhold_0)>0, 1, 0) 
                df_0.loc[:, "Delay_Status"] = df_0["Status"].rolling(delay_time).sum()
                df_0 = df_0.dropna()
                First_condition_Status = df_0.iloc[-1]["Delay_Status"]

                # Main Condition
                thredhold = float(select_method_df[select_method_df["Point_ID"]==point_id]["Threshold"].values[0])
                df_main = data_temp[[point_id]].copy()
                if method == "小於": 
                    df_main.loc[:, "Status"] = np.where((df_main[point_id] - thredhold)<0, 1, 0) 
                if method == "大於":
                    df_main.loc[:, "Status"] = np.where((df_main[point_id] - thredhold)>0, 1, 0)  ###
                df_main.loc[:, "Delay_Status"] = df_main["Status"].rolling(delay_time).sum()
                df_main.loc[:, "Alarm"] = df_main["Delay_Status"] - df_main["Delay_Status"].shift(1) 
                df_main = df_main.dropna()
                Delay_status = df_main.iloc[-1]["Delay_Status"]
                Alarm_status = df_main.iloc[-1]["Alarm"]
                data_value = df_main.iloc[-1][point_id]
                dtn = df_main.index[-1].replace(second=0, microsecond=0).isoformat()
                
                if ((Delay_status == delay_time) and (Alarm_status == 1)) and (First_condition_Status == delay_time):
                    print(F"{point_id}: Warning!")
                    Alarm_list["Activate"].append({
                        "Point_ID": point_id,
                        "Time": dtn, 
                        "Event": rule_table.iloc[idx]["Event_Trigger"], 
                        "Threshold": thredhold, 
                        "Value": data_value,
                        "Advice": rule_table.iloc[idx]["Advice"],
                        "Status": "事件追蹤中", 
                        "Finish_Time": None, 
                        "Memo": None,
                        "Event_ID": str(uuid.uuid1())})
                if ((Delay_status == delay_time-1) and (Alarm_status == -1)) or (First_condition_Status == delay_time-1):
                    print(F"{point_id}: Warning!")
                    Alarm_list["Deactivate"].append({
                        "Point_ID": point_id,
                        "Time": dtn, 
                        "Event": rule_table.iloc[idx]["Event_Dismiss"], 
                        "Threshold": thredhold, 
                        "Value": data_value,
                        "Advice": "已恢復正常", 
                        "Status": "Event finished", 
                        "Finish_Time": dtn})      
        return {"Activate": pd.DataFrame(Alarm_list["Activate"]), "Deactivate": pd.DataFrame(Alarm_list["Deactivate"])}

    def get_Events(self):
        Single_Rule_Event, Multi_Rule_Event = {"Activate": pd.DataFrame(), "Deactivate": pd.DataFrame()}, {"Activate": pd.DataFrame(), "Deactivate": pd.DataFrame()}
        # Single_Rule_Event
        try:
            Single_Rule_Event = self.SingleRule_event()
        except:
            pass
        
        # Multi_Rule_Event
        try:
            Multi_Rule_Event = self.MultiRule_event()
        except:
            pass

        Act_DF = pd.concat([Single_Rule_Event["Activate"], Multi_Rule_Event["Activate"]], axis=0)
        Deact_DF = pd.concat([Single_Rule_Event["Deactivate"], Multi_Rule_Event["Deactivate"]], axis=0)
        return {"Activate": Act_DF, "Deactivate": Deact_DF}

# Line_Notify_module().get_Events()


# %%
# Save Event
def save_event(events: dict):
    if len(events["Activate"]) == 0:
        print("No new event saved.")
        return 
    disiredOrder = ["Time", "Event", "Point_ID", "Threshold", "Value", "Advice", "Event_ID", "Status", "Finish_Time", "Memo"]
    Event_df = events["Activate"].loc[:, disiredOrder]
    Event_df.reset_index(drop=True, inplace=True)
    # SQL connection
    DF = Event_df.astype(object).where(pd.notnull(Event_df), None)
    table = MSSQL_table_Events
    conn = pymssql.connect(server=MSSQL_IP, user=MSSQL_user, password=MSSQL_password, database=MSSQL_database)   
    cur = conn.cursor()
    wildcards = "%s, %s, %s, %d, %d, %s, %s, %s, %s, %s"   ### 型別問題，待自動化
    data = [tuple(x) for x in DF.values]
    cur.executemany(F"INSERT INTO {table} VALUES({wildcards})", data)
    conn.commit()
    cur.close()
    conn.close()
    print("New event saved.")
    return data
# save_event(Line_Notify_module().get_Events())


# Solved Event
def solved_event(events: dict):
    if len(events["Deactivate"]) == 0:
        print("No current event dismissed.")
        return 
    dtn = datetime.datetime.now().replace(microsecond=0).isoformat()
    disiredOrder = ["Time", "Event", "Point_ID", "Threshold", "Value", "Advice", "Status", "Finish_Time"]
    Event_df = events["Deactivate"].loc[:, disiredOrder]
    print(Event_df)
    Event_df.set_index("Time", drop=True, inplace=True)
    print(Event_df)
    # SQL connection
    Total_UP_DF_list = []
    DF = Event_df.astype(object).where(pd.notnull(Event_df), None)
    table = MSSQL_table_Events
    conn = pymssql.connect(server=MSSQL_IP, user=MSSQL_user, password=MSSQL_password, database=MSSQL_database)   
    cur = conn.cursor(as_dict=True)
    for idx in DF.index:
        cur.execute(F"SELECT * FROM {table} WHERE Point_ID = '{Event_df.loc[idx, 'Point_ID']}'")
        row = cur.fetchall()
        Res_df = pd.DataFrame(row)
        Res_df["Time"] = pd.to_datetime(Res_df["Time"])
        Res_df.sort_values(by=["Time"], inplace=True)
        Update_df = Res_df.iloc[[-1], :]
        cur.execute(F"UPDATE {table} SET Status = 'Finished', Finish_Time = '{dtn}' WHERE Event_ID = '{Update_df.iloc[-1]['Event_ID']}'")
        conn.commit()
        Total_UP_DF_list.append(Update_df)
    cur.close()
    conn.close()
    Total_UP_DF = pd.concat(Total_UP_DF_list, axis=0)
    print("Current event dismissed.")
    return Total_UP_DF
# solved_event(Line_Notify_module().get_Events())


# %%
# Function Gate
def get_line_msg(msg):
    msg_dict = msg
    total_msg = {"Activate": [], "Deactivate": []}
    if len(msg_dict["Activate"])>0:
        temp = msg_dict["Activate"].to_dict("records")
        for key in temp:
            total_msg["Activate"].append(key)
    if len(msg_dict["Deactivate"])>0:
        temp = msg_dict["Deactivate"].to_dict("records")
        for key in temp:
            total_msg["Deactivate"].append(key)    
    return total_msg
# get_line_msg(Line_Notify_module().get_Events())


# %%
def line_msg(msg):
    # Return if nothing
    if msg == {"Activate": [], "Deactivate": []}:
        print("No Line msg sent.")
        return 
    sent_msg = "\n"

    # Preprocess line msg
    if len(msg["Activate"]) > 0:
        sent_msg += "📢事件發生: \n "
        print(8)
        for _msg in msg["Activate"]:
            sent_msg += F'事件: {_msg["Event"]}\n'
            sent_msg += F'現場數值 = {_msg["Value"]} \n '
            sent_msg += F'邊界值 = {_msg["Threshold"]} \n '
            sent_msg += F'Advice: {_msg["Advice"]}\n'
            sent_msg += F'(時間戳記: {_msg["Time"]})\n\n '
            print(9)
        sent_msg += "\n "

    if len(msg["Deactivate"]) > 0:
        sent_msg += "🆗Event_Dismiss: \n "
        for _msg in msg["Deactivate"]:
            sent_msg += F'事件: {_msg["Event"]}\n'
            sent_msg += F'{_msg["Point_ID"]} = {_msg["Value"]} \n '
            sent_msg += F'邊界值 = {_msg["Threshold"]} \n '
            sent_msg += F'狀態: {_msg["Advice"]}\n\n '

    # Send line msg
    line_api_main(sent_msg)
    print("Line msg send!")
# line_msg(get_line_msg(Line_Notify_module().get_Events()))


#%%
def Task_line_msg():
    print("Tasks start")
    msg_dict = Line_Notify_module().get_Events()

    # Event Record
    try:
        save_event(msg_dict)
    except:
        error_text = "Error on event save"
        print(error_text)
        error_log(error_text)
        pass
    try:
        solved_event(msg_dict)
    except:
        error_text = "Error on event dismiss"
        print(error_text)
        error_log(error_text)
        pass    
    
    # Line msg
    try:
        msg = get_line_msg(msg_dict)
        line_msg(msg)
    except:
        error_text = "Error on line msg"
        print(error_text)
        error_log(error_text)
        pass 
    
    # Finish
    print("Tasks finished!")
Task_line_msg()


# %% APScheduler Function
def job_func(job_id):
    dtn = datetime.datetime.now()
    print(F"job {job_id} is runed at {dtn.isoformat()}")  
    task = Task_line_msg()


def job_exception_listener(event):  
    if event.exception:  
        # todo：異常處理, 告警等  
        error_text = "The job crashed!"
        print(error_text)
        error_log(error_text)
    else:  
        print("The job worked~") 


# %%
if __name__=="__main__":
    # Process logging
    logging.basicConfig()  
    logging.getLogger("apscheduler").setLevel(logging.DEBUG) 

    # Create scheduler by aps
    scheduler = BlockingScheduler()  
    scheduler.add_job(
        job_func, id="1", name="task_Line_note", 
        trigger="interval", minutes=1, max_instances=2, misfire_grace_time=1, 
        jobstore="default", executor="default", args=[1]) 
    scheduler.add_listener(
        job_exception_listener, 
        EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)  
    scheduler.start() 
    print(scheduler.get_jobs())


# %%
