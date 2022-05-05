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


# %%
def get_config():
    with open(os.path.join(ETC_PATH, "config.json")) as jsonfile:
        config = json.load(jsonfile)
    return config


# %%
%%time
class Test_module:
    def __init__(self):
        self.Single_rule, self.Multi_rule = self.test_rule()
        pass


    def test_rule(self):
        rule_df = pd.read_excel(os.path.join(TEST_PATH, TEST_Condition_Table))
        Single_rule = rule_df[rule_df["Related_Point_list"].isna()]
        Multi_rule = rule_df[~rule_df["Related_Point_list"].isna()]
        return Single_rule, Multi_rule


    def test_data_m(self):
        data_df = pd.read_excel(os.path.join(TEST_PATH, TEST_Data_Table), sheet_name="Multi_rule")
        return data_df


    def test_event_SingleRule(self):
        rule_table = self.Single_rule
        data_table = self.test_data_m()
        data_table.set_index("Time", inplace=True)

        Alarm_list = {"Activate": [], "Deactivate": []}
        Method_list = list(rule_table.loc[:, "Method"].unique())

        for method in Method_list:
            select_method_df = rule_table[rule_table["Method"]==method]
            select_method_df.reset_index(drop=True, inplace=True)
            for idx in select_method_df.index[:1]:
                point_id = select_method_df.loc[idx, "Point_ID"]
                print(point_id)
                df_temp = data_table[[select_method_df.loc[idx, "Point_ID"]]]
                delay_time = int(select_method_df[select_method_df["Point_ID"]==point_id]["Time_period_minutes"].values)
                print(delay_time)
                thredhold = float(select_method_df[select_method_df["Point_ID"]==point_id]["Threshold"].values[0])
                resample_data = df_temp.resample(F"1min").fillna("pad")
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


    def test_event_MultiRule(self):
        rule_table = self.Multi_rule
        data_table = self.test_data_m()
        data_table.set_index("Time", inplace=True)

        Alarm_list = {"Activate": [], "Deactivate": []}
        Method_list = list(rule_table.loc[:, "Method"].unique())

        for method in Method_list:
            select_method_df = rule_table[rule_table["Method"]==method]
            select_method_df.reset_index(drop=True, inplace=True)
            for idx in select_method_df.index:
                point_id = select_method_df.loc[idx, "Point_ID"]
                related_id = select_method_df.loc[idx, "Related_Point_list"]
                print(point_id)
                df_temp = data_table[[point_id, related_id]]
                resample_data = df_temp.resample(F"1min").fillna("pad")
                delay_time = int(select_method_df[select_method_df["Point_ID"]==point_id]["Time_period_minutes"].values)
                print(delay_time)

                # First condition
                thredhold_0 = float(select_method_df[select_method_df["Point_ID"]==point_id]["Related_Threshold_list"].values[0])
                df_0 = resample_data[[related_id]].copy()
                if method == "小於": 
                    df_0.loc[:, "Status"] = np.where((df_0[related_id] - thredhold_0)<0, 1, 0) 
                if method == "大於":
                    df_0.loc[:, "Status"] = np.where((df_0[related_id] - thredhold_0)>0, 1, 0) 
                df_0.loc[:, "Delay_Status"] = df_0["Status"].rolling(delay_time).sum()
                df_0 = df_0.dropna()
                First_condition_Status = df_0.iloc[-1]["Delay_Status"]

                # Main Condition
                thredhold = float(select_method_df[select_method_df["Point_ID"]==point_id]["Threshold"].values[0])
                df_main = resample_data[[point_id]].copy()
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


    def get_test_event(self):
        Single_Rule_Event = self.test_event_SingleRule()
        Multi_Rule_Event = self.test_event_MultiRule()
        Act_DF = pd.concat([Single_Rule_Event["Activate"], Multi_Rule_Event["Activate"]], axis=0)
        Deact_DF = pd.concat([Single_Rule_Event["Deactivate"], Multi_Rule_Event["Deactivate"]], axis=0)
        return {"Activate": Act_DF, "Deactivate": Deact_DF}

# test = Test_module().get_test_event()
# test["Deactivate"]
# test["Activate"]