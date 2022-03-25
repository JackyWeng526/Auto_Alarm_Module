#%% Import pakages
import pandas as pd
import numpy as np
import datetime
import time
import pymssql
import os


# %% Setup the python scripts and file paths
BATH_PATH = os.path.dirname(os.path.abspath(__file__))
ETC_PATH = os.path.join(BATH_PATH, "..", "etc")
DATA_PATH = os.path.join(BATH_PATH, "..", "data")
BA_Point_filename = [f for f in os.listdir(ETC_PATH) if ("BA_Point_List" in f) & (f.endswith(".xlsx"))]
BA_Point_Table = BA_Point_filename[0]


########################################################################################################
MSSQL_IP = "122.116.215.201:51433"
MSSQL_user = "EcoFirst_admin"
MSSQL_password = "1qaz@WSX5487"
MSSQL_database = "TaipeiPost"
MSSQL_table = "dbo.BA_point_Data"
# MSSQL_table = "dbo.raw_data"
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


get_TCP_point(["CHL400-I_Pump%", "CHL400-ITD"])
# %%
# Collect BA Data from MS SQL **Note: It's better to execute cursor directly with MSSQL rather than use pd.read_sql
# def get_point_data(points, conditions):













# %%
start_time = datetime.datetime(2022, 3, 22)
end_time = datetime.datetime(2022, 3, 23)

res = datetime.timedelta(minutes=1).total_seconds()
end = int(datetime.datetime.now().timestamp() / res) * res
show = datetime.datetime.fromtimestamp(end).isoformat().replace("T"," ")
end = datetime.datetime.fromtimestamp(end) - datetime.timedelta(minutes=1) 
start = end - datetime.timedelta(minutes=1) 
# start, end


select_index = "Time"
table = MSSQL_table
conn = pymssql.connect(
    server=MSSQL_IP, 
    user=MSSQL_user, 
    password=MSSQL_password, 
    database=MSSQL_database)   
cur = conn.cursor()

# sql_latest_time = F"SELECT MAX({select_index}) FROM {table}"
# cur.execute(sql_latest_time)
# data_time=cur.fetchall()
# conn.close()

# latest_time = data_time[0][0]
# latest_time_2 = latest_time - datetime.timedelta(minutes=1)
# sql = F"SELECT * FROM {table} WHERE {select_index} BETWEEN '{latest_time_2.isoformat(' ', 'seconds')}' AND '{latest_time.isoformat(' ', 'seconds')}'"
sql = F"SELECT * FROM {table} WHERE {select_index} BETWEEN '{start}' AND '{end}'"
cur.execute(sql)
data=cur.fetchall()
conn.close()
pd.DataFrame(data)



# %%
if __name__=="__main__":
  time.sleep(2)

# %%
