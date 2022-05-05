# %% Import packages
import pandas as pd 
import pymssql
import datetime
import os


BATH_PATH = os.path.dirname(os.path.abspath(__file__))
ETC_PATH = os.path.join(BATH_PATH, "..", "etc")
DATA_PATH = os.path.join(BATH_PATH, "..", "data")
BA_Point_filename = [f for f in os.listdir(ETC_PATH) if ("BA_Point_List" in f) & (f.endswith(".xlsx"))]
BA_Point_Table = BA_Point_filename[0]
Alarm_rule_filename = [f for f in os.listdir(ETC_PATH) if ("Conditions" in f) & (f.endswith(".xlsx"))]
Alarm_rule_Table = Alarm_rule_filename[0]
print(F"Point Table: {BA_Point_Table}") # If there is more than one file of Point table, please check the file name and contents.
print(F"Alarm_rule Table: {Alarm_rule_Table}") # If there is more than one file of Point table, please check the file name and contents.


# Parameters
##################################################
# MSSQL
MSSQL_IP = "125.229.80.51:51433"
MSSQL_user = "EcoFirst_admin"
# MSSQL_IP = "125.229.80.51:51487"
# MSSQL_user = "SA"
MSSQL_password = "1qaz@WSX5487"
MSSQL_database = "TaipeiPost"
MSSQL_table_BA = "dbo.BA_point_Data"
MSSQL_point_table = "dbo.BA_point_Table"
MSSQL_Alarm_rule_table = "dbo.Alarm_Rule_Table"
##################################################


# %%
# Function of error recording
def error_log(error_txt):
    txt = {"Error type": error_txt, "Time": datetime.datetime.now().isoformat()}
    if "error_log.txt" not in os.listdir(ETC_PATH):
        f = open(os.path.join(ETC_PATH, "error_log.txt"), "w")
        f.write(F"{txt}\n")
        f.close()
    else:
        f = open(os.path.join(ETC_PATH, "error_log.txt"), "a")
        f.write(F"{txt}\n")
        f.close() 


# %%
txt = '延時'
def string_to_list(string, mode):
    items = []
    item = ""
    itemExpected = True
    for char in string[1:]:
        if itemExpected and char not in ["]", ",", "["]:
            item += char
            print(0)
        elif char in [",", "[", "]"]:
            itemExpected = True
            items.append(item)
            item = ""
    newItems = []
    try:
        if mode == "int":
            for i in items:
                newItems.append(int(i))
        elif mode == "float":
            for i in items:
                newItems.append(float(i))
        elif mode == "boolean":
            for i in items:
                if i in ["true", "True"]:
                    newItems.append(True)
                elif i in ["false", "False"]:
                    newItems.append(False)
                else:
                    newItems.append(None)
        elif mode == "string":
            return items
        else:
            raise Exception("the 'mode'/second parameter of string_to_list() must be one of: 'int', 'string', 'bool', or 'float'")
        return newItems
    except:
        print("Error")
# string_to_list(txt, "string")


# %%
def create_Alarm_rule_mssql():
    conn = pymssql.connect(
        server=MSSQL_IP, 
        user=MSSQL_user, password=MSSQL_password, 
        database=MSSQL_database)  # Connect to DB file
    cursor = conn.cursor() # Create cursor 
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'") 
    table_list = [i[0] for i in cursor.fetchall()]
    cursor.close()
    conn.close()

    if MSSQL_Alarm_rule_table.split(".")[1] not in table_list:
        conn = pymssql.connect(
            server=MSSQL_IP, 
            user=MSSQL_user, password=MSSQL_password, 
            database=MSSQL_database)  # Connect to DB file
        cursor = conn.cursor() # Create cursor  
        sql_txt = F"""
        CREATE TABLE {MSSQL_Alarm_rule_table} (
        Event_number int NOT NULL,
        Data_Name nvarchar(50) NOT NULL,
        Method nvarchar(50) NOT NULL,
        Threshold decimal(18, 2) NOT NULL,
        Time_period_minutes decimal(18, 2) NOT NULL,
        Event_Description nvarchar(50) NOT NULL,
        Event_Trigger nvarchar(50) NOT NULL,
        Event_Dismiss nvarchar(50) NOT NULL,
        Advice nvarchar(50) NOT NULL,
        Memo nvarchar(50) NULL,
        Related_Point_list nvarchar(50) NULL,
        Related_Method_list nvarchar(50) NULL,
        Related_Threshold_list nvarchar(50) NULL);
        """   
        cursor.execute(sql_txt) 
        conn.commit()
        cursor.close()
        conn.close()
        print(F"{MSSQL_Alarm_rule_table} created!")
    else:
        print(F"{MSSQL_Alarm_rule_table} already exited!")
        pass
# create_Alarm_rule_mssql()


# %%
# Update point Table
def update_Alarm_rule_table():
    # Check table
    try:
        create_Alarm_rule_mssql()
    except:
        pass

    # Check Data
    table = MSSQL_Alarm_rule_table
    conn = pymssql.connect(
        server=MSSQL_IP, 
        user=MSSQL_user, password=MSSQL_password, 
        database=MSSQL_database)    
    cur = conn.cursor(as_dict=True)
    sql = F"SELECT * FROM {table}"
    cur.execute(sql)
    data = cur.fetchall()
    cur.close()
    conn.close()
    DF = pd.DataFrame(data)

    # Local Data
    select_col = ["Event_number", "Point_ID", "Method", "Threshold", "Time_period_minutes", "Event_Description", "Event_Trigger", "Event_Dismiss", "Advice", "Memo", "Related_Point_list", "Related_Method_list", "Related_Threshold_list"]
    local_DF = pd.read_excel(os.path.join(ETC_PATH, Alarm_rule_Table), sheet_name="Alarm_list")
    local_DF = local_DF[select_col]
    local_DF = local_DF.astype(object).where(pd.notnull(local_DF), None)
    # return DF
    if len(DF) == 0:
        wildcards = ",".join(["%s"] * len(local_DF.columns)) 
        data = [tuple(x) for x in local_DF.values]
        conn = pymssql.connect(server=MSSQL_IP, user=MSSQL_user, password=MSSQL_password, database=MSSQL_database)    
        cur = conn.cursor()
        cur.executemany(F"INSERT INTO {table} VALUES({wildcards})", data)
        conn.commit()
        cur.close()
        conn.close()
        print(F"{table} data uploaded!")
    else:
        update_df = pd.concat([DF, local_DF]).drop_duplicates(keep=False)
        if len(update_df) == 0:
            print(F"No {table} data updated!")
        else:
            wildcards = ",".join(["%s"] * len(update_df.columns)) 
            data = [tuple(x) for x in local_DF.values]
            conn = pymssql.connect(server=MSSQL_IP, user=MSSQL_user, password=MSSQL_password, database=MSSQL_database)    
            cur = conn.cursor()
            cur.executemany(F"INSERT INTO {table} VALUES({wildcards})", data)
            conn.commit()
            cur.close()
            conn.close()
            print(F"{table} data updated!")
# update_Alarm_rule_table()


# %% main process
if __name__ == "__main__":
    update_Alarm_rule_table()


# %%
