# import os
# import re
# import shutil
# import pandas as pd
# from datetime import datetime, timedelta
# from fastapi import FastAPI, File, UploadFile
# import psycopg2
# from dotenv import load_dotenv

# # Load environment variables from .env file
# load_dotenv()

# # Database credentials
# username_database = os.getenv('username_database')
# password = os.getenv('password')
# host = os.getenv('host')
# port = os.getenv('port')
# database = os.getenv('database')

# app = FastAPI()

# UPLOAD_DIR = "uploads"
# os.makedirs(UPLOAD_DIR, exist_ok=True)  # Ensure the upload directory exists

# @app.post("/upload/")
# async def upload_file(file: UploadFile = File(...)):
#     file_path = os.path.join(UPLOAD_DIR, file.filename)
    
#     # Save the uploaded file locally
#     with open(file_path, "wb") as buffer:
#         shutil.copyfileobj(file.file, buffer)

#     # Process raw data and insert into the first table
#     data = process_raw_data(file_path)
#     # Process the "gold" (aggregated) data and insert into the second table
#     gold_data(data)
    
#     return {"filename": file.filename, "message": "File processed successfully."}

# def process_raw_data(file_path: str) -> pd.DataFrame:
# # Read CSV file using the specified encoding
# data = pd.read_csv(file_path, encoding='ISO-8859-1')

# # Function to extract times and calculate total working time
# def calculate_total_working_time(punch_record):
#     # Extract times from the string
#     times = re.findall(r'\d{2}:\d{2}', punch_record)
    
#     # Initialize total working time
#     total_working_time = timedelta()
    
#     # Calculate durations
#     for i in range(0, len(times)-1, 2):
#         start_time = datetime.strptime(times[i], "%H:%M")
#         end_time = datetime.strptime(times[i+1], "%H:%M")
#         duration = end_time - start_time
#         total_working_time += duration

# # Calculate total hours and minutes
# total_seconds = total_working_time.total_seconds()
# hours = int(total_seconds // 3600)
# minutes = int((total_seconds % 3600) // 60)

# # Return total working time in "HH:MM" format
# return f"{hours:02d}:{minutes:02d}"

# data.rename(columns={' OutTime': 'OutTime'}, inplace=True)
# column_to_drop=["Employee Code","Duration"]
# data=data.drop(column_to_drop,axis=1)
# names_to_remove = ["Incuspaze Service", "ISCSE053","Incuspaze Mathew","ISCSE032","Director"]
# # Remove rows where "Employee Name" is in the names_to_remove list
# data = data[~data["Employee Name"].isin(names_to_remove)]
# #remove department 
# department_to_drop=["Default"]
# data = data[~data["Department"].isin(department_to_drop)]
# #To remove intime and outtime with 00:00 time
# data = data.loc[(data["InTime"].ne("0:00")) & (data["OutTime"].ne("0:00"))]
# # Convert the "Date" column to datetime without specifying the format
# data["Date"] = pd.to_datetime(data["AttendanceDate"], dayfirst=True)
# data["DayOfWeek"] = data["Date"].dt.day_name()
# data["MonthName"] = data["Date"].dt.month_name()
# data["Year"] = data["Date"].dt.year
# DayOfWeek_drop=["Sunday","Saturday"]
# data = data[~data["DayOfWeek"].isin(DayOfWeek_drop)] 
# status_drop=["Absent"]
# data = data[~data["Status"].isin(status_drop)] # Use dayfirst=True if the day is the first component
# data = data.sort_values(by="Date")
# data["InTimeFloat"] = data["InTime"].str.replace(":", ".").astype(float)

# # Compare if InTime is less than or equal to 12

# data=data[data["InTimeFloat"]<=11.59]

# data["OutTimeFloat"] = data["OutTime"].str.replace(":", ".").astype(float)
# data=data[data["OutTimeFloat"]>=13.30]

# def calculate_duration(intime, outtime):
# # Convert times to datetime objects
# time_format = "%H:%M"
# intime = datetime.strptime(intime, time_format)
# outtime = datetime.strptime(outtime, time_format)

# # Calculate the duration
# duration = outtime - intime
# # Convert duration to hours and minutes
# duration_seconds = duration.total_seconds()
# hours = int(duration_seconds // 3600)
# minutes = int((duration_seconds % 3600) // 60)

# # Return formatted duration as hours:minutes
# return f"{hours}:{minutes:02d}"

# # Apply the function to the DataFrame
# data["Duration_in_office"] = data.apply(lambda row: calculate_duration(row["InTime"], row["OutTime"]), axis=1)
# def drop_columns(data, columns_to_drop):
# for col in columns_to_drop:
#     if col in data.columns:
#         data.drop(col, axis=1, inplace=True)
#     else:
#         print(f"No column named '{col}' to drop")

# # Example usage:
# # Assuming 'data' is your DataFrame and you want to drop specific columns
# columns_to_drop = ["Date","InTimeFloats","Status","OutTimeFloat","InTimeFloat"] #"PunchRecords
# drop_columns(data, columns_to_drop)


# # Check and replace for Markapuram Venkata Raja
# if "Markapuram Venkata Raja" in data["Employee Name"].values:
# data.loc[data["Employee Name"] == "Markapuram Venkata Raja", "Department"] = \
# data.loc[data["Employee Name"] == "Markapuram Venkata Raja", "Department"].replace({"BD": "Dev"})

# # Check and replace for Sunkari Adithya
# if "Sunkari Adithya" in data["Employee Name"].values:
# data.loc[data["Employee Name"] == "Sunkari Adithya", "Department"] = \
# data.loc[data["Employee Name"] == "Sunkari Adithya", "Department"].replace({"BD": "Dev"})
# if "Srinivasa Selvaparhi Naidu Manapaka" in data["Employee Name"].values:
# data["Employee Name"] = data["Employee Name"].replace("Srinivasa Selvaparhi Naidu Manapaka", "Srinivasa Salvapathi Naidu Manapaka")


# data["TotalWorkingTime"] = data["PunchRecords"].apply(calculate_total_working_time)
# new_order = ['Employee Name', 'Department', 'InTime','OutTime','AttendanceDate',"DayOfWeek","MonthName","Year","Duration_in_office","TotalWorkingTime",'PunchRecords']
# # Reassign the DataFrame with the new column order
# data = data[new_order]

# df_csv = data.copy()#dapd.read_csv(csv_file_path)
# df_csv.drop(columns=["PunchRecords"], inplace=True)
# #Step 2: Rename columns to match database columns
# df_csv.rename(columns={
# 'employee_name': 'employee_name',
# 'Department': 'department',
# "InTime":"in_time",
# "OutTime":"out_time",
# "AttendanceDate":"attendance_date",
# "DayOfWeek" :"day_of_week",
# 'MonthName':'month_name',
# 'Year': 'year',
# 'Duration_in_office': 'duration_in_office ',
# 'TotalWorkingTime': 'total_working_time '
# }, inplace=True)

# # Construct the connection string
# database_url = f"postgresql://{username_database}:{password}@{host}:{port}/{database}"

# def connect_to_postgresql(database_url):
# try:
#     # Connect to the PostgreSQL database (CockroachDB is compatible with PostgreSQL)
#     conn = psycopg2.connect(database_url, sslmode='require')  # Add sslmode='require' for secure connection
#     print("The database is connected")
#     return conn
# except Exception as e:
#     print("Error connecting to PostgreSQL:", e)
#     return None

# # Call the function to establish the connection
# connection = connect_to_postgresql(database_url)

# # Step 3: Connect to your database
# if connection:
# try:
#     # Step 4: Create a cursor object
#     cursor = connection.cursor()

#     # Convert the DataFrame rows into a list of tuples
#     rows_to_insert = [tuple(row) for row in df_csv.values]

#     # Define your insert SQL query based on the renamed DataFrame
#     insert_query = """
#     INSERT INTO employee_attendance_daily  (employee_name,department,in_time,out_time,attendance_date,day_of_week,month_name,year, 
#                                     duration_in_office, total_working_time) 
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
#     """

#     # Step 5: Execute the batch insert
#     cursor.executemany(insert_query, rows_to_insert)
#     connection.commit()  # Commit the transaction
#     print(f"{cursor.rowcount} rows were inserted.")

# except Exception as e:
#     print("Error while inserting into database:", e)

# finally:
#     # Step 6: Close the cursor and connection
#     if connection:
#         cursor.close()
#         connection.close()
#         print("The connection is closed")
# return data

# def gold_data(data):
#     def time_to_seconds(t):
#         t = datetime.strptime(t, "%H:%M")
#         return t.hour * 3600 + t.minute * 60
#     # Apply the conversion to 'InTime' and 'OutTime' columns
#     data["INTimeInSeconds"] = data["InTime"].apply(time_to_seconds)
#     data["OUTTimeInSeconds"] = data["OutTime"].apply(time_to_seconds)
#     data["Duration_in_office"] = data["Duration_in_office"].apply(time_to_seconds)
#     data["TotalWorkingTime"] = data["TotalWorkingTime"].apply(time_to_seconds)
        
# # Calculate mean seconds for InTime and OutTime, including Year
#     mean_seconds_INTIME = data.groupby(["Employee Name", "Department", "MonthName", "Year"])["INTimeInSeconds"].mean()
#     mean_seconds_OUTTIME = data.groupby(["Employee Name", "Department", "MonthName", "Year"])["OUTTimeInSeconds"].mean()
#     mean_seconds_dur_office = data.groupby(["Employee Name", "Department", "MonthName", "Year"])["Duration_in_office"].mean()
#     mean_working_hours = data.groupby(["Employee Name", "Department", "MonthName", "Year"])["TotalWorkingTime"].mean()

#     # Convert mean seconds back to a time object
#     mean_in_times = mean_seconds_INTIME.apply(lambda x: (datetime.min + timedelta(seconds=x)).time())
#     mean_out_times = mean_seconds_OUTTIME.apply(lambda x: (datetime.min + timedelta(seconds=x)).time())
#     mean_dur_offie = mean_seconds_dur_office.apply(lambda x: (datetime.min + timedelta(seconds=x)).time())
#     mean_working_houres=mean_working_hours.apply(lambda x: (datetime.min + timedelta(seconds=x)).time())
#     # Convert the Series to DataFrames
#     mean_in_times_df = mean_in_times.reset_index()
#     mean_out_times_df = mean_out_times.reset_index() 
#     mean_dur_offie_df=mean_dur_offie.reset_index()
#     mean_working_houres_df=mean_working_houres.reset_index()
#     # Rename the columns
#     mean_in_times_df.columns = ["Employee Name", "Department","MonthName" ,"Year","Mean InTime"]
#     mean_out_times_df.columns = ["Employee Name", "Department", "MonthName","Year","Mean OutTime"]
#     mean_dur_offie_df.columns = ["Employee Name", "Department","MonthName","Year", "Duration INOffice"]
#     mean_working_houres_df.columns = ["Employee Name", "Department","MonthName","Year","Working houres Duration"]
#     # Format the 'Mean InTime' and 'Mean OutTime' columns to "HH:MM"
#     mean_in_times_df["Mean InTime"] = mean_in_times_df["Mean InTime"].apply(lambda t: t.strftime("%H:%M"))
#     mean_out_times_df["Mean OutTime"] = mean_out_times_df["Mean OutTime"].apply(lambda t: t.strftime("%H:%M"))
#     mean_dur_offie_df["Duration INOffice"] = mean_dur_offie_df["Duration INOffice"].apply(lambda t: t.strftime("%H:%M"))
#     mean_working_houres_df["Working houres Duration"] = mean_working_houres_df["Working houres Duration"].apply(lambda t: t.strftime("%H:%M"))
#     # Merge the mean in times and out times DataFrames
#     mean_times_df1 = pd.merge(mean_in_times_df, mean_out_times_df, on=["Employee Name", "Department","MonthName","Year"])
#     mean_times_df=pd.merge(mean_times_df1,mean_dur_offie_df,on=["Employee Name", "Department","MonthName","Year"])
#     mean_times_df=pd.merge(mean_times_df,mean_working_houres_df,on=["Employee Name", "Department","MonthName","Year"])
#     columns_to_drop=["TotalWorkingTime","INTimeInSeconds","OUTTimeInSeconds"]
#     data.drop(columns_to_drop,axis=1,inplace=True)
#     mean_times_df
#     df_csv=mean_times_df.copy()
#     df_csv.rename(columns={
#     'Employee Name': 'employee_name',
#     'Department': 'department',
#     'MonthName': 'month_name',
#     'Year': 'year',
#     'Mean InTime': 'mean_intime',
#     'Mean OutTime': 'mean_outtime',
#     'Duration INOffice': 'duration_in_office',
#     'Working houres Duration': 'working_hours_duration'
#     }, inplace=True)
#     # Construct the connection string
#     database_url = f"postgresql://{username_database}:{password}@{host}:{port}/{database}"

#     def connect_to_postgresql(database_url):
#         try:
#             # Connect to the PostgreSQL database (CockroachDB is compatible with PostgreSQL)
#             conn = psycopg2.connect(database_url, sslmode='require')  # Add sslmode='require' for secure connection
#             print("The database is connected you can access it")
#             return conn
#         except Exception as e:
#             print("Error connecting to PostgreSQL:", e)
#             return None

#     # Call the function to establish the connection
#     connection = connect_to_postgresql(database_url)

#     # Step 3: Connect to your database
#     if connection:
#         try:
#             # Step 4: Create a cursor object
#             cursor = connection.cursor()

#             # Convert the DataFrame rows into a list of tuples
#             rows_to_insert = [tuple(row) for row in df_csv.values]

#             # Define your insert SQL query based on the renamed DataFrame
#             insert_query = """
#             INSERT INTO employee_work_hours (employee_name, department, month_name, year, 
#                                             mean_intime, mean_outtime, duration_in_office, working_hours_duration) 
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#             """

#             # Step 5: Execute the batch insert
#             cursor.executemany(insert_query, rows_to_insert)
#             connection.commit()  # Commit the transaction
#             print(f"{cursor.rowcount} rows were inserted.")

#         except Exception as e:
#             print("Error while inserting into database:", e)

#         finally:
#             # Step 6: Close the cursor and connection
#             if connection:
#                 cursor.close()
#                 connection.close()
#                 print("The connection is closed")
import os
import re
import shutil
import pandas as pd
from datetime import datetime, timedelta
from fastapi import FastAPI, File, UploadFile
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database credentials
username_database = os.getenv('username_database')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')
database = os.getenv('database')

app = FastAPI()

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)  # Ensure the upload directory exists

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    
    # Save the uploaded file locally
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Process raw data and insert into the first table
    data = process_raw_data(file_path)
    # Process the "gold" (aggregated) data and insert into the second table
    gold_data(data)
    
    return {"filename": file.filename, "message": "File processed successfully."}

def process_raw_data(file_path: str) -> pd.DataFrame:
    # Read CSV file using the specified encoding
    data = pd.read_csv(file_path, encoding='ISO-8859-1')

    # Function to extract times from a punch record and calculate total working time
    def calculate_total_working_time(punch_record: str) -> str:
        # Extract times in HH:MM format from the punch record
        times = re.findall(r'\d{2}:\d{2}', punch_record)
        total_working_time = timedelta()
        # Loop over the times in pairs
        for i in range(0, len(times) - 1, 2):
            start_time = datetime.strptime(times[i], "%H:%M")
            end_time = datetime.strptime(times[i+1], "%H:%M")
            duration = end_time - start_time
            total_working_time += duration
        # Convert the total working time to hours and minutes
        total_seconds = total_working_time.total_seconds()
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        return f"{hours:02d}:{minutes:02d}"

    # Clean up column names and drop unwanted columns
    data.rename(columns={' OutTime': 'OutTime'}, inplace=True)
    columns_to_drop = ["Employee Code", "Duration"]
    data = data.drop(columns=columns_to_drop, axis=1)
    
    # Remove rows based on employee names
    names_to_remove = ["Incuspaze Service", "ISCSE053", "Incuspaze Mathew", "ISCSE032", "Director"]
    data = data[~data["Employee Name"].isin(names_to_remove)]
    
    # Remove unwanted departments
    department_to_drop = ["Default"]
    data = data[~data["Department"].isin(department_to_drop)]
    
    # Remove records with 00:00 in InTime and OutTime
    data = data.loc[(data["InTime"] != "0:00") & (data["OutTime"] != "0:00")]
    
    # Convert attendance date and extract additional date information
    data["Date"] = pd.to_datetime(data["AttendanceDate"], dayfirst=True)
    data["DayOfWeek"] = data["Date"].dt.day_name()
    data["MonthName"] = data["Date"].dt.month_name()
    data["Year"] = data["Date"].dt.year
    
    # Remove weekends and absent statuses
    weekends = ["Sunday", "Saturday"]
    data = data[~data["DayOfWeek"].isin(weekends)]
    data = data[data["Status"] != "Absent"]
    
    data = data.sort_values(by="Date")
    
    # Convert InTime and OutTime to float representations for filtering
    data["InTimeFloat"] = data["InTime"].str.replace(":", ".").astype(float)
    data = data[data["InTimeFloat"] <= 11.59]
    
    data["OutTimeFloat"] = data["OutTime"].str.replace(":", ".").astype(float)
    data = data[data["OutTimeFloat"] >= 13.30]
    
    # Function to calculate duration between in and out times
    def calculate_duration(intime: str, outtime: str) -> str:
        time_format = "%H:%M"
        intime_dt = datetime.strptime(intime, time_format)
        outtime_dt = datetime.strptime(outtime, time_format)
        duration = outtime_dt - intime_dt
        duration_seconds = duration.total_seconds()
        hours = int(duration_seconds // 3600)
        minutes = int((duration_seconds % 3600) // 60)
        return f"{hours}:{minutes:02d}"
    
    data["Duration_in_office"] = data.apply(
        lambda row: calculate_duration(row["InTime"], row["OutTime"]), axis=1
    )
    
    # Drop extra columns that are no longer needed
    def drop_columns(df: pd.DataFrame, cols: list):
        for col in cols:
            if col in df.columns:
                df.drop(col, axis=1, inplace=True)
            else:
                print(f"Column '{col}' not found to drop.")
    
    drop_columns(data, ["Date", "InTimeFloats", "Status", "OutTimeFloat", "InTimeFloat"])
    
    # Make corrections for specific employee names and departments
    if "Markapuram Venkata Raja" in data["Employee Name"].values:
        data.loc[data["Employee Name"] == "Markapuram Venkata Raja", "Department"] = \
            data.loc[data["Employee Name"] == "Markapuram Venkata Raja", "Department"].replace({"BD": "Dev"})
    
    if "Sunkari Adithya" in data["Employee Name"].values:
        data.loc[data["Employee Name"] == "Sunkari Adithya", "Department"] = \
            data.loc[data["Employee Name"] == "Sunkari Adithya", "Department"].replace({"BD": "Dev"})
    
    if "Srinivasa Selvaparhi Naidu Manapaka" in data["Employee Name"].values:
        data["Employee Name"] = data["Employee Name"].replace(
            "Srinivasa Selvaparhi Naidu Manapaka", "Srinivasa Salvapathi Naidu Manapaka"
        )
    
    # Calculate total working time from punch records
    data["TotalWorkingTime"] = data["PunchRecords"].apply(calculate_total_working_time)
    
    # Reorder the columns as needed
    new_order = [
        'Employee Name', 'Department', 'InTime', 'OutTime', 'AttendanceDate',
        "DayOfWeek", "MonthName", "Year", "Duration_in_office", "TotalWorkingTime", 'PunchRecords'
    ]
    data = data[new_order]
    
    # Prepare a DataFrame for insertion into the employee_attendance_daily table
    df_csv = data.copy()
    df_csv.drop(columns=["PunchRecords"], inplace=True)
    
    # Rename columns to match database table columns (without extra spaces)
    df_csv.rename(columns={
        "Employee Name": "employee_name",
        "Department": "department",
        "InTime": "in_time",
        "OutTime": "out_time",
        "AttendanceDate": "attendance_date",
        "DayOfWeek": "day_of_week",
        "MonthName": "month_name",
        "Year": "year",
        "Duration_in_office": "duration_in_office",
        "TotalWorkingTime": "total_working_time"
    }, inplace=True)
    
    # Insert df_csv into the database table employee_attendance_daily
    database_url = f"postgresql://{username_database}:{password}@{host}:{port}/{database}"
    connection = connect_to_postgresql(database_url)
    
    if connection:
        try:
            cursor = connection.cursor()
            rows_to_insert = [tuple(row) for row in df_csv.values]
            insert_query = """
                INSERT INTO employee_attendance_daily 
                (employee_name, department, in_time, out_time, attendance_date, day_of_week, month_name, year, 
                 duration_in_office, total_working_time) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, rows_to_insert)
            connection.commit()
            print(f"{cursor.rowcount} rows were inserted into employee_attendance_daily.")
        except Exception as e:
            print("Error while inserting into database:", e)
        finally:
            cursor.close()
            connection.close()
            print("The connection to employee_attendance_daily is closed.")
    else:
        print("Connection to the database could not be established.")
    
    return data

def connect_to_postgresql(database_url: str):
    try:
        conn = psycopg2.connect(database_url, sslmode='require')
        print("The database is connected")
        return conn
    except Exception as e:
        print("Error connecting to PostgreSQL:", e)
        return None

def gold_data(data: pd.DataFrame):
    # Helper function to convert time string "HH:MM" to total seconds
    def time_to_seconds(t: str) -> int:
        t_obj = datetime.strptime(t, "%H:%M")
        return t_obj.hour * 3600 + t_obj.minute * 60

    # Convert time columns to seconds
    data["INTimeInSeconds"] = data["InTime"].apply(time_to_seconds)
    data["OUTTimeInSeconds"] = data["OutTime"].apply(time_to_seconds)
    data["Duration_in_office_seconds"] = data["Duration_in_office"].apply(time_to_seconds)
    data["TotalWorkingTime_seconds"] = data["TotalWorkingTime"].apply(time_to_seconds)
    
    # Calculate mean values grouped by Employee Name, Department, MonthName, and Year
    group_cols = ["Employee Name", "Department", "MonthName", "Year"]
    mean_intime_sec = data.groupby(group_cols)["INTimeInSeconds"].mean()
    mean_outtime_sec = data.groupby(group_cols)["OUTTimeInSeconds"].mean()
    mean_duration_office_sec = data.groupby(group_cols)["Duration_in_office_seconds"].mean()
    mean_total_working_sec = data.groupby(group_cols)["TotalWorkingTime_seconds"].mean()
    
    # Convert mean seconds back to time strings in "HH:MM" format
    mean_intime = mean_intime_sec.apply(lambda x: (datetime.min + timedelta(seconds=x)).strftime("%H:%M"))
    mean_outtime = mean_outtime_sec.apply(lambda x: (datetime.min + timedelta(seconds=x)).strftime("%H:%M"))
    mean_duration_office = mean_duration_office_sec.apply(lambda x: (datetime.min + timedelta(seconds=x)).strftime("%H:%M"))
    mean_total_working = mean_total_working_sec.apply(lambda x: (datetime.min + timedelta(seconds=x)).strftime("%H:%M"))
    
    # Reset indices to convert Series to DataFrame
    mean_intime_df = mean_intime.reset_index().rename(columns={0: "mean_intime"})
    mean_outtime_df = mean_outtime.reset_index().rename(columns={0: "mean_outtime"})
    mean_duration_office_df = mean_duration_office.reset_index().rename(columns={0: "duration_in_office"})
    mean_total_working_df = mean_total_working.reset_index().rename(columns={0: "working_hours_duration"})
    
    # Merge the mean DataFrames together
    mean_times_df = pd.merge(mean_intime_df, mean_outtime_df, on=group_cols)
    mean_times_df = pd.merge(mean_times_df, mean_duration_office_df, on=group_cols)
    mean_times_df = pd.merge(mean_times_df, mean_total_working_df, on=group_cols)
    
    # Drop intermediate columns from the original DataFrame if desired
    data.drop(
        columns=["TotalWorkingTime_seconds", "INTimeInSeconds", "OUTTimeInSeconds", "Duration_in_office_seconds"],
        inplace=True
    )
    
    # Prepare the DataFrame for insertion by renaming columns to match the database table
    df_csv = mean_times_df.copy()
    df_csv.rename(columns={
        "Employee Name": "employee_name",
        "Department": "department",
        "MonthName": "month_name",
        "Year": "year",
        "mean_intime": "mean_intime",
        "mean_outtime": "mean_outtime",
        "duration_in_office": "duration_in_office",
        "working_hours_duration": "working_hours_duration"
    }, inplace=True)
    
    # Insert the aggregated data into the employee_work_hours table
    database_url = f"postgresql://{username_database}:{password}@{host}:{port}/{database}"
    connection = connect_to_postgresql(database_url)
    
    if connection:
        try:
            cursor = connection.cursor()
            rows_to_insert = [tuple(row) for row in df_csv.values]
            insert_query = """
                INSERT INTO employee_work_hours 
                (employee_name, department, month_name, year, mean_intime, mean_outtime, duration_in_office, working_hours_duration)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, rows_to_insert)
            connection.commit()
            print(f"{cursor.rowcount} rows were inserted into employee_work_hours.")
        except Exception as e:
            print("Error while inserting into employee_work_hours:", e)
        finally:
            cursor.close()
            connection.close()
            print("The connection to employee_work_hours is closed.")
    else:
        print("Connection to the database could not be established for employee_work_hours.")

# The code now ends here.
