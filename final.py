import pandas as pd
import mysql.connector
import os
import numpy as np

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    database="data_task"
)
mycursor = mydb.cursor()

def list_folders(path):
    folders = []
    for entry in os.listdir(path):
        full_path = os.path.join(path, entry)
        if os.path.isdir(full_path):
            folders.append(entry)
    return folders

def list_csv_files(folder_path):
    csv_files = []
    for entry in os.listdir(folder_path):
        full_path = os.path.join(folder_path, entry)
        if os.path.isdir(full_path):
            csv_files.extend(list_csv_files(full_path))
        elif entry.lower().endswith('.csv'):
            csv_files.append(entry)
    return csv_files

# Getting all folders inside data folder
folder_path = r'C:\Users\navee\Downloads\TaskData'
folder_list = list_folders(folder_path)

print(folder_list)

temp_folder_path = folder_path

total_entries=0

with open('WholeDataLogs.txt', 'w') as log_file:

	for folder in folder_list:
			
		folder_path = temp_folder_path	

		folder_path = os.path.join(folder_path, folder)
		print("\nFolder Path:", folder_path, file=log_file)
		log_file.flush()

		csv_files_list = list_csv_files(folder_path)  # All CSV files inside a folder in a list

		for csv_file in csv_files_list:

			file_path = os.path.join(folder_path, csv_file)
			print("\nProcessing file:", file_path, file=log_file)
			log_file.flush()

			# Read the CSV file into a DataFrame
			df = pd.read_csv(file_path)

			if df.shape[1] > 17:
				df = df.iloc[:,:17]
							
			# converting type to date of createion date column
			df['Creation date'] = pd.to_datetime(df['Creation date'])
			
			df=df.fillna(np.nan).replace([np.nan], [None])


			# Check if "charge_invoice_no" column exists in the DataFrame, if not add it with null values
			if "Charge Invoice #" not in df.columns:
				print("Creating column charge invoice #" , file=log_file)
				log_file.flush()
				df.insert(8, "Charge Invoice #", np.nan)
				
						
			column_names_list = df.columns.tolist()
			# print("Column Names List:", column_names_list)

			# Convert all columns (except "Financial charge") to string
			for col_name in column_names_list:
				if col_name != "Financial charge":
					df[col_name].replace(np.nan, None, inplace=True)
					df[col_name] = df[col_name].astype(str)

			rows_data = []
			for index, row in df.iterrows():
				rows_data.append( tuple( row.tolist() ) )


			query = """
								INSERT INTO wholedata (vendor, creation_date, status, notes, issue_type, issue_id, infraction_subtype_code,
												reversal_invoice_no, charge_invoice_no , financial_charge, vendor_code, fulfillment_center, ASIN,
												product_name, store, purchase_order_no, Sub_type_of_the_non_compliance)
								VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
							"""

			mycursor.executemany(query, rows_data)
			mydb.commit()

			total_entries = total_entries + mycursor.rowcount 
			print(mycursor.rowcount, "was inserted. Total entries" ,total_entries , file=log_file )
			log_file.flush()