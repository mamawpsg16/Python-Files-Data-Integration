import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import pandas as pd
from datetime import datetime
import shutil
import json
import mysql.connector
from mysql.connector import errorcode

downloads_dir = 'C:\\Users\\kevin.mensah\\Downloads\\FilesForExtraction' #DIRECTORY TO WATCH FOR CHANGE
extracted_dir = 'C:\\Users\\kevin.mensah\\Downloads\\ExtractedData' #DIRECTORY TO TRANSFER
# WATCH ALL EXCEL/CSV IN A FOLDER
# GET SOME DETAILS IN THE FILE THEN MOVE IN TO A FOLDER IF FOLDER DOESNT EXIST THEN PUT READ AND WRITE PERMISSION IN THE CREATED FOLDER
# CONVERT FILE DATA INTO THE FILE TYPE

class Product():
    def __init__(self) -> None:
        with open('config.json', 'r') as config_file:
            self.config_data = json.load(config_file)
        self.connection = None
        self.database_name = 'python_crud' 
        self.table_name    = 'items' 
    
    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                **self.config_data['database']
            )
        except mysql.connector.Error as err:
            # Handle connection errors
            print(f"Error connecting to the database: {err}")
    def disconnect(self):
        if self.connection:
            self.connection.close()
            
    def executeQuery(self, query, data = None, method = "get"):
        self.config_data['database']['database'] = self.database_name
        cursor = None
        try:
             # Create a cursor object to interact with the database
            self.connect()
            cursor = self.connection.cursor(dictionary=True)
            if method != "get" :
                if len(data) > 1:
                    cursor.executemany(query, data)
                    self.connection.commit()
                elif method == 'delete':
                    print("DELETE", data)
                    cursor.execute(query, data)
                    self.connection.commit()
                else:
                    cursor.execute(query, data[0])
                    self.connection.commit()
            else:
                cursor.execute(query)
                return  cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"Error connecting to the database: {err}")
            return None
        finally:
            if cursor:
                cursor.close()
            self.disconnect()

    def all(self):
        # Create a connection to the MySQL database
        return self.executeQuery(f"SELECT * FROM {self.table_name}")
    
    def store(self, data = None):
        # query = ("INSERT INTO event_items "
        #     "(event_id, name, description, quantity, chance_rate, `order`, color)"
        #     "VALUES (%(event_id)s, %(name)s, %(description)s, %(quantity)s, %(chance_rate)s, %(order)s, %(color)s)")
        query = (f"INSERT INTO `{self.table_name}`"
            "(name, description, quantity)"
            " VALUES (%s, %s, %s)"
            " ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), quantity = VALUES(quantity)"
            )
        self.executeQuery(query, data, 'post')

    def destroy(self, item_name):
        query = f"DELETE FROM {self.table_name} WHERE name = %(name)s"
        self.executeQuery(query, {"name":item_name}, 'delete')
    
    def createTable(self):
        cursor = None
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS `{self.table_name}` ("
                    # "`id` int(11) NOT NULL AUTO_INCREMENT,"
                    "`name` varchar(100) NOT NULL,"
                    "`description` varchar(100) DEFAULT NULL,"
                    "`quantity` int(11) NOT NULL,"
                    "`status` int(11) DEFAULT 1,"
                    "`created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                    "`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    "PRIMARY KEY (`name`)"
                    # "KEY `name` (`name`)"
                    ") ENGINE=InnoDB")
        # print(f"Table event_items created successfully.")
        if cursor:
            cursor.close()
        self.disconnect()

    def createDatabase(self):
        cursor = None
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
        # print(f"Database python_crud created successfully.")
        self.config_data['database']['database'] = self.database_name
        self.createTable()
        if cursor:
            cursor.close()
        self.disconnect()

   

def scanDirectory():
    if not os.path.exists(extracted_dir):
        os.makedirs(extracted_dir)

    with os.scandir(downloads_dir) as entries:
        for entry in entries:
            event = Product()
            # file_path = f"{extracted_dir}\\{entry.name}"
            if entry.name.endswith(".csv"):
                df_excel = pd.read_csv(entry)

                # Copy the original file to a new file
                # shutil.copy2(entry, extracted_dir)

                # # Read the copied Excel file into a DataFrame
                df_excel = pd.read_csv(entry, index_col=None)
                df_excel.fillna(value={"description": ""}, inplace=True)

                # # Extract specific columns
                # data = df_excel[["name", "country"]]
                # data.to_dict(file_path, index = False)
                print(df_excel, "df_excel")
                data = df_excel.to_records(index = False)
                print(data, "data")
                event.store(data.tolist())

            if entry.name.endswith(".xlsx"):
                df_excel = pd.read_excel(entry)
        
                # file_path = f"{extracted_dir}\\{entry.name}"
                # Copy the original file to a new file
                # shutil.copy2(entry, extracted_dir)

                # Read the copied Excel file into a DataFrame
                df_excel = pd.read_excel(entry, index_col=None)
                df_excel.fillna(value={"description": ""}, inplace=True)
                
                # Extract specific columns
                # data = df_excel[["name", "country"]]
                # print(file_path, "file_path")
                #EXPORT
                # data.to_excel(entry,sheet_name = 'Sheet', index = False)

                data = df_excel.to_records(index = False)
                event.store(data.tolist())

class Watcher:
    def __init__(self, directory=".", handler=FileSystemEventHandler()):
        self.observer = Observer()
        self.handler = handler
        self.directory = directory

    def run(self):
        self.observer.schedule(self.handler, self.directory, recursive=True)
        self.observer.start()
        # print("\nWatcher Running in {}/\n".format(self.directory))
        print(f"Watcher Running in {self.directory}/")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()
        print("\nWatcher Terminated\n")


class MyHandler(FileSystemEventHandler):
    def on_modified(self, event):
        scanDirectory()

     
        
if __name__=="__main__":
    product = Product() 
    # product.createDatabase() # ONE TIME  THEN COMMENT
    # product.createTable() # ONE TIME  THEN COMMENT
    # print(product.all(),"ALL")
    # product.destroy()
    # scanDirectory()
    w = Watcher(downloads_dir, MyHandler())
    # w.run()

  


