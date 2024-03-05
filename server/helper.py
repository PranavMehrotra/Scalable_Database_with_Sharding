import os
import numpy as np
import pandas as pd
import mysql.connector
from multiprocessing.dummy import Pool

class SQLHandler:
    def __init__(self,host='localhost',user='root',password='saransh03sharma',db='server_database'):
        self.jobrunner=Pool(1)
        self.host=host
        self.user=user
        self.password=password
        self.db=db

    def connect(self):
        connected=False
        while not connected:
            try:
                self.mydb = mysql.connector.connect(host=self.host,user=self.user,password=self.password)
                self.Use_database(self.db)
                connected=True
                print("MYSQL server connected.")
            except Exception:
                pass
        return "Connected", 200
    
    def disconnect(self):
        connected = True
        while connected:
            try:
                if self.mydb.is_connected():
                    self.mydb.close()
                    print("MYSQL server closed.")
                    connected = False
            except Exception as e:
                print("Error while disconnecting:", e)
                pass
        return "Disconnected", 200
    
    def query(self, sql):
        try:
            cursor = self.mydb.cursor()
            cursor.execute(sql)
        except Exception:
            self.connect()
            cursor = self.mydb.cursor()
            cursor.execute(sql)
        res=cursor.fetchall()
        cursor.close()
        self.mydb.commit()
        return res

    def Use_database(self,dbname='server_database'):
        try:
            res=self.query("SHOW DATABASES")
            if dbname not in [r[0] for r in res]:
                self.query(f"CREATE DATABASE {dbname}")
            self.query(f"USE {dbname}")
            return dbname,200
        except Exception as e:
            return e, 500

    def Drop_database(self,dbname='server_database'):
        try:
            res=self.query("SHOW DATABASES")
            if dbname in [r[0] for r in res]:
                self.query(f"DROP DATABASE {dbname}")
            return "Successfully dropped database",200
        except Exception as e:
            return e, 500

    def Create_table(self, tabname, columns, dtypes):
        try:
            # Check if the table already exists
            res = self.query("SHOW TABLES")
            if tabname not in [r[0] for r in res]:
                # Map data types
                dmap = {'Number': 'INT', 'String': 'VARCHAR(32)'}

                # Construct column configuration
                col_config = ', '.join([f"{c} {dmap[d]}" for c, d in zip(columns, dtypes)])

                # Execute SQL query to create the table
                self.query(f"CREATE TABLE {tabname} (id INT AUTO_INCREMENT PRIMARY KEY, {col_config})")

            return tabname,200
        except Exception as e:
            return e, 500



    def Get_range(self,table_name,low, high, col):
        try:
            # Execute SQL query to retrieve entries within the specified range
            rows = self.query(f"SELECT {col} FROM {table_name} WHERE {col}>={low} AND {col}<{high}")

            # Check if any rows were returned
            if len(rows) == 0:
                raise KeyError(f"No entries found where {col} is in the range [{low}, {high})")
            else:
                # Return the list of values from the query result
                return [row[0] for row in rows],200
        except Exception as e:
            return e, 500


    def Delete_entry(self,table_name,idx,col):
        try:
            res = self.query(f"DELETE FROM {table_name} WHERE {col}={idx}")
            if res.rowcount > 0:
                return "Entry deleted successfully",200
            else:
                return "No matching entry found for deletion",404
        except Exception as e:
            return e,500


    def Update_database(self,table_name,Stud_id,updated_val):
        try:
            # Iterate through the key-value pairs of the dictionary
            update_queries = []
            for col, val in updated_val.items():
                # Check the type of the value
                if isinstance(val, str):
                    # For string values, enclose in single quotes
                    update_queries.append(f"{col}='{val}'")
                else:
                    update_queries.append(f"{col}={val}")

            # Join the update queries into a single string
            update_query_str = ', '.join(update_queries)

            # Execute the update query
            res = self.query(f"UPDATE {table_name} SET {update_query_str} WHERE Stud_id={Stud_id}")

            # Check if any row was affected (updated)
            if res.rowcount > 0:
                return "Entry updated successfully",200
            else:
                return "No matching entry found for update",404

        except Exception as e:
            return e,500

    def Insert(self,table_name,row):
    
        try:
            row_str = '0'
            
            for v in row:
                if isinstance(v, str):
                    row_str += f", '{v}'"
                else:
                    v_reduced = 'NULL' if np.isnan(v) else v
                    row_str += f", {v_reduced}"
            
            res = self.query(f"INSERT INTO {table_name} VALUES ({row_str})")
            
            return "Successfully inserted {row}", 200
        except Exception as e:
            return e, 500

