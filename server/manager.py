from helper import SQLHandler
import json

class Manager:
    def __init__(self,host='localhost',user='root',password='saransh03sharma',db='server_database'):
        self.sql_handler=SQLHandler(host='localhost',user='root',password='saransh03sharma',db='server_database')
        
        
    def Config_database(self,config):
        try:
            self.sql_handler.connect()
            if isinstance(config, str):
            # If config is already a string, no need to load it again
                schema = json.loads(config)
            # Now schema is a dictionary containing the parsed JSON data
            elif isinstance(config, dict):
                # If config is a dictionary, no need to load it, just use it directly
                schema = config
                # Now schema is a

            if 'schema' in schema and 'columns' in schema['schema'] and 'dtypes' in schema['schema'] and 'shards' in schema:
                columns = schema['schema']['columns']
                dtypes = schema['schema']['dtypes']
                shards = schema['shards']

                for shard in shards:
                    message, status = self.sql_handler.Create_table(shard, columns, dtypes)
                self.sql_handler.disconnect()
                if status == 200:
                    return "Tables created successfully", 200
                else: return message, status
            else:
                self.sql_handler.disconnect()
                return "Invalid JSON format", 400
        except Exception as e:
            self.sql_handler.disconnect()
            return e, 500
    
    def Copy_database(self,json_data):
        
        try:
            self.sql_handler.connect()
            if isinstance(json_data, str):
            # If json_data is already a string, no need to load it again
                schema = json.loads(json_data)
            # Now schema is a dictionary containing the parsed JSON data
            elif isinstance(json_data, dict):
                # If json_data is a dictionary, no need to load it, just use it directly
                schema = json_data
            
            database_copy = {}
            for table_name in schema['shards']:   
                
                # Read all entries of the current table
                table_rows,status = self.sql_handler.Get_table_rows(table_name)
                
                # Add the table name and rows to the dictionary
                database_copy[table_name] = table_rows
            
            self.sql_handler.disconnect()
            return database_copy,200

        except Exception as e:
            # Handle exceptions
            print(f"Error copying database: {str(e)}")
            self.sql_handler.disconnect()
            return None,500
    
    def Read_database(self,table_name,low, high):
        try:
            self.sql_handler.connect()
            # Read all entries of the current table
            table_rows,status = self.sql_handler.Get_range(table_name,low,high, "Stud_id")
            if status==200:
                self.sql_handler.disconnect()
                return table_rows,200
            else:
                self.sql_handler.disconnect()
                return None,status
        except Exception as e:
            # Handle exceptions
            print(f"Error reading database: {str(e)}")
            self.sql_handler.disconnect()
            return None,500
        
    
    def Write_database(self,rows):
        return 
    
    def Update_database(self,tablename, stud_id,data):
       
        try:
            self.sql_handler.connect()
            message, status = self.sql_handler.Update_database(tablename, stud_id,data)
            
            if status==200:
                self.sql_handler.disconnect()
                if message == []:
                    return "No matching entries found",404
                return message,200

            else:
                self.sql_handler.disconnect()
                return  message,status

        except Exception as e:
            self.sql_handler.disconnect()
            return e,500
        
    
    def Delete_database(self,tablename,stud_id):
        try:
            self.sql_handler.connect()
            message, status = self.sql_handler.Delete_entry(tablename, stud_id,'Stud_id')
            if status==200:
                self.sql_handler.disconnect()
                if message == []:
                    return "No matching entries found",404
                return message,200
            else:
                self.sql_handler.disconnect()
                return message,status
        except Exception as e:
            self.sql_handler.disconnect()
            return e,500 
    
