from helper import SQLHandler
import json

class Manager:
    def __init__(self,host='localhost',user='root',password='saransh03sharma',db='server_database'):
        self.sql_handler=SQLHandler(host=host,user=user,password=password,db=db)
        
        
    def Config_database(self,config):
        try:
            
            message, status = self.sql_handler.connect()
            
            if status != 200:
                return message, status

            if isinstance(config, str):
            # If config is already a string, no need to load it again
                config = json.loads(config)
            
            # Now schema is a dictionary containing the parsed JSON data
            elif isinstance(config, dict):
                # If config is a dictionary, no need to load it, just use it directly
                config = config

               
            if 'shards' not in config:
                return "'shards' field missing in request", 400
        
            # Additional sanity checks for schema consistency
            schema = config.get('schema', {})
            shards = config.get('shards', [])
            columns = schema.get('columns', [])
            dtypes = schema.get('dtypes', [])
            
            # Check if the number of columns matches the number of data types
            if len(columns) != len(dtypes):
                return "Number of columns and dtypes don't match", 400

            if schema and columns and dtypes and shards:
                
                for shard in shards:
                    message, status = self.sql_handler.Create_table(shard, columns, dtypes)
                
                if status == 200:
                    return "Tables created successfully", 200
                else: return message, status
            else:
                return "Invalid JSON format", 400
        except Exception as e:
            return e, 500
    
    def Copy_database(self,json_data):
        
        try:
            if 'shards' not in json_data:
                return "'shards' field missing in request",400
        
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status            

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
            
            return database_copy,200

        except Exception as e:
            # Handle exceptions
            print(f"Error copying database: {str(e)}")
            return e,500
    
    def Read_database(self,request_json):
        try:
            
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status
                
            if 'shard' not in request_json:
                return "'shard' field missing in request", 400
    
            stud_id_obj = request_json["Stud_id"]
            table_name = request_json['shard']
            low, high = stud_id_obj["low"], stud_id_obj["high"]

            if "low" not in stud_id_obj or "high" not in stud_id_obj:
                return "Both low and high values are required", 400
            

            # Read all entries of the current table
            table_rows,status = self.sql_handler.Get_range(table_name,low,high, "Stud_id")
            if status==200:                
                return table_rows,200
            else:
                message = table_rows
                return message,status
        except Exception as e:
            # Handle exceptions
            print(f"Error reading database: {str(e)}")
            return e,500
        
    
    def Write_database(self,request_json):
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

            if 'shard' not in request_json:
                return "'shard' field missing in request", 400
        
            tablename = request_json.get("shard")
            data = request_json.get("data")
            curr_idx = request_json.get("curr_idx")


            res,status = self.sql_handler.query(f"SELECT COUNT(*) FROM {tablename}")
            
            if status != 200:
                return res, status
            
            valid_idx = res[0][0]
            if(curr_idx!=valid_idx+1):
                
                return "Invalid index",400
            
            schema = '(Stud_id,Stud_name,Stud_marks)'
            message, status = self.sql_handler.Insert(tablename, data,schema)
            if status != 200:
                
                return message, status
            
            
            return "Entries added successfully", 200
        
        except Exception as e:
            
            return e, 500
        return 
    
    def Update_database(self,request_json):
       
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

                
            if 'shard' not in request_json:
                return "'shard' field missing in request", 400
            
            if 'Stud_id' not in request_json:
                return "'Stud_id' field missing in request", 400
            
            if 'data' not in request_json:
                return "'data' field missing in request", 400
            
            stud_id = request_json.get("Stud_id")
            data = request_json.get("data")
            tablename = request_json.get("shard")

            # Check if the 'Stud_id' in the payload matches the 'Stud_id' in the 'data' object
            if stud_id == data["Stud_id"]:
                message, status = self.sql_handler.Update_database(tablename, stud_id,data)
            
                if status==200:
                    if message == []:
                        return "No matching entries found",404
                    return message,200

                else:                    
                    return  message,status
            else:
                return "Stud_id in 'data' does not match Stud_id in payload", 400

        except Exception as e:            
            return e,500
        
    
    def Delete_database(self,request_json):
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

            if 'shard' not in request_json:
                return "'shard' field missing in request", 400

            if 'Stud_id' not in request_json:
                return "'Stud_id' field missing in request", 400
            
            stud_id = request_json.get("Stud_id")
            tablename = request_json.get("shard")

            message, status = self.sql_handler.Delete_entry(tablename, stud_id,'Stud_id')
            
            if status==200:
                if message == []:
                    return "No matching entries found",404
                return message,200
            else:    
                return message,status
        
        except Exception as e:    
            return e,500 
    
