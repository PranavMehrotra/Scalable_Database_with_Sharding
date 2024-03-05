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
                    self.sql_handler.Create_table(shard, columns, dtypes)
                
                return "Tables created successfully", 200
            else:
                return "Invalid JSON format", 400
        except Exception as e:
            return e, 500
    
    def Copy_database(self):
        return 
    
    def Read_database(self,low, high):
        return 
    
    def Write_database(self,rows):
        return 
    
    def Update_database(self,row):
        return 
    
    def Delete_database(self,shardid):
        return 
    
    def disconnect(self):
        try:
            self.sql_handler.disconnect()
            return "Disconnected", 200
        except Exception as e:
            return e, 500