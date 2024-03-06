# Import necessary modules
from aiohttp import web
import os
from manager import Manager
import json

mgr = Manager()
# Initialize server_id as an empty string (will be set later)
server_id = ""

async def config(request):
    try:
        print("Config endpoint called")
        request_json = await request.json()  # Extract JSON data from request
        if 'shards' not in request_json:
            return web.json_response({"error": "'shards' field missing in request"}, status=400)
        
        # Additional sanity checks for schema consistency
        schema = request_json.get('schema', {})
        columns = schema.get('columns', [])
        dtypes = schema.get('dtypes', [])
        
        # Check if the number of columns matches the number of data types
        if len(columns) != len(dtypes):
            return web.json_response({"error": "Number of columns and dtypes don't match"}, status=400)
        
        message, status = mgr.Config_database(request_json)

        if status == 200:
            # Extract shards from the request JSON
            shards = request_json.get('shards', [])
            # Construct the response message
            message = ", ".join([f"{server_id}:{shard}" for shard in shards]) + " configured"
            
            # Construct the JSON response
            response_json = {
                "message": message,
                "status": "success"
            }
            
            
            # Return the JSON response with status code 200
            return web.json_response(response_json, status=200)
        
        else:
            print(f"Error in Config endpoint: {str(message)}")
            return web.json_response({"error": f"str{message}"}, status=status)

    except Exception as e:
        # Log the exception and return an error response if an exception occurs
        print(f"Error in Config endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)
    
# Define an asynchronous function for handling the heartbeat endpoint
async def heartbeat(request):
    try:
        # Return a simple 200 OK response
        return web.Response(status=200)
    
    except Exception as e:
        # Log the exception and return an error response if an exception occurs
        print(f"Error in heartbeat endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)


async def copy_database(request):
    try:
        request_json = await request.json()  # Extract JSON data from request
        if 'shards' not in request_json:
            return web.json_response({"error": "'shards' field missing in request"}, status=400)
        
        # Call the Copy_database method of the Manager class
        database_copy, status = mgr.Copy_database(request_json)
        
        
        # Construct the response JSON
        response_json = {}
        
        # Add database copy for each shard
        for shard_name, shard_data in database_copy.items():
            updated_shard_data = [[row[1:], ] for row in shard_data]
            response_json[shard_name] = updated_shard_data
        
        # Add the status
        response_json["status"] = "success"
        
        if status == 200:
            # Return a JSON response with the database copy and the corresponding status code
            return web.json_response(response_json, status=status)
        else:
            return web.json_response({"error": "Internal Server Error"}, status=500)
    
    except Exception as e:
        # Log the exception and return an error response if an exception occurs
        print(f"Error in copy endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)
 

async def read_database(request):
    try:
        request_json = await request.json()  # Extract JSON data from request
        if 'shard' not in request_json:
            return web.json_response({"error": "'shard' field missing in request"}, status=400)
    
        stud_id_obj = request_json["Stud_id"]

        if "low" not in stud_id_obj or "high" not in stud_id_obj:
            return web.json_response({"error": "Both low and high values are required"}, status=400)
        
        database_entry, status = mgr.Read_database(request_json['shard'],stud_id_obj["low"], stud_id_obj["high"])
        
        umodified_list = [(item[1], item[2], item[3]) for item in database_entry]

        
        response_data = {
                "data": umodified_list,
                "status": "success"
            }
        if status==200:
            return web.json_response(response_data, status=status)
        else:
            return web.json_response({"error": {database_entry}}, status=status)
    except Exception as e:
        # Log the exception and return an error response if an exception occurs
        print(f"Error in read endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

async def write_database(request):
    try:
        request_json = await request.json()  # Extract JSON data from request
        if 'shard' not in request_json:
            return web.json_response({"error": "'shard' field missing in request"}, status=400)
        
        message, status = mgr.Write_database(request_json['shard'],request_json['data'])
        response_data = {
                "message": message,
                "status": "success"
            }
        return web.json_response(response_data, status=status)
    
    except Exception as e:
        # Log the exception and return an error response if an exception occurs
        print(f"Error in write endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)
    

async def update(request):
    try:
        request_json = await request.json()  # Extract JSON data from request
        if 'shard' not in request_json:
            return web.json_response({"error": "'shard' field missing in request"}, status=400)
        
        if 'Stud_id' not in request_json:
            return web.json_response({"error": "'Stud_id' field missing in request"}, status=400)
        
        if 'data' not in request_json:
            return web.json_response({"error": "'data' field missing in request"}, status=400)
        
        stud_id = request_json.get("Stud_id")
        data = request_json.get("data")

        # Check if the 'Stud_id' in the payload matches the 'Stud_id' in the 'data' object
        if stud_id == data["Stud_id"]:
            # Call mgr.Read_database function
            message, status = mgr.Update_database(request_json['shard'], stud_id,data)
            if status == 200:
                response_data = {
                    "message": f"Data entry for Stud_id:{stud_id} updated",
                    "status": "success"
                }
                return web.json_response(response_data, status=status)
            else:
                print(1)
                response_data = {
                    "message": f"{str(message)}",
                    "status": "failure"
                }
                return web.json_response(response_data, status=status)

        else:
            return web.json_response({"error": "Stud_id in 'data' does not match Stud_id in payload"}, status=400)
        
        
    except Exception as e:
        # Log the exception and return an error response if an exception occurs
        print(f"Error in update endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

async def del_database(request):
    try:
        request_json = await request.json()  # Extract JSON data from request
        if 'shard' not in request_json:
            return web.json_response({"error": "'shard' field missing in request"}, status=400)
        
        if 'Stud_id' not in request_json:
            return web.json_response({"error": "'Stud_id' field missing in request"}, status=400)
        
        stud_id = request_json.get("Stud_id")
        message, status = mgr.Delete_database(request_json['shard'],request_json['Stud_id'])
        
        if status==200:
            response_data = {
                "message": f"Data entry with Stud_id:{stud_id} removed",
                "status": "success"
            }
            
            return web.json_response(response_data, status=status)
        else:
            response_data = {
                "message": f'{str(message)}',
                "status": "failure"
            }
            
            return web.json_response(response_data, status=status)
   
    except Exception as e:
        # Log the exception and return an error response if an exception occurs
        print(f"Error in delete endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)
   

# Define a synchronous function for handling requests to unknown endpoints
async def not_found(request):
    # Return a 400 Bad Request response with a plain text message
    return web.Response(text="Not Found", status=400)


# Define the main function to run the web server
def run_server():
    # Create an instance of the web Application
    app = web.Application()

    # Add routes for the home and heartbeat endpoints
    app.router.add_post('/config', config)
    app.router.add_get('/heartbeat', heartbeat)
    app.router.add_get('/copy', copy_database)
    app.router.add_post('/read', read_database)
    app.router.add_post('/write', write_database)
    app.router.add_put('/update', update)
    app.router.add_delete('/del', del_database)


    # Add a catch-all route for any other endpoint, which returns a 400 Bad Request
    app.router.add_route('*', '/{tail:.*}', not_found)

    # Run the web application on port 5000
    web.run_app(app, port=5000)

# Entry point of the script
if __name__ == '__main__':
    # Get the server_id from the environment variable
    server_id = 'Server0' #os.environ.get("SERVER_ID")
    # Run the web server
    run_server()
