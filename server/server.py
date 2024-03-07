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
        message, status = mgr.Config_database(request_json)

        if status == 200:
            shards = request_json.get('shards', [])
            message = ", ".join([f"{server_id}:{shard}" for shard in shards]) + " configured"
            
            # Construct the JSON response
            response_json = {
                "message": message,
                "status": "success"
            }
            
        else:
           
            response_json = {
                "error": str(message),
                "status": "failure"
            }
        
        return web.json_response(response_json, status=status)

    except Exception as e:
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
        print("Copy endpoint called")
        request_json = await request.json()  # Extract JSON data from request       
        database_copy, status = mgr.Copy_database(request_json)
        
        if status == 200:
            response_json = {}
            
            for shard_name, shard_data in database_copy.items():
                updated_shard_data = [[row[1:], ] for row in shard_data]
                response_json[shard_name] = updated_shard_data
            
            response_json["status"] = "success"
            
        else:
            message = database_copy
            response_json = {
                "error": message,
                "status": "failure"
            }

        # Return a JSON response with the database copy and the corresponding status code
        return web.json_response(response_json, status=status)
    
    except Exception as e:
        # Log the exception and return an error response if an exception occurs
        print(f"Error in copy endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

async def read_database(request):
    try:
        print("Read endpoint called")
        request_json = await request.json()  # Extract JSON data from request
        database_entry, status = mgr.Read_database(request_json)
        
     
        if status==200:
            umodified_list = [(item[1], item[2], item[3]) for item in database_entry]
            response_data = {
                    "data": umodified_list,
                    "status": "success"
                }
        else:
            message = umodified_list
            response_data = {
                    "error": message,
                    "status": "failure"
                }
    
        return web.json_response(response_data, status=status)
   
    except Exception as e:
        
        print(f"Error in read endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

async def write_database(request):
    try:
        print("Write endpoint called")
        request_json = await request.json()  # Extract JSON data from request
        message, status = mgr.Write_database(request_json)
        
        if status == 200:
            response_data = {
                    "message": message,
                    "status": "success"
                }
            
        else:
            response_data = {
                    "error": message,
                    "status": "failure"
                }
            
        return web.json_response(response_data, status=status)  
    
    except Exception as e:
        
        print(f"Error in write endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)  

async def update(request):
    
    try:
        print("Update endpoint called")
        request_json = await request.json()  # Extract JSON data from request
        stud_id = request_json.get("Stud_id")
        message, status = mgr.Update_database(request_json)
    
        if status == 200:
            response_data = {
                "message": f"Data entry for Stud_id:{stud_id} updated",
                "status": "success"
            }
            return web.json_response(response_data, status=status)
        
        else:
            response_data = {
                "message": f"{str(message)}",
                "status": "failure"
            }
            return web.json_response(response_data, status=status)        
        
    except Exception as e:
        
        print(f"Error in update endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

async def del_database(request):
    try:
        print("Delete endpoint called")
        request_json = await request.json()  # Extract JSON data from request
        stud_id = request_json.get("Stud_id")
        message, status = mgr.Delete_database(request_json)

        if status==200:
            response_data = {
                "message": f"Data entry with Stud_id:{stud_id} removed",
                "status": "success"
            }
        
        else:
            response_data = {
                "error": f'{str(message)}',
                "status": "failure"
            }    
        
        return web.json_response(response_data, status=status)
   
    except Exception as e:
        
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
