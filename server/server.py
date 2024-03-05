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
            # Extract shards from the request JSON
            shards = request_json.get('shards', [])
            # Construct the response message
            message = ", ".join([f"{server_id}:{shard}" for shard in shards]) + " configured"
            
            # Construct the JSON response
            response_json = {
                "message": message,
                "status": "success"
            }
            
            mgr.disconnect()
            # Return the JSON response with status code 200
            return web.json_response(response_json, status=200)
        
        else:
            print(f"Error in Config endpoint: {str(message)}")
            return web.json_response({"error": "Internal Server Error"}, status=500)

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
    return {}

async def read_database(request):
    return {}

async def write_database(request):
    return {}

async def update(request):
    return {}

async def del_database(request):
    return {}


# Define an asynchronous function for handling the home endpoint
async def home(request):
    try:
        
        # Prepare response data
        response_data = {
            "message": f"Hello from Server: {server_id}",
            "status": "successful"
        }

        # Return a JSON response with a 200 OK status and Cache-Control header
        return web.json_response(response_data, status=200, headers={"Cache-Control": "no-store"})
    
    except Exception as e:
        # Log the exception and return an error response if an exception occurs
        print(f"Error in home endpoint: {str(e)}")
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
