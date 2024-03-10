import signal
from aiohttp import web
import random
from load_balancer import LoadBalancer
from heartbeat import HeartBeat
from docker_utils import *
import aiohttp
import requests
from typing import Dict

SERVER_PORT = 5000
NUM_INITIAL_SERVERS = 3
RANDOM_SEED = 4326
SLEEP_BEFORE_FIRST_REQUEST = 2

lb : LoadBalancer = ""
hb_threads: Dict[str, HeartBeat] = {}
shardT = {}   # shardT is a dictionary that maps "Stud_id_low" to a list ["Shard_id", "Shard_size", "valid_idx"]
                # Example: shardT[100] = ["sh1", "100", 123]
StudT_schema = {}   # schema is a dictionary, which has list of all columns of the StudT table and their data types
db_server_hostname = "db_server"
ShardT_schema = {
    "columns": ["Stud_id_low", "Shard_id", "Shard_size", "valid_idx"],
    "dtypes": ["Number", "String", "Number", "Number"],
    "pk": ["Stud_id_low"],
}
MapT_schema = {
    "columns": ["Shard_id", "Server_id"],
    "dtypes": ["String", "String"],
    "pk": [],
}
init_done = False


def generate_random_req_id():
    return random.randint(10000, 99999)

async def home(request):
    global lb
    
    # Generate a random request id
    req_id = generate_random_req_id()

    # Assign a server to the request using the load balancer
    server = lb.assign_server(req_id)
    if (server == ""):
        # No servers available, return a failure response
        response_json = {
            "message": f"<Error> Cannot process request! No active servers!",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
    print(f"client_handler: Request {req_id} assigned to server: {server}", flush=True)

    try:
        # Send the request to the server and get the response, use aiohttp
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.5)) as session:
            async with session.get(f'http://{server}:{SERVER_PORT}/home') as response:
            # async with request.app['client_session'].get(f'http://{server}:{SERVER_PORT}/home') as response:
                response_json = await response.json()
                response_status = response.status
                
                # increment count of requests served by the server
                lb.increment_server_req_count(server)
                
                # Return the response from the server
                return web.json_response(response_json, status=response_status, headers={"Cache-Control": "no-store"})
        # async with request.app['client_session'].get(f'http://{server}:{SERVER_PORT}/home') as response:
            # response_json = await response.json()
            # response_status = response.status
            # # Return the response from the server
            # return web.json_response(response_json, status=response_status, headers={"Cache-Control": "no-store"})
    except:
        # Request failed, return a failure response
        response_json = {
            "message": f"<Error> Request Failed",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
async def add_server_handler(request):
    global lb
    global hb_threads
    try:
        # Get a payload json from the request
        payload = await request.json()
        # print(payload, flush=True)
        # Get the number of servers to be added
        num_servers = int(payload['n'])
        # Get the list of new_shards
        shards = list(payload.get('new_shards', []))
        # Get the dictionary of server to shard mapping
        serv_to_shard = dict(payload.get('servers', {}))
        
        print(f"client_handler: Received Request to add N: {num_servers} servers, shards: {shards}, server to shard mapping: {serv_to_shard}", flush=True)
    except:
        response_json = {
            "message": f"<Error> Invalid payload format",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    if num_servers<=0:
        response_json = {
            "message": f"<Error> Invalid number of servers to be added: {num_servers}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
    # Add the shards to the system
    if len(shards) > 0:
        def get_values(shard):
            return [shard[col] for col in ShardT_schema["columns"] if col in shard]

        list_of_shards = list(map(get_values, shards))
        print(f"client_handler: Adding Shards: {list_of_shards}", flush=True)
        new_shards = lb.add_shards(list_of_shards)
        if len(new_shards) <= 0:
            response_json = {
                "message": f"<Error> Failed to add shards to the system",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        # Populate the shardT
        for shard in new_shards:
            shardT[shard[0]] = shard[1:] + [0]
        print(f"client_handler: Added {len(new_shards)} shards to the system")
        print(f"client_handler: ShardT: {shardT}")
    # Add the servers to the system
    num_added, added_servers, err = lb.add_servers(num_servers, serv_to_shard)

    if num_added<=0:
        response_json = {
            "message": f"<Error> Failed to add servers to the system",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)

    print(f"client_handler: Added {num_added} servers to the system")
    print(f"client_handler: Added Servers: {added_servers}", flush=True)
    if err!="":
        print(f"client_handler: Error: {err}")

    # Spawn the heartbeat threads for the added servers
    for server in added_servers:
        t1 = HeartBeat(lb, server)
        hb_threads[server] = t1
        t1.start()

    # Return the full list of servers in the system
    server_list = lb.list_servers()
    response_json = {
        "N" : len(server_list),
        "message": f"Added servers: {added_servers}",
        "status": "successful"
    }

    return web.json_response(response_json, status=200)



async def remove_server_handler(request):
    global lb
    global hb_threads

    try:
        # Get a payload json from the request
        payload = await request.json()
        # payload = await request.text()
        # print(payload, flush=True)
        # Get the number of servers to be removed
        num_servers = int(payload['n'])
        # num_servers = 3
        # Get the list of preferred hostnames
        pref_hosts = list(payload.get('servers', []))
        # pref_hosts = ['pranav']
        print(f"client_handler: Received Request to remove N: {num_servers} servers, Hostnames: {pref_hosts}", flush=True)
    except:
        response_json = {
            "message": f"<Error> Invalid payload format",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)

    if num_servers<=0:
        response_json = {
            "message": f"<Error> Invalid number of servers to be removed: {num_servers}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
    # Remove the servers from the system
    num_removed, removed_servers, err = lb.remove_servers(num_servers, pref_hosts)

    if num_removed<=0:
        response_json = {
            "message": f"<Error> Failed to remove servers from the system",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
    print(f"client_handler: Removed {num_removed} servers from the system")
    print(f"client_handler: Removed Servers: {removed_servers}", flush=True)
    if err!="":
        print(f"client_handler: Error: {err}")

    # Kill the heartbeat threads for the removed servers
    for server in removed_servers:
        hb_threads[server].stop()
        del hb_threads[server]
        # close the docker containers and corresponding threads for the servers that were finally removed
        kill_server_cntnr(server)

    # Return the full list of servers in the system
    server_list = lb.list_servers()
    response_json = {
        "message": {
            "N" : len(server_list),
            "replicas": server_list
        },
        "status": "successful"
    }

    return web.json_response(response_json, status=200)


async def rep_handler(request):
    global lb
    print(f"client_handler: Received Request to list all servers", flush=True)
    # return a list of all the current servers
    server_list = lb.list_servers()
    response_json = {
        "message": {
            "N" : len(server_list),
            "replicas": server_list
        },
        "status": "successful"
    }
    return web.json_response(response_json, status=200)

## Nyati's changes here: 

 
async def lb_analysis(request):
    global lb
    print(f"client_handler: Received Request to provide server load statistics", flush=True)
    load_count = lb.get_server_load_stats()
    response_json = {
        "message": f"Server Load Statistics:",
        "dict": load_count,
        "status": "successful"
    }
    return web.json_response(response_json, status=200)

## My changes below:
# Function to send a heartbeat to the db server and return True if the server is alive, else False
def heartbeat_db_server():
    try:
        response = requests.get(f'http://{db_server_hostname}:{SERVER_PORT}/heartbeat')
        response_status = response.status_code
        if response_status == 200:
            return True
        else:
            return False
    except:
        return False

# Function to send POST request to the server /config endpoint to initialize the database
async def config_server(server, payload):
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.5)) as session:
            async with session.post(f'http://{server}:{SERVER_PORT}/config', json=payload) as response:
                response_status = response.status
                if response_status == 200:
                    return True
                else:
                    print(f"client_handler: Failed to configure server {server}\nResponse: {await response.json()}", flush=True)
                    return False
    except:
        return False

# Function to write values to the /write endpoint of server
## IMPORTANT: Only to be used for writing to the db_server
async def write_server(server, payload):
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.5)) as session:
            async with session.post(f'http://{server}:{SERVER_PORT}/write', json=payload) as response:
                response_status = response.status
                if response_status == 200:
                    return True
                else:
                    print(f"client_handler: Failed to write to server {server}\nResponse: {await response.json()}", flush=True)
                    return False
    except:
        return False

async def spawn_and_config_db_server(serv_to_shard: Dict[str, list]):
    # Spawn the db_server container
    if not spawn_db_server_cntnr(db_server_hostname):
        response_json = {
            "message": f"<Error> Failed to start the db_server container",
            "status": "failure"
        }
        return False, response_json
    
    await asyncio.sleep(SLEEP_BEFORE_FIRST_REQUEST)
    # Configure the db_server with the two tables: ShardT and MapT
    payload = {
        "schemas": {
            "ShardT": ShardT_schema,
            "MapT": MapT_schema
        },
        "StudT_schema": StudT_schema,
    }
    if not await config_server(db_server_hostname, payload):
        response_json = {
            "message": f"<Error> Failed to configure the db_server",
            "status": "failure"
        }
        return False, response_json
    
    # Populate the ShardT_schema and MapT_schema
    payload = {
        "table": "ShardT",
        "data": [],
    }
    for shard, val in shardT.items():
        # Map ShardT_schema["columns"] to shard, val
        payload["data"].append(dict(zip(ShardT_schema["columns"], [shard] + val)))
    # print(f"client_handler: Writing ShardT to db_server: {payload}", flush=True)
    if not await write_server(db_server_hostname, payload):
        response_json = {
            "message": f"<Error> Failed to write ShardT table to the db_server",
            "status": "failure"
        }
        return False, response_json
    
    payload = {
        "table": "MapT",
        "data": [],
    }
    for server, shards in serv_to_shard.items():
        for shard in shards:
            payload["data"].append(dict(zip(MapT_schema["columns"], [shard, server])))
    if not await write_server(db_server_hostname, payload):
        response_json = {
            "message": f"<Error> Failed to write MapT table to the db_server",
            "status": "failure"
        }
        return False, response_json
    print(f"client_handler: db_server container started and configured successfully")
    response_json = {
        "message": f"db_server container started and configured successfully",
        "status": "successful"
    }
    return True, response_json

async def init_handler(request):
    global lb
    global hb_threads
    global shardT
    global StudT_schema
    global init_done
    # Check init_done flag
    if init_done:
        response_json = {
            "message": f"<Error> Database already initialized from the /init endpoint, init can be done only once",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    try:
        # Get a payload json from the request
        payload = await request.json()
        print(payload, flush=True)
        # Get the number of servers to be added
        num_servers = int(payload['N'])
        # Get the StudT_schema
        studt_schema = dict(payload['schema'])
        # Get the list of new shards and their details
        shards = list(payload['shards'])
        # Get the dictionary of server to shard mapping
        serv_to_shard = dict(payload['servers'])
        
        print(f"client_handler: Received Request to add N: {num_servers} servers, StudT schema: {studt_schema}, shards: {shards}, server to shard mapping: {serv_to_shard}", flush=True)
    except:
        response_json = {
            "message": f"<Error> Invalid payload format",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    if num_servers<=0:
        response_json = {
            "message": f"<Error> Invalid number of servers to be added: {num_servers}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
    # If the db_server container is already running, then remove the existing servers, stop the heartbeat threads
    if heartbeat_db_server():
        # Kill the heartbeat threads for tem_servers
        tem_servers = list(hb_threads.keys())
        for server in tem_servers:
            hb_threads[server].stop()
            del hb_threads[server]
            # close the docker containers and corresponding threads for the servers that were finally removed
            kill_server_cntnr(server)

        # Kill the db_server container
        kill_db_server_cntnr(db_server_hostname)

        # Delete the LoadBalancer object
        del lb
        # New LoadBalancer object
        lb = LoadBalancer()
        # Clear the shardT
        shardT = {}
        # Clear the hb_threads
        hb_threads = {}

    new_shards = []
    # Add the shards to the system
    if len(shards) > 0:
        def get_values(shard):
            return [shard[col] for col in ShardT_schema["columns"] if col in shard]
        
        list_of_shards = list(map(get_values, shards))
        print(f"client_handler: Adding Shards: {list_of_shards}", flush=True)
        new_shards = lb.add_shards(list_of_shards)
        if len(new_shards) <= 0:
            response_json = {
                "message": f"<Error> Failed to add shards to the system",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        # Populate the shardT
        for shard in new_shards:
            shardT[shard[0]] = shard[1:] + [0]
        print(f"client_handler: Added {len(new_shards)} shards to the system")
        print(f"client_handler: ShardT: {shardT}")
    # Add the servers to the system
    num_added, added_servers, err = lb.add_servers(num_servers, serv_to_shard)
    if err!="":
        print(f"client_handler: Error: {err}")
    if num_added<=0:
        response_json = {
            "message": f"<Error> Failed to add servers to the system",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    print(f"client_handler: Added {num_added} servers to the system", flush=True)
    # Spawn the heartbeat threads for the added servers
    for server in added_servers:
        t1 = HeartBeat(lb, server)
        hb_threads[server] = t1
        t1.start()

    # Populate the StudT_schema
    StudT_schema = studt_schema
    
    # await asyncio.sleep(SLEEP_BEFORE_FIRST_REQUEST)


    # db_server is not running, spawn a new container and configure it
    success, response_json = await spawn_and_config_db_server(serv_to_shard)
    if not success:
        return web.json_response(response_json, status=400)

    payload = {
        "schema": studt_schema,
        "shards": []
    }
    # Configure the servers with the StudT schema and the server to shard mapping
    for server in added_servers:
        payload["shards"] = serv_to_shard[server]
        # Send a POST request to the server /config endpoint to initialize the database
        if not await config_server(server, payload):
            response_json = {
                "message": f"<Error> Failed to configure server {server}",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)



    print(f"client_handler: Initialized all the servers and configured them successfully", flush=True)
    # Set the init_done flag to True
    init_done = True

    response_json = {
        "message": "Configured Database",
        "status": "successful"
    }
    return web.json_response(response_json, status=200)

async def status_handler(request):
    global lb
    global hb_threads
    global shardT
    global StudT_schema
    global init_done
    servers, serv_to_shard = lb.list_servers(send_shard_info=True)
    shards = [dict(zip(ShardT_schema["columns"], [shard] + shardT[shard])) for shard in shardT.keys()]
    response_json = {
        "message": {
            "N": len(servers),
            "schema": StudT_schema,
            "shards": shards,
            "servers": serv_to_shard,
        },
        "status": "successful"
    }
    return web.json_response(response_json, status=200)

def recover_from_db_server():
    global lb
    global hb_threads
    global shardT
    global StudT_schema
    try:
        if heartbeat_db_server():
            # Get the database from the db_server
            response = requests.get(f'http://{db_server_hostname}:{SERVER_PORT}/read')
            response_status = response.status_code
            response_json = response.json()
            if response_status == 200:
                print(f"client_handler: Database recovery from db_server started")
                # Get the StudT_schema
                StudT_schema = response_json["StudT_schema"]
                database = response_json["data"]
                shardT_data = database["ShardT"]
                # Add the shards to the system
                new_shards = lb.add_shards(shardT_data)
                if len(new_shards) <= 0:
                    print(f"client_handler: Failed to add shards to the system")
                    return False
                # Populate the shardT
                for shard in new_shards:
                    shardT[shard[0]] = shard[1:]
                print(f"client_handler: Added {len(new_shards)} shards to the system")
                print(f"client_handler: ShardT: {shardT}")
                
                # Get the MapT
                mapT_data = database["MapT"]
                serv_to_shard = {}
                for val in mapT_data:
                    if val[1] not in serv_to_shard:
                        serv_to_shard[val[1]] = []
                    serv_to_shard[val[1]].append(val[0])
                # Add the servers to the system
                num_added, added_servers, err = lb.add_servers(len(serv_to_shard), serv_to_shard, should_spawn=False)
                if err!="":
                    print(f"client_handler: Error: {err}")
                    return False
                if num_added<=0:
                    print(f"client_handler: Failed to add servers to the system")
                    return False
                print(f"client_handler: Added {num_added} servers to the system")
                # Spawn the heartbeat threads for the added servers
                for server in added_servers:
                    t1 = HeartBeat(lb, server)
                    hb_threads[server] = t1
                    t1.start()
                return True
            else:
                return False
        else:
            return False
    except Exception as e:
        print(f"client_handler: Error in recovering from db_server: {str(e)}")
        return False

async def not_found(request):
    global lb
    print(f"client_handler: Invalid Request Received: {request.rel_url}", flush=True)
    response_json = {
        "message": f"<Error> '{request.rel_url}' endpoint does not exist in server replicas",
        "status": "failure"
    }
    return web.json_response(response_json, status=400)

def interrupt_handler(signum, frame):
    # Handle the interrupt signal
    print(f"client_handler: Received Interrupt Signal {signum}, Exiting...", flush=True)
    # Kill the heartbeat threads
    for server in hb_threads.keys():
        hb_threads[server].stop()
        kill_server_cntnr(server)
    # Kill the db_server container
    kill_db_server_cntnr(db_server_hostname)
    exit(0)

def run_load_balancer():
    global lb
    global hb_threads
    random.seed(RANDOM_SEED)
    signal.signal(signal.SIGINT, interrupt_handler)
    signal.signal(signal.SIGTERM, interrupt_handler)
    signal.signal(signal.SIGQUIT, interrupt_handler)
    signal.signal(signal.SIGABRT, interrupt_handler)
    ### Add check if DB server already exists(by sending heartbeat to it)
    ### if yes, then copy the configurations and start heartbeat threads for them 
    ### DO NOT SPAWN NEW SERVERS, cause they might be already running, if not, heartbeat will take care of it
    # initial_servers = []
    # for i in range(NUM_INITIAL_SERVERS):
    #     initial_servers.append(f"server{i+1}")
    lb = LoadBalancer()
    ### Call Add shards and add servers here(without spawning new containers)
    # Try to recover from the db_server
    done = recover_from_db_server()
    if done:
        print(f"client_handler: Recovered from db_server successfully", flush=True)
    else:
        print(f"client_handler: DB server not running, starting fresh", flush=True)
    app = web.Application()
    app.router.add_get('/home', home)
    app.router.add_post('/add', add_server_handler)
    app.router.add_delete('/rm', remove_server_handler)
    app.router.add_get('/rep', rep_handler)
    app.router.add_get('/lb_analysis', lb_analysis)
    app.router.add_post('/init', init_handler)
    app.router.add_get('/status', status_handler)

    app.router.add_route('*', '/{tail:.*}', not_found)

    web.run_app(app, port=5000)

    print("Load Balancer Ready!", flush=True)

    for thread in hb_threads.values():
        thread.join()
