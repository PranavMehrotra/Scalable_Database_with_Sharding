from aiohttp import web
import random
from load_balancer import LoadBalancer
from heartbeat import HeartBeat
from docker_utils import spawn_server_cntnr, kill_server_cntnr
import aiohttp
from typing import Dict

SERVER_PORT = 5000
NUM_INITIAL_SERVERS = 3
RANDOM_SEED = 4326

lb : LoadBalancer = ""
hb_threads: Dict[str, HeartBeat] = {}
shardT = {}   # shardT is a dictionary that maps "Stud_id_low" to a list ["Shartd_id", "Shard_size", "valid_idx"]
                # Example: shardT[100] = ["sh1", "100", 123]

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
        new_shards = lb.add_shards(shards)
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
async def init_handler(request):
    pass

async def status_handler(request):
    pass

async def not_found(request):
    global lb
    print(f"client_handler: Invalid Request Received: {request.rel_url}", flush=True)
    response_json = {
        "message": f"<Error> '{request.rel_url}' endpoint does not exist in server replicas",
        "status": "failure"
    }
    return web.json_response(response_json, status=400)


def run_load_balancer():
    global lb
    global hb_threads
    random.seed(RANDOM_SEED)

    ### Add check if DB server already exists(by sending heartbeat to it)
    ### if yes, then copy the configurations and start heartbeat threads for them 
    ### DO NOT SPAWN NEW SERVERS, cause they might be already running, if not, heartbeat will take care of it
    # initial_servers = []
    # for i in range(NUM_INITIAL_SERVERS):
    #     initial_servers.append(f"server{i+1}")
    lb = LoadBalancer()
    ### Call Add shards and add servers here(without spawning new containers)
    tem_servers = lb.list_servers()
    # print(tem_servers)
    for server in tem_servers:
        t1 = HeartBeat(lb, server)
        hb_threads[server] = t1
        t1.start()
    app = web.Application()
    app.router.add_get('/home', home)
    app.router.add_post('/add', add_server_handler)
    app.router.add_delete('/rm', remove_server_handler)
    app.router.add_get('/rep', rep_handler)
    app.router.add_get('/lb_analysis', lb_analysis)

    app.router.add_route('*', '/{tail:.*}', not_found)

    web.run_app(app, port=5000)

    print("Load Balancer Ready!", flush=True)

    for thread in hb_threads.values():
        thread.join()
