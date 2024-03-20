import aiohttp
import asyncio
from pprint import pprint
import string
import random
from typing import Dict

random.seed(42)

ip_address = '0.0.0.0'
port = 5000
STUD_ID_MAX = 12000
SHARD_SIZE = 4096

def generate_random_string(length=4):
    letters = string.ascii_uppercase
    return "S_" + ''.join(random.choice(letters) for _ in range(length))


def generate_random_range():
    low = random.randint(0, STUD_ID_MAX)
    # high = random.randint(low, low + 1000)
    high = random.randint(low, low + 2000)
    return low, high

def create_consistent_hashmap(servers: int,
                            shards: int,
                            replicas: int) -> Dict:
    
    total_replicas = shards * replicas
    replicas_per_server = total_replicas // servers
    replicas_left = total_replicas % servers
    server_shard_map = {}
    shard_replica_count = {f"sh{i+1}": replicas for i in range(shards)}
    server_replica_assgn = {}
    for i in range(servers):
        if i < replicas_left:
            server_replica_assgn[f"Server{i}"] = replicas_per_server + 1
        else:
            server_replica_assgn[f"Server{i}"] = replicas_per_server

    for i in range(servers):
        start = i*replicas_per_server
        end = (i+1)*replicas_per_server
        idxs_per_server = [(idx%shards+1) for idx in range(start, end)]
        shard_ids_per_server = [f"sh{idx}" for idx in idxs_per_server]
        server_shard_map[f"Server{i}"] = shard_ids_per_server
        server_replica_assgn[f"Server{i}"] -= len(shard_ids_per_server)
        if server_replica_assgn[f"Server{i}"] == 0:
            server_replica_assgn.pop(f"Server{i}")
        for idx in idxs_per_server:
            shard_replica_count[f"sh{idx}"] -= 1
            if shard_replica_count[f"sh{idx}"] == 0:
                shard_replica_count.pop(f"sh{idx}")

    # distribute the remaining replicas among the left over servers
    assert len(server_replica_assgn) == len(shard_replica_count) == replicas_left, "Replicas not distributed properly"
    servers_left_ids = list(server_replica_assgn.keys())
    shards_left_ids = list(shard_replica_count.keys())

    for server, shard in zip(servers_left_ids, shards_left_ids):
        server_shard_map[server].append(shard)
        server_replica_assgn[server] -= 1
        shard_replica_count[shard] -= 1

    return server_shard_map


def create_init_json(args: Dict) -> Dict:
    servers = int(args.servers)
    shards = int(args.shards)
    replicas = int(args.replicas)

    return {
        "N": servers,
        "schema": {
                    "columns":["Stud_id","Stud_name","Stud_marks"],
                    "dtypes":["Number","String","String"]
                },
        "shards": [
            {
                "Stud_id_low": SHARD_SIZE * i,
                "Shard_id": f"sh{i+1}",
                "Shard_size": SHARD_SIZE
            }
            for i in range(shards)
        ],
        "servers": create_consistent_hashmap(servers, shards, replicas)
    }

# ----- API Calls -----

async def init(session, json_data):
    try:
        async with session.post(f'http://{ip_address}:{port}/init', json=json_data) as response:
            if response.status == 200:
                print("JSON Request Successful")
                print(await response.json())
            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
            return response.status
    except Exception as e:
        print("Error:", e)
        return 500

async def status(session):
    try:
        async with session.get(f'http://{ip_address}:{port}/status') as response:
            if response.status == 200:
                response_json = await response.json()
                
                if response_json.get("data", None) is not None:
                    data_list = response_json.get("data", [])

                else:
                    print("Response message: ")
                    pprint(response_json.get('message', 'No message'))

            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
        
            return response.status
    except Exception as e:
        print("Error:", e)
        return 500
    
async def read_shard(session, json_data):
    try:
            # print(f"Sending Read Request: {json_data}")
        # async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{ip_address}:{port}/read', json=json_data) as response:
            if response.status == 200:
                    # print("JSON Request Successful")
                response_json = await response.json()
                
                if response_json.get("data", None) is not None:
                    data_list = response_json.get("data", [])

                    print("No of shards queried: ", response_json.get("shards_queried", 0))

                    # print(response_json)

                    print("No of data points read:", len(data_list))

                    data_list.sort(key=lambda x: x["Stud_id"])
                    print("Data points read:")

                    # for data in response_json.get("data", []):
                    #     print(data)

                    print("No of data points read for shard 1:", len([data for data in data_list if data["Stud_id"] < 4096]))
                    print("No of data points read for shard 2:", len([data for data in data_list if data["Stud_id"] >= 4096 and data["Stud_id"] < 8192]))
                    print("No of data points read for shard 3:", len([data for data in data_list if data["Stud_id"] >= 8192 and data["Stud_id"] < 12288]))
                    print("No of data points read for shard 4:", len([data for data in data_list if data["Stud_id"] >= 12288 and data["Stud_id"] < 16384]))
                    print("No of data points read for shard 5:", len([data for data in data_list if data["Stud_id"] >= 16384 and data["Stud_id"] < 20480]))
                else:
                    print(f"Response message: {response_json.get('message', 'No message')}")
                # print("No of data points 
            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
        
            return response.status
    except Exception as e:
        print("Error:", e)
        return 500

async def write_shard(session, json_data):
    try:
        # print(f"Sending Write Request: {json_data}")
        # connector = aiohttp.TCPConnector(force_close=True)
        # async with aiohttp.ClientSession(connector=connector) as session:
        async with session.post(f'http://{ip_address}:{port}/write', json=json_data) as response:
            if response.status == 200:
                # print("JSON Request Successful")
                # print(await response.json())
                pass
            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
            return response.status
    except Exception as e:
        print("Error:", e)
        return 500

async def update_shard_entry(session, json_data):
    try:
        print(f"Sending Update Request: {json_data}")
        async with session.put(f'http://{ip_address}:{port}/update', json=json_data) as response:
            if response.status == 200:
                # print("JSON Request Successful")
                print(await response.json())
            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
            return response.status
    except Exception as e:
        print("Error:", e)
        return 500
    
async def delete_shard_entry(session, json_data):
    try:
        print(f"Sending Delete Request: {json_data}")
        async with session.delete(f'http://{ip_address}:{port}/del', json=json_data) as response:
            if response.status == 200:
                # print("JSON Request Successful")
                print(await response.json())
            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
            return response.status
    except Exception as e:
        print("Error:", e)
        return 500
        

async def send_requests(
        num_requests: int, 
        type: str = "write",
        args: Dict = None):
    try:
        async with aiohttp.ClientSession() as session:
            tasks = []
            if type == "write":
                ids = list(random.sample(range(1, STUD_ID_MAX), num_requests))
                num_per_req = num_requests//100
                
                # 100 reqs, each of num_per_req data points
                # tasks = [write_shard(session, {"data": [{"Stud_id": ids[i*num_per_req+j], "Stud_name": generate_random_string(), "Stud_marks": random.randint(0, 100)} for j in range(num_per_req)]}) for i in range(100)]
                
                # num_requests reqs, with 1 data point each
                tasks = [write_shard(session, {"data": [{"Stud_id": id, "Stud_name": generate_random_string(), "Stud_marks": random.randint(0, 100)}]}) for id in ids]
                
                # FOR Loop
                # tasks = [{"data": [{"Stud_id": id, "Stud_name": generate_random_string(), "Stud_marks": random.randint(0, 100)}]} for id in ids]
                # for task in tasks:
                #     await write_shard(task)
            elif type == "read":
                for i in range(num_requests):
                    low,high = generate_random_range()
                    # low = 0
                    # high = STUD_ID_MAX
                    read_json = {
                        "Stud_id":{ "low": low, "high": high}
                    }
                    
                    tasks.append(read_shard(session, read_json))
                  
            elif type == "update":
                for i in range(num_requests):
                    stu_id = random.randint(1, STUD_ID_MAX)
                    # stu_id = 11732
                    stu_name = generate_random_string()
                    # stu_name = "S_ABCD"
                    stu_marks = random.randint(0, 100)
                    # stu_marks = 68
                    update_json = {
                        "Stud_id": stu_id,
                        "data": {"Stud_id": stu_id, "Stud_name": stu_name, "Stud_marks": stu_marks}
                        # "data": {"Stud_id": stu_id, "Stud_marks": stu_marks}
                        # "data": {"Stud_id": stu_id, "Stud_name": stu_name}
                        # "data": {"Stud_id": stu_id}
                    }
                    tasks.append(update_shard_entry(session, update_json))
                    
            elif type == "delete":
                for i in range(num_requests):
                    stu_id = random.randint(1, STUD_ID_MAX)
                    # stu_id = 435
                    delete_json = {
                        "Stud_id": stu_id
                    }
                    tasks.append(delete_shard_entry(session, delete_json))
            # await asyncio.gather(*tasks)
                    
            elif type == "init":
                json_data = create_init_json(args)
                tasks.append(init(session, json_data))

            elif type == "status":
                tasks.append(status(session))

            statuses = await asyncio.gather(*tasks)
        
        # print(f"Success: {num_requests} {type} requests to the load balancer sent successfully.")
        # print(f"\n")
        # check the no of successful requests and failed requests
        num_success = statuses.count(200)
        num_failed =  len(statuses) - num_success
        print(f"No of successful requests: {num_success}/{num_requests}")
        print(f"No of failed requests: {num_failed}/{num_requests}")
 
    except Exception as e:
        print(f"Error: An exception occurred while sending multiple {type} requests: {e}")
