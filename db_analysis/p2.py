import random
import time
import asyncio
import argparse
from analysis import send_requests

random.seed(42)

'''
    TASK 2:

     - Increase shard replicas to 7.
     - Compute speeds for 10000 read and 10000 write requests.

'''

SHARD_SIZE = 4096

parser = argparse.ArgumentParser()

parser.add_argument("--type", type=str, required=True, default="read")
parser.add_argument("--nreqs", type=int, default=1)
parser.add_argument("--servers", type=int, default=6)
parser.add_argument("--shards", type=int, default=4)
parser.add_argument("--replicas", type=int, default=3)

args = parser.parse_args()

# def create_consistent_hashmap(servers: int,
#                             shards: int,
#                             replicas: int) -> Dict:
    
#     replicas_per_server = shards * replicas // servers
#     server_shard_map = {}

#     for i in range(servers):
#         start = (i*replicas_per_server)%shards
#         end = ((i+1)*replicas_per_server)%shards
#         if start > end:
#             idxs_per_server = list(range(start, shards)) + list(range(end))
#         else:
#             idxs_per_server = list(range(start, end))
#         idxs_per_server = [idx+1 for idx in idxs_per_server]
#         shard_ids_per_server = [f"sh{idx}" for idx in idxs_per_server]
#         server_shard_map[f"Server{i}"] = shard_ids_per_server

#     return server_shard_map


# def create_init_json(args: Dict) -> Dict:
#     servers = int(args.servers)
#     shards = int(args.shards)
#     replicas = int(args.replicas)

#     return {
#         "N": servers,
#         "schema": {
#                     "columns":["Stud_id","Stud_name","Stud_marks"],
#                     "dtypes":["Number","String","String"]
#                 },
#         "shards": [
#             {
#                 "Stud_id_low": SHARD_SIZE * i,
#                 "Shard_id": f"sh{i+1}",
#                 "Shard_size": SHARD_SIZE
#             }
#             for i in range(shards)
#         ],
#         "servers": create_consistent_hashmap(servers, shards, replicas)
#     }

if __name__ == '__main__':
    
    request_type = args.type
    num_requests = args.nreqs

    try:   
            
        print(f"Request Type: {request_type}")
         
        if request_type == "init":
            start = time.time()
            asyncio.run(send_requests(1,"init",args))
            end = time.time()
            print(f"Time taken to send init request: {end-start} seconds")

        if request_type == "status":
            start = time.time()
            asyncio.run(send_requests(1,"status"))
            end = time.time()
            print(f"Time taken to send status request: {end-start} seconds")
            
        elif request_type == "write":    
            start = time.time()
            asyncio.run(send_requests(num_requests,"write"))
            end = time.time()
            print(f"Time taken to send {num_requests} requests: {end-start} seconds")

        elif request_type == "read":
            start = time.time()
            asyncio.run(send_requests(num_requests,"read"))
            end = time.time()
            print(f"Time taken to send {num_requests} requests: {end-start} seconds")
        
        elif request_type == "update":
            start = time.time()
            asyncio.run(send_requests(num_requests,"update"))
            end = time.time()
            print(f"Time taken to send {num_requests} requests: {end-start} seconds")
            
        elif request_type == "delete":
            start = time.time()
            asyncio.run(send_requests(num_requests,"delete"))
            end = time.time()
            print(f"Time taken to send {num_requests} requests: {end-start} seconds")
        
    except Exception as e:
        print(f"Error: An exception occurred in overall run: {e}")
