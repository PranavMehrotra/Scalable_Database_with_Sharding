import threading
import sys
import os
import requests
import time
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from load_balancer import LoadBalancer
from docker_utils import kill_server_cntnr

HEARTBEAT_INTERVAL = 0.2
SEND_FIRST_HEARTBEAT_AFTER = 0.5

class HeartBeat(threading.Thread):
    def __init__(self, lb: LoadBalancer, server_name, server_port=5000):
        super(HeartBeat, self).__init__()
        self._lb = lb
        self._server_name = server_name
        self._server_port = server_port
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def run(self):
        lb = self._lb
        server_name = self._server_name
        server_port = self._server_port
        print("heartbeat: Heartbeat thread started for server: ", server_name, flush=True)
        time.sleep(SEND_FIRST_HEARTBEAT_AFTER)
        cntr = 0
        while True:
            # Check if the thread is stopped
            if self.stopped():
                print("heartbeat: Stopping heartbeat thread for server: ", server_name)
                return
            # print("heartbeat: Starting a session!")
            # with aiohttp.ClientSession() as session:
            # print("heartbeat: Session started!")
            
            try:
                # with session.get(f'http://{server_name}:{server_port}/heartbeat') as response:
                    # print("heartbeat: Connected to server, Response received!")
                    # if response.status != 200 and {await response.text()}['message'] != "ok":
                    
                    ## To-Do: Check for timeout also
                response = requests.get(f'http://{server_name}:{server_port}/heartbeat', timeout=0.1)
                if response.status_code != 200 and response.status_code != 400:
                    cntr += 1
                    if cntr >= 2:
                        # Check if the thread is stopped
                        if self.stopped():
                            print("heartbeat: Stopping heartbeat thread for server: ", server_name)
                            # session.close()
                            return
                        print(f"heartbeat: Server {server_name} is down!")
                        print(f"heartbeat: Spawning a new server: {server_name}!")
                        cntr = 0
                        
                        #remove server from
                        lb.remove_servers(1, [server_name])
                        kill_server_cntnr(server_name)
                        
                        # reinstantiate an image of the server
                        lb.add_servers(1, [server_name])
                        
                        # print("heartbeat: Closing session!")
                        # await session.close()
                        
                        # break
                else :
                    cntr = 0

            # except aiohttp.client_exceptions.ClientConnectorError as e:
            except Exception as e: # this is better as it is more generic and will catch all exceptions

                cntr += 1
                if cntr >= 2:
                    print(f"heartbeat: Could not connect to server {server_name} due to {str(e.__class__.__name__)}")
                    print(f"heartbeat: Error: {e}")
                # Check if the thread is stopped
                    if self.stopped():
                        print("heartbeat: Stopping heartbeat thread for server: ", server_name)
                        # session.close()
                        return
                    print(f"heartbeat: Server {server_name} is down!")
                    print(f"heartbeat: Spawning a new server: {server_name}!")
                    cntr = 0
                    
                    #remove server from the loadbalancer
                    lb.remove_servers(1, [server_name])

                    try:
                        kill_server_cntnr(server_name)
                    except Exception as e:
                        print(f"heartbeat: could not kill server {server_name}\nError: {e}")
                    
                    # reinstantiate an image of the server
                    lb.add_servers(1, [server_name])
                
                # await session.close()
                # print("heartbeat: Closing session!")
                
                # break

            # print("heartbeat: Closing session and sleeping!")
            # session.close()
            time.sleep(HEARTBEAT_INTERVAL)

# async def check_heartbeat(Lb, server_name, server_ip="127.0.0.1", server_port=5000):
#     print("heartbeat: Heartbeat thread started for server: ", server_name)
    
#     cntr = 0
#     while True:
#         # print("heartbeat: Starting a session!")
#         async with aiohttp.ClientSession() as session:
#             print("heartbeat: Session started!")
            
#             try:
#                 async with session.get(f'http://{server_ip}:{server_port}/heartbeat') as response:
#                     # print("heartbeat: Connected to server, Response received!")
#                     # if response.status != 200 and {await response.text()}['message'] != "ok":
                    
#                     ## To-Do: Check for timeout also
                    
#                     if response.status != 200 and response.status != 400:
#                         cntr += 1
#                         if cntr >= 2:
#                             print(f"heartbeat: Server {server_name} is down!")
#                             print(f"heartbeat: Spawning a new server: {server_name}!")
#                             cntr = 0
                            
#                             #remove server from
#                             Lb.remove_servers(1, [server_name])
                            
#                             # reinstantiate an image of the server
#                             Lb.add_servers(1, [server_name])
                            
#                             # print("heartbeat: Closing session!")
#                             # await session.close()
                            
#                             # break
#                     else :
#                         cntr = 0       

#             # except aiohttp.client_exceptions.ClientConnectorError as e:
#             except Exception as e: # this is better as it is more generic and will catch all exceptions
#                 print(f"heartbeat: Could not connect to server {server_name} due to {str(e.__class__.__name__)}")
#                 cntr = 0 
                
#                 print(f"heartbeat: Server {server_name} is down!")
#                 print(f"heartbeat: Spawning a new server: {server_name}!")
#                 #remove server from
#                 num_rem, servers_rem, error = Lb.remove_servers(1, [server_name])
#                 print(f"heartbeat: Removed {num_rem} servers: {servers_rem}")
                
#                 # reinstantiate an image of the server
#                 num_add, servers_add, error = Lb.add_servers(1, [server_name])
#                 print(f"heartbeat: Added {num_add} servers: {servers_add}")
                
#                 # await session.close()
#                 # print("heartbeat: Closing session!")
                
#                 # break
        
#         print("heartbeat: Closing session and sleeping!") 
#         await session.close()              
                        
#         await asyncio.sleep(0.2)

#     return
    

        
    
    