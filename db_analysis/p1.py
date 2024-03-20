import sys
import random
import time
import asyncio
from analysis import send_requests

random.seed(42)

'''
    TASK 1:

        Compute speeds for 10000 read and 10000 write requests.

'''

if __name__ == '__main__':
  
    NUM_WRITE_REQUESTS = 10000
    NUM_READ_REQUESTS = 10000
    NUM_UPDATE_REQUESTS = 1
    NUM_DELETE_REQUESTS = 1
    request_type = "write"
    
    try:   
        
        if len(sys.argv) > 1:
            request_type = sys.argv[1]
            
        print(f"Request Type: {request_type}")
         
        if request_type == "write":    
            if len(sys.argv) > 2:
                NUM_WRITE_REQUESTS = int(sys.argv[2])
            start = time.time()
            asyncio.run(send_requests(NUM_WRITE_REQUESTS,"write"))
            end = time.time()
            print(f"Time taken to send {NUM_WRITE_REQUESTS} requests: {end-start} seconds")

        elif request_type == "read":
            if len(sys.argv) > 2:
                NUM_READ_REQUESTS = int(sys.argv[2])
            start = time.time()
            asyncio.run(send_requests(NUM_READ_REQUESTS,"read"))
            end = time.time()
            print(f"Time taken to send {NUM_READ_REQUESTS} requests: {end-start} seconds")
        
        elif request_type == "update":
            if len(sys.argv) > 2:
                NUM_UPDATE_REQUESTS = int(sys.argv[2])
            start = time.time()
            asyncio.run(send_requests(NUM_UPDATE_REQUESTS,"update"))
            end = time.time()
            print(f"Time taken to send {NUM_UPDATE_REQUESTS} requests: {end-start} seconds")
            
        elif request_type == "delete":
            if len(sys.argv) > 2:
                NUM_DELETE_REQUESTS = int(sys.argv[2])
            start = time.time()
            asyncio.run(send_requests(NUM_DELETE_REQUESTS,"delete"))
            end = time.time()
            print(f"Time taken to send {NUM_DELETE_REQUESTS} requests: {end-start} seconds")
        
    except Exception as e:
        print(f"Error: An exception occurred in overall run: {e}")