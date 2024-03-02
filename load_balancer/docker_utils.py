import os
import asyncio

def spawn_server_cntnr(hostname):
    try:
        # Spawn a new container with the environment variable
        res = os.popen(f'sudo docker run --name {hostname} --network mynet --network-alias {hostname} -e SERVER_ID={hostname} -d -p 5000 server_img:latest').read()
        print(res)
        if res is None or len(res) == 0:
            if res is None:
                print("Finally, NONE!")
            print(f"Error: Unable to start container with ID {hostname}.", flush=True)
            return False
        else:
            print(f"Success: Container with ID {hostname} started successfully.", flush=True)
            return True
    
    except Exception as e:
        print(f"Error: An exception occurred during container spawn: {e}", flush=True)
        return False

# async def spawn_server_cntnr(hostname):
#     try:
#         # Spawn a new container with the environment variable
#         cmd = f'sudo docker run --name {hostname} --network mynet --network-alias {hostname} -e SERVER_ID={hostname} -d -p 5000 server_img:latest'

#         process = await asyncio.create_subprocess_shell(
#             cmd,
#             stdout=asyncio.subprocess.PIPE,
#             stderr=asyncio.subprocess.PIPE
#         )

#         stdout, stderr = await process.communicate()

#         if process.returncode != 0:
#             print(f"Error: Unable to start container with ID {hostname}.")
#             print(f"stdout: {stdout.decode()}")
#             print(f"stderr: {stderr.decode()}")
#             return False
#         else:
#             print(f"Success: Container with ID {hostname} started successfully.")
#             return True
#     except Exception as e:
#         print(f"Exception: {e}")
#         return False


def kill_server_cntnr(hostname):
    try:
        # Remove the container
        os.system(f"sudo docker stop {hostname} && sudo docker rm {hostname}")
        print(f"Success: Container with ID {hostname} stopped and removed.")
        return True
    
    except Exception as e:
        print(f"Error: An exception occurred during container removal: {e}")
        return False

