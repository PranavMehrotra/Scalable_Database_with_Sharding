import asyncio
import aiohttp
import datetime

async def send_json_request(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post('http://127.0.0.1:5000/config', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text())
    except Exception as e:
        print("Error:", e)


async def send_copy(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get('http://127.0.0.1:5000/copy', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text())
    except Exception as e:
        print("Error:", e)

async def read_shard(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post('http://127.0.0.1:5000/read', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text())
    except Exception as e:
        print("Error:", e)

async def update_shard(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put('http://127.0.0.1:5000/update', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text())
    except Exception as e:
        print("Error:", e)

async def delete_shard(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.delete('http://127.0.0.1:5000/del', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text())
    except Exception as e:
        print("Error:", e)


async def write_shard(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post('http://127.0.0.1:5000/write', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text())
    except Exception as e:
        print("Error:", e)

async def main():
    # Example JSON data
    json_data = {
        "schema": {
            "columns": ["Stud_id", "Stud_name", "Stud_marks"],
            "dtypes": ["Number", "String", "String"]
        },
        "shards": ["sh1"]
    }
    await send_json_request(json_data)
    # json_data = {
    #     "shards": ["sh3", "sh4"]
    # }
    # await send_copy(json_data)
    # json_data = {
    #     "shard": "sh3",
    #     "Stud_id":{ "low": 12, "high": 13}
    # }
    # await read_shard(json_data)

    # json_data = {
    #     "shard": "sh3",
    #     "Stud_id":{ "low": 40, "high": 90}
    # }
    # await read_shard(json_data)

    # json_data = {
    #     "shard":"sh1",
    #     "Stud_id":65,
    #     "data": {"Stud_id":65,"Stud_name":'saransh_sharma',"Stud_marks":300} 
    #     }
    # await update_shard(json_data)

    # json_data = {
    #     "shard":"sh3",
    #     "Stud_id":3,
    #     }
    # await delete_shard(json_data)

    # json_data = {
    # "shard":"sh2",
    # "curr_idx": 507,
    # "data": [{"Stud_id":2255,"Stud_name":'sharma',"Stud_marks":27},{"Stud_id":2252,"Stud_name":'sharm',"Stud_marks":47},{"Stud_id":225,"Stud_name":'shar',"Stud_marks":127}] 
    # }
    # await write_shard(json_data)
if __name__ == '__main__':
    asyncio.run(main())

