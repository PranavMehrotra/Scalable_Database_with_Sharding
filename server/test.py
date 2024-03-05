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
                    print("Error in JSON Request")
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
        "shards": ["sh3", "sh4"]
    }
    await send_json_request(json_data)

if __name__ == '__main__':
    asyncio.run(main())

