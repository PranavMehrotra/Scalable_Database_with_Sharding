import asyncio
import datetime
from helper import SQLHandler
from aiohttp import web



sql_handler = SQLHandler(max_retries=5)

try:
    print("Connecting to database...")
    error, status=sql_handler.connect()
    if status!=200:
        # raise Exception(error)
        print(error, flush=True)
    # res = sql_handler.query("USE server_database;")
    # print(res, flush=True)
    # res,status = sql_handler.query(f"SELECT * FROM sh3 WHERE Stud_id=1;")
    # print(res==[], flush=True)
# except Exception as e:
#     print(e)
finally:
    print("\nHi.", flush=True)
    app = web.Application()
    web.run_app(app, port=5000)