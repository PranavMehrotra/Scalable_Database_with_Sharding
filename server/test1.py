import asyncio
import aiohttp
import datetime
from helper import SQLHandler


sql_handler = SQLHandler()

try:
    sql_handler.connect()
    res = sql_handler.query("USE server_database;")
    print(res)
    res,status = sql_handler.query(f"SELECT * FROM sh3 WHERE Stud_id=1;")
    print(res==[])
except Exception as e:
    print(e)