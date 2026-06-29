from ..resources.todo_model import Todo
import psycopg
from psycopg import Connection, AsyncConnection
import redis
import redis.asyncio as aioredis
from redis.exceptions import RedisError
from dotenv import load_dotenv
import re

# I might need to change this name now that redis is in here

load_dotenv()

CACHETTL = 300
latest_cache_key:str = None

# connection_params_db = {
#     "host": os.environ.get('DB_HOST'), 
#     "port": os.environ.get('DB_PORT'),
#     "dbname": os.environ.get('DB_DATABASE'),
#     "user": os.environ.get('DB_USER'),
#     "password": os.environ.get('DB_PASSWORD'),
# }

# connection_params_cache = {
#     "host": os.environ.get('RDC_HOST'),
#     "port": os.environ.get('RDC_PORT'),
#     "db": 0,
#     "decode_responses": True,
# }

# def init_todo_list(conn_db: Connection = Depends(helper.get_pg_sync_conn)) -> None:
#     try:
#         #with helper.get_pg_sync_conn() as connection_db:
#         with conn_db.cursor() as cursor:
#             cursor.execute('''
#                 CREATE TABLE IF NOT EXISTS todo_list (
#                     id SERIAL PRIMARY KEY,
#                     todo TEXT NOT NULL,
#                     resolved INTEGER NOT NULL DEFAULT 0
#                 )
#             ''')
#     except psycopg.OperationalError as e:
#         print("Failed to open database:", e, "(in short, you failed lmao.)")


async def retrieve_latest_todo(conn_db: AsyncConnection, conn_cache: aioredis.Redis) -> tuple:
    try:
        global latest_cache_key
        if latest_cache_key != None:
            #async with helper.get_rdcache_async_conn() as connection_cache:
            todo = await conn_cache.get(latest_cache_key)
            print("retrieved from cache!")
            if todo != None:
                return tuple(Todo.model_validate_json(todo).model_dump().values())
        
        #async with await helper.get_pg_async_conn() as connection_db:
        async with conn_db.cursor() as cursor:
            await cursor.execute("SELECT * FROM todo_list ORDER BY id DESC LIMIT 1")
            latest_row = await cursor.fetchone()
            print("retrieved from db!")
            if latest_row != None:
                return latest_row
        
    except (psycopg.OperationalError, RedisError) as e:
        print("Failed to open database and/or cache:", e)

def get_numeric_sort_key(key):
    match = re.search(r'\d+', key[5:])
    return int(match.group()) if match else 0

def retrieve_all_todos(conn_db: Connection, conn_cache: redis.Redis) -> tuple:
    try:
        todos = []
        cached_primary_keys = []
        # with helper.get_rdcache_sync_conn() as connection_cache:
        unordered_keys = list(conn_cache.scan_iter(match='todo:*'))
        sorted_keys = sorted(unordered_keys, key=get_numeric_sort_key)

        pipeline = conn_cache.pipeline()
        for key in sorted_keys:
            pipeline.get(key)
        cached_values = pipeline.execute()

        for key, raw_json in zip(sorted_keys, cached_values):
            if not raw_json:
                continue
            try:
                todos.append(tuple(Todo.model_validate_json(raw_json).model_dump().values()))
                cached_primary_keys.append(re.sub(r'\D+', '', key[5:]))
            except Exception:
                continue
        
        print(f"retrieved from cache: {cached_primary_keys}")

        # with helper.get_pg_sync_conn() as connection_db:
        with conn_db.cursor() as cursor:
            cursor.execute("SELECT * FROM todo_list WHERE id != ALL(%s)ORDER BY id", (cached_primary_keys,))
            all_rows = cursor.fetchall()
            todos = all_rows + todos

        print(todos)
        return todos
    except psycopg.OperationalError as e:
        print("Failed to open database and/or cache:", e, "(in short, you failed lmao.)")

async def add_todo(todo:Todo, conn_db: AsyncConnection, conn_cache: aioredis.Redis):
    try:
        global latest_cache_key
        async with conn_db.cursor() as cursor:
            await cursor.execute("INSERT INTO todo_list (todo) VALUES (%(todo)s) RETURNING *", todo.model_dump())
            await conn_db.commit()
            latest_todo = await cursor.fetchone()
        
        latest_cache_key = f"todo:{latest_todo[0]}"
        todo.id = latest_todo[0]
        await conn_cache.setex(latest_cache_key, CACHETTL, todo.model_dump_json()) 
    except psycopg.OperationalError as e:
        print("Failed to open database and/or cache:", e, "(in short, you failed lmao.)")

async def remove_todo(primary_key:int, conn_db: AsyncConnection, conn_cache: aioredis.Redis) -> tuple:
    try:
        #with helper.get_pg_sync_conn() as connection_db, helper.get_rdcache_sync_conn() as connection_cache:
        async with conn_db.cursor() as cursor:
            await cursor.execute("DELETE FROM todo_list WHERE id = %s", (primary_key,))
            await conn_db.commit()

        await conn_cache.delete(f"todo:{primary_key}")
    except psycopg.OperationalError as e:
        print("Failed to open database and/or cache:", e, "(in short, you failed lmao.)")

async def update_todo(primary_key:int, _resolved:int, conn_db: AsyncConnection, conn_cache: aioredis.Redis) -> tuple:
    try:
        #with helper.get_pg_sync_conn() as connection_db, helper.get_rdcache_sync_conn() as connection_cache:
        async with conn_db.cursor() as cursor:
            await cursor.execute("UPDATE todo_list SET resolved = %s WHERE id = %s RETURNING todo", (_resolved, primary_key,))
            await conn_db.commit()

            updated_todo = await cursor.fetchone()
            conn_cache.setex(f"todo:{primary_key}",     
                             CACHETTL, 
                             Todo(id=primary_key, todo=updated_todo[0], resolved=_resolved).model_dump_json())
    except psycopg.OperationalError as e:
        print("Failed to open database and/or cache:", e, "(in short, you failed lmao.)")
