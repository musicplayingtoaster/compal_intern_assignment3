import asyncio, sys, logging
from typing import AsyncGenerator, Generator
from psycopg import Connection, AsyncConnection
from psycopg_pool import ConnectionPool, AsyncConnectionPool
import redis, redis.asyncio as aioredis
from resources.connections import connection_params_db, connection_params_redis_cache
from . import database

from contextlib import asynccontextmanager
from fastapi import FastAPI

postgres_sync_pool: ConnectionPool = None
postgres_async_pool: AsyncConnectionPool = None
rediscache_sync_client: redis.Redis | None = None
rediscache_async_client: aioredis.Redis | None = None

logger = logging.getLogger("uvicorn.error")


def create_lifespan(rabbitmq_listener):
    logger.info("Starting [create_lifespan] function")

    @asynccontextmanager
    async def lifespan(app:FastAPI):
        print("lifespan stuff started!", flush=True)
        sys.stdout.flush()

        try:
            global postgres_sync_pool, postgres_async_pool, rediscache_sync_client, rediscache_async_client
            postgres_sync_pool = ConnectionPool(kwargs=connection_params_db, 
                                                open=False, 
                                                check=ConnectionPool.check_connection)
            postgres_async_pool = AsyncConnectionPool(kwargs=connection_params_db, 
                                                      open=False,
                                                      check=ConnectionPool.check_connection)
            
            sync_pool = redis.ConnectionPool(**connection_params_redis_cache)
            async_pool = aioredis.ConnectionPool(**connection_params_redis_cache)

            rediscache_sync_client = redis.Redis(connection_pool=sync_pool)
            rediscache_async_client = aioredis.Redis(connection_pool=async_pool)

            postgres_sync_pool.open()
            await postgres_async_pool.open()

            with next(get_pg_sync_conn()) as sync_conn:
                database.init_todo_list(conn_db=sync_conn)
                print("Database Initizalization Attempted!")
                logger.info("Database Initialization Attempted!")
            
            listener_task = asyncio.create_task(rabbitmq_listener())
            print("Listener task started!")
            logger.info("Listener task ready!")

            yield
        
        except Exception as e:
            print(f"CRITICAL LIFESPAN ERROR CAUGHT: {str(e)}")
            logger.error(f"CRITICAL LIFESPAN ERROR CAUGHT: {str(e)}", exc_info=True)
            sys.exit(1) 

        finally:
            logger.info("Cleaning up lifespan resources...")
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass

            if postgres_sync_pool:
                postgres_sync_pool.close()
            if postgres_async_pool:
                await postgres_async_pool.close()
            if rediscache_sync_client:
                rediscache_sync_client.close()
            if rediscache_async_client:
                await rediscache_async_client.aclose()
    
    logger.info("Returning Lifespan")
    return lifespan

# Stuff Down Here = Dependency Injection Functions
# Example for connection (conn: Connection = Depends(get_pg_sync_conn))
def get_pg_sync_conn() -> Generator[Connection, None, None]:
    with postgres_sync_pool.connection() as connection:
        yield connection

async def get_pg_async_conn() -> AsyncGenerator[AsyncConnection, None]:
    async with postgres_async_pool.connection() as connection:
        yield connection

def get_rdcache_sync_conn() -> redis.Redis:
    if rediscache_sync_client is None:
        raise RuntimeError("Sync Redis Cache client is not initialized")
    return rediscache_sync_client

# Example again but for redis (async def get_value(key: str, redis: aioredis.Redis = Depends(get_redis)):)
async def get_rdcache_async_conn() -> aioredis.Redis:
    if rediscache_async_client is None:
        raise RuntimeError("Async Redis Cache client is not initialized")
    return rediscache_async_client