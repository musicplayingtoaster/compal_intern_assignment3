import asyncio
from typing import AsyncGenerator, Generator
from psycopg import Connection, AsyncConnection
from psycopg_pool import ConnectionPool, AsyncConnectionPool
import redis, redis.asyncio as aioredis
from ..resources.connections import connection_params_db, connection_params_redis_cache

from contextlib import contextmanager, asynccontextmanager

class DatabaseAccessor:
    def __init__(self, listener):
        self.postgres_sync_pool = None
        self.postgres_async_pool = None
        self.rediscache_sync_client = None
        self.rediscache_async_client = None

        self.listener_task = None
        self.listener = listener

    async def __aenter__(self):
        self.postgres_sync_pool = ConnectionPool(kwargs=connection_params_db, open=False)
        self.postgres_async_pool = AsyncConnectionPool(kwargs=connection_params_db, open=False)

        sync_pool = redis.ConnectionPool(**connection_params_redis_cache)
        async_pool = aioredis.ConnectionPool(**connection_params_redis_cache)

        self.rediscache_sync_client = redis.Redis(connection_pool=sync_pool)
        self.rediscache_async_client = aioredis.Redis(connection_pool=async_pool)

        self.postgres_sync_pool.open()
        await self.postgres_async_pool.open()

        self.listener_task = asyncio.create_task(self.listener())

        return self

    async def __aexit__(self, exc_type, exc, tb):
        print("Cleaning up resources...")

        if self.listener_task:
            self.listener_task.cancel()
            try:
                await self.listener_task
            except asyncio.CancelledError:
                pass

        if self.postgres_sync_pool:
            self.postgres_sync_pool.close()
        if self.postgres_async_pool:
            await self.postgres_async_pool.close()
        if self.rediscache_sync_client:
            self.rediscache_sync_client.close()
        if self.rediscache_async_client:
            await self.rediscache_async_client.aclose()

        print("Resources closed successfully.")

            

    @contextmanager
    def get_pg_sync_conn(self) -> Generator[Connection, None, None]:
        if self.postgres_sync_pool is None:
            raise RuntimeError("Sync Postgres Pool is not initialized")
        with self.postgres_sync_pool.connection() as connection:
            yield connection

    @asynccontextmanager
    async def get_pg_async_conn(self) -> AsyncGenerator[AsyncConnection, None]:
        if self.postgres_async_pool is None:
            raise RuntimeError("Async Postgres Pool is not initialized")
        async with self.postgres_async_pool.connection() as connection:
            yield connection


    def get_rdcache_sync_conn(self) -> redis.Redis:
        if self.rediscache_sync_client is None:
            raise RuntimeError("Sync Redis Cache client is not initialized")
        return self.rediscache_sync_client


    async def get_rdcache_async_conn(self) -> aioredis.Redis:
        if self.rediscache_async_client is None:
            raise RuntimeError("Async Redis Cache client is not initialized")
        return self.rediscache_async_client