import asyncio
from redis_listener import listen_to_redis

async def main():
    await listen_to_redis()

if __name__ == "__main__":
    asyncio.run(main())
