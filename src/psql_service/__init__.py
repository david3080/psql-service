from . import server
import asyncio

def main():
    # サーバーのメイン関数を呼び出す
    asyncio.run(server.main())

__all__ = ["main", "server"]
