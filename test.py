import asyncio
from enum import verify
import http
import aiohttp
from vkbottle.api import API
from aiohttp import TCPConnector
import certifi
import ssl

ssl_ctx = ssl.create_default_context(cafile="certificates/vk-com-chain.cer")

api = API(
    "vk1.a.UBSol2yW1wcHMx5y_3k3Zv0GYktfCd3RKplDi4AFgRoHuNa67fvbSI_r84epyk-4DeKtrk2mSoQxGRLZtjnuuYXr4YEjRG1yv5uNYy5biSxne-Xkqealp6oeGsRK0nm3pbe4x7qfIMBKf2AqkA4eEI5l_GZ96CdvEnkftdrLCemuwRceRnerEsj3zYPXOXzik0k2JG9pjsbUhc46mTia9A",
)


async def get_wall_posts(domain: str, count: int = 10, offset: int = 0):
    """
    Fetches posts from a VK wall by domain.

    :param domain: The domain of the group or user.
    :param count: Number of posts to fetch.
    :param offset: Offset for pagination.
    :return: List of WallWallpostFull objects.
    """
    conn = aiohttp.TCPConnector(ssl=ssl_ctx)
    return await api.wall.get(domain=domain, count=count, offset=offset, connector=conn)


if __name__ == "__main__":
    print(asyncio.run(get_wall_posts("http://vk.com/boardgames_mipt")))
