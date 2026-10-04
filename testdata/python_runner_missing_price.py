import asyncio
import sys

from livepeer_gateway.errors import LivepeerGatewayError
from livepeer_gateway.live_runner import register_runner


async def main():
    try:
        reg = await register_runner(
            sys.argv[1],
            secret=sys.argv[2],
            runner_url=sys.argv[3],
            app=sys.argv[4],
            mode=sys.argv[5],
            label=sys.argv[6],
            capacity=1,
            price=0,
            auto_detect_gpu=False,
            heartbeat_interval_s=60,
        )
    except LivepeerGatewayError as exc:
        msg = str(exc)
        if getattr(exc, "status", getattr(exc, "status_code", None)) == 400 or "price" in msg.lower():
            return
        raise
    else:
        await reg.close()
        raise AssertionError("register_runner unexpectedly succeeded without positive price_info")


asyncio.run(main())
