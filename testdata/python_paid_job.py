import asyncio
import json
import sys

from livepeer_gateway.live_runner import LiveRunnerInstance, LiveRunnerPriceInfo, call_runner


async def main():
    spec = json.load(sys.stdin)
    raw = spec["runner"]
    url = raw["url"]
    price = raw["price_info"]
    runner = LiveRunnerInstance(
        url=url, app=raw["app"], runner_id="", mode=raw["mode"],
        orchestrator_url=spec["orchestrator"], raw=raw,
        price_info=LiveRunnerPriceInfo(price=float(price["price"]), currency=price["currency"], unit=price["unit"]),
    )
    # For persistent runners the discovery URL reserves the session. The SDK
    # reservation API owns ongoing funding and the application URL.
    if runner.mode == "persistent":
        reservation = await call_runner(runner=runner, signer_url=spec["signer"], signer_headers=spec["headers"], timeout=10)
        session = reservation.data
        assert session["session_id"]
        result = await call_runner(session["app_url"], payload={"prompt": "hello"}, timeout=10)
        assert result.data == {"paid": True, "prompt": "hello"}, result.data
        # Runner unregister owns final cleanup of the SDK reservation.
    else:
        result = await call_runner(runner=runner, signer_url=spec["signer"], signer_headers=spec["headers"], payload={"prompt": "hello"}, timeout=10)
        assert result.data == {"paid": True, "prompt": "hello"}, result.data
    print("paid job passed")


asyncio.run(main())
