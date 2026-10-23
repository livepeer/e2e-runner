import asyncio
import json
import sys

from livepeer_gateway.live_runner import register_runner


async def main():
    reg = await register_runner(
        sys.argv[1],
        secret=sys.argv[2],
        runner_url=sys.argv[3],
        app=sys.argv[4],
        mode=sys.argv[5],
        proxy=sys.argv[9].lower() == "true",
        capacity=int(sys.argv[7]),
        metadata=sys.argv[8],
        price=float(sys.argv[10]) if len(sys.argv) > 10 else 10,
        currency="usd",
        unit=sys.argv[11] if len(sys.argv) > 11 else "hour",
        auto_detect_gpu=False,
    )
    print(
        json.dumps(
            {
                "runner_id": reg.runner_id,
                "orchestrator_url": reg.orchestrator_url,
                "heartbeat_interval_s": reg.heartbeat_interval_s,
            }
        ),
        flush=True,
    )
    await asyncio.get_running_loop().run_in_executor(None, sys.stdin.buffer.read)
    await reg.close()


asyncio.run(main())
