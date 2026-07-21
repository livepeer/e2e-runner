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
        label=sys.argv[6],
        capacity=int(sys.argv[7]),
        metadata=sys.argv[8],
        price=10,
        currency="usd",
        unit="hour",
        auto_detect_gpu=False,
        heartbeat_interval_s=60,
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
