import asyncio
import json
import sys

import aiohttp
from livepeer_gateway.live_runner import call_runner
from livepeer_gateway.selection import runner_selector


async def main():
    spec = json.loads(sys.stdin.readline())
    async with await runner_selector(
        orchestrators=spec["orchestrators"], signer_url=spec["signer"],
        app=spec["app"], body={"prompt": "hello"}, timeout=8,
    ) as cursor:
        assert cursor.candidates, "no candidates discovered"
        assert cursor.candidates[0].orchestrator_url == spec["orchestrators"][0]
        print("discovered", flush=True)
        assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "continue"
        # A direct probe records the wire contract independently of selection.
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as client:
            for headers in ({}, {"Livepeer-Payer-Address": spec["payer"]}):
                async with client.post(cursor.candidates[0].url, headers=headers) as response:
                    body = await response.json()
                    assert response.status == 503, (response.status, body)
                    assert "payment_params" not in body and "manifest_id" not in body, body
        try:
            result = await cursor.next()
        except Exception as error:
            print(json.dumps({"status": "failed", "mode": spec["mode"],
                              "candidates": [r.orchestrator_url for r in cursor.candidates],
                              "error": str(error),
                              "rejections": [{"url": r.url, "reason": r.reason} for r in cursor.rejections]}), flush=True)
            raise
        assert result.runner.orchestrator_url == spec["orchestrators"][1], result.runner_url
        assert len(cursor.rejections) == 1, cursor.rejections
        assert cursor.rejections[0].url == cursor.candidates[0].url
        assert "503" in cursor.rejections[0].reason, cursor.rejections[0]
        if spec["mode"] == "persistent":
            result = await call_runner(result.data["app_url"], payload={"prompt": "hello"}, timeout=8)
        assert result.data == {"paid": True, "prompt": "hello"}, result.data
        print(json.dumps({"status": "passed", "mode": spec["mode"], "busy_status": 503,
                          "failed_candidates": len(cursor.rejections), "selected_orchestrator": spec["orchestrators"][1]}))


asyncio.run(main())
