"""Exercise real SDK payment retries with caller-owned streaming sources."""
import asyncio
import base64
import json
import sys
import tempfile

import aiohttp

from livepeer_gateway import LivepeerHTTPError, StreamPayload
from livepeer_gateway.live_runner import LiveRunnerInstance, LiveRunnerPriceInfo, call_runner


async def main():
    spec = json.load(sys.stdin)
    raw = spec["runner"]
    price = raw["price_info"]
    runner = LiveRunnerInstance(
        url=raw["url"], app=raw["app"], runner_id="", mode=raw["mode"],
        orchestrator_url=spec["orchestrator"], raw=raw,
        price_info=LiveRunnerPriceInfo(price=float(price["price"]), currency=price["currency"], unit=price["unit"]),
    )
    data = base64.b64decode(spec["payload"])
    reads = 0
    payments = 0

    async with aiohttp.ClientSession() as control:
        async def payment_ready(session):
            nonlocal payments
            assert session.manifest_id
            assert reads == 0, "402 challenge consumed the upload source"
            payments += 1
            if not spec["reject"]:
                async with control.post(spec["control"] + "/arm") as response:
                    assert response.status == 204

        async def chunks():
            nonlocal reads
            for offset in range(0, len(data), 16384):
                reads += 1
                async with control.post(spec["control"] + "/source") as response:
                    assert response.status == 204, await response.text()
                yield data[offset:offset + 16384]

        class FileSource:
            def __init__(self, file):
                self.file = file

            def seekable(self):
                return True

            @property
            def closed(self):
                return self.file.closed

            def close(self):
                self.file.close()

            def tell(self):
                return self.file.tell()

            def seek(self, position):
                return self.file.seek(position)

            def read(self, size):
                nonlocal reads
                # File reads run in the SDK's I/O worker. Check the same gate
                # from the event loop before touching the file.
                future = asyncio.run_coroutine_threadsafe(check_read(), loop)
                future.result(timeout=10)
                reads += 1
                return self.file.read(size)

        async def check_read():
            async with control.post(spec["control"] + "/source") as response:
                assert response.status == 204, await response.text()

        loop = asyncio.get_running_loop()
        with tempfile.TemporaryFile() as file:
            file.write(b"skip" + data)
            file.seek(4)
            source = FileSource(file) if spec["source"] == "file" else chunks()
            try:
                try:
                    result = await call_runner(
                        runner=runner, signer_url=spec["signer"],
                        payload=StreamPayload(source=source, expect100=True, retry_buffer=False),
                        content_type="application/octet-stream", timeout=10,
                        on_payment_session=payment_ready,
                    )
                except LivepeerHTTPError as error:
                    assert spec["reject"] and error.status_code == 403, error
                    assert reads == 0, "rejected payment consumed the upload source"
                    assert file.tell() == 4
                else:
                    assert not spec["reject"], "invalid payment was accepted"
                    assert result.data == spec["expected"], result.data
                    assert reads > 1, reads
                assert payments == 1, payments
                assert not file.closed, "SDK closed the caller's file"
            finally:
                print(json.dumps({"source": spec["source"], "reads": reads, "payments": payments, "bytes": len(data)}), flush=True)
                if spec["source"] == "iterator":
                    await source.aclose()


asyncio.run(main())
