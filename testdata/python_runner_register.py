import asyncio
from contextlib import asynccontextmanager
import json
import sys

from aiohttp import web

from livepeer_gateway.live_runner import register_runner, stop_runner_session

spec = json.loads(sys.stdin.readline())


@asynccontextmanager
async def registration(runner_url):
    reg = await register_runner(runner_url=runner_url, auto_detect_gpu=False, **spec["registration"])
    try:
        print(json.dumps({"runner_id": reg.runner_id, "orchestrator_url": reg.orchestrator_url,
                          "heartbeat_interval_s": reg.heartbeat_interval_s}), flush=True)
        yield reg
    finally:
        await reg.close()


async def main():
    if spec["controls"]:
        await serve_session_controls()
        return
    async with registration(spec["runner_url"]):
        await asyncio.to_thread(sys.stdin.buffer.read)


async def serve_session_controls():
    reg = None

    async def channels(request):
        if reg is None:
            return web.Response(text="runner not registered", status=503)
        created = await reg.create_trickle_channels(
            request,
            [{"name": "events", "mime_type": "application/json"}],
        )
        deleted = await reg.remove_trickle_channels(request, ["events"])
        return web.json_response(
            {
                "runner_route": request.headers.get("Livepeer-Runner-Route", ""),
                "session_id": request.headers.get("Livepeer-Session-Id", ""),
                "control_url": request.headers.get("Livepeer-Session-Control", ""),
                "channel_name": created[0].get("channel_name", "") if created else "",
                "deleted": deleted,
            }
        )

    async def proxy(request):
        if reg is None:
            return web.Response(text="runner not registered", status=503)
        created = await reg.create_proxy(request, f"{runner_url}/proxy-target")
        return web.json_response(
            {
                "proxy_id": created.proxy_id,
                "proxy_url": created.url,
            }
        )

    async def proxy_default(request):
        if reg is None:
            return web.Response(text="runner not registered", status=503)
        created = await reg.create_proxy(request)
        return web.json_response(
            {
                "proxy_id": created.proxy_id,
                "proxy_url": created.url,
            }
        )

    async def proxy_target(request):
        body = await request.text()
        return web.json_response(
            {
                "method": request.method,
                "path": request.path,
                "query": request.rel_url.query_string,
                "body": body,
                "runner_route": request.headers.get("Livepeer-Runner-Route", ""),
                "session_id": request.headers.get("Livepeer-Session-Id", ""),
                "control_url": request.headers.get("Livepeer-Session-Control", ""),
            }
        )

    async def stop(request):
        await stop_runner_session(request)
        return web.Response(status=204)

    app = web.Application()
    app.router.add_post("/channels", channels)
    app.router.add_post("/proxy", proxy)
    app.router.add_post("/proxy-default", proxy_default)
    app.router.add_route("*", "/proxy-target/{tail:.*}", proxy_target)
    app.router.add_route("*", "/app-proxy/{tail:.*}", proxy_target)
    app.router.add_post("/stop", stop)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", spec["port"])
    await site.start()
    sockets = site._server.sockets
    port = sockets[0].getsockname()[1]
    runner_url = spec["public_url"] or f"http://127.0.0.1:{port}"

    try:
        async with registration(runner_url) as registered:
            reg = registered
            await asyncio.to_thread(sys.stdin.buffer.read)
    finally:
        await runner.cleanup()


asyncio.run(main())
