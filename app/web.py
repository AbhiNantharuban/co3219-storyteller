"""The website server: page + API + live updates + metrics.
Stateless: all data lives in Redis, so any copy can serve any user."""
import asyncio
import json
import logging
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field
from redis.exceptions import RedisError

from . import config, store

log = logging.getLogger("web")
STATIC = Path(__file__).parent / "static"

REQ_LATENCY = Histogram("storyteller_http_request_seconds", "HTTP request latency", ["path", "method"])
G_CONTRIB = Gauge("storyteller_contributions_total", "Passages saved (all stories)")
G_ROUNDS = Gauge("storyteller_rounds_total", "Rounds completed")
G_CONFLICTS = Gauge("storyteller_conflicts_total", "Rounds rejected because of a version conflict")
G_TIMEOUTS = Gauge("storyteller_timeouts_total", "Rounds where no proposals arrived in time")
G_PER_MIN = Gauge("storyteller_contributions_last_minute", "Passages saved in the last 60s")
G_AVG_ROUND = Gauge("storyteller_avg_round_seconds", "Average round time")
G_QUEUE = Gauge("storyteller_queue_length", "Tasks waiting per agent queue", ["queue"])
G_CLIENTS = Gauge("storyteller_connected_clients", "Live browser connections on this copy")

r = store.connect()
clients = {}


async def read_stats():
    s = await r.hgetall(store.STATS)
    now = time.time()
    rounds = int(s.get("rounds", 0))
    queues = {role: await r.llen(store.q(role)) for role in config.WRITER_ROLES + ["editor"]}
    return {
        "served_by": config.POD_NAME,
        "leader": await r.get(store.LOCK_KEY),
        "llm_mode": config.LLM_MODE,
        "rounds_total": rounds,
        "contributions_total": int(s.get("contributions", 0)),
        "conflicts_total": int(s.get("conflicts", 0)),
        "timeouts_total": int(s.get("timeouts", 0)),
        "contributions_last_minute": await r.zcount(store.STATS_COMMITS, now - 60, now),
        "avg_round_ms": round(float(s.get("round_ms_sum", 0)) / rounds) if rounds else None,
        "queue_lengths": queues,
        "stories": await r.scard(store.K_STORIES),
        "connected_clients_this_pod": sum(len(v) for v in clients.values()),
    }


async def metrics_refresher():
    while True:
        try:
            s = await read_stats()
            G_CONTRIB.set(s["contributions_total"])
            G_ROUNDS.set(s["rounds_total"])
            G_CONFLICTS.set(s["conflicts_total"])
            G_TIMEOUTS.set(s["timeouts_total"])
            G_PER_MIN.set(s["contributions_last_minute"])
            G_AVG_ROUND.set((s["avg_round_ms"] or 0) / 1000)
            for qname, n in s["queue_lengths"].items():
                G_QUEUE.labels(queue=qname).set(n)
            G_CLIENTS.set(s["connected_clients_this_pod"])
        except RedisError:
            pass
        await asyncio.sleep(5)


async def update_listener():
    while True:
        pubsub = r.pubsub()
        try:
            await pubsub.psubscribe("story:*:updates")
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if not msg:
                    continue
                sid = msg["channel"].split(":")[1]
                dead = []
                for ws in list(clients.get(sid, ())):
                    try:
                        await ws.send_text(msg["data"])
                    except Exception:
                        dead.append(ws)
                for ws in dead:
                    clients.get(sid, set()).discard(ws)
        except (RedisError, OSError) as exc:
            log.warning("update listener lost Redis (%s); reconnecting", exc)
            await asyncio.sleep(1)
        finally:
            try:
                await pubsub.aclose()
            except Exception:
                pass


async def presence_keeper():
    while True:
        try:
            for sid, conns in list(clients.items()):
                if conns:
                    await r.set(store.k_active(sid), config.POD_NAME, ex=60)
        except RedisError:
            pass
        await asyncio.sleep(15)


@asynccontextmanager
async def lifespan(_app):
    for _ in range(30):
        try:
            await store.ensure_story(r, config.DEFAULT_STORY_ID, config.DEFAULT_TITLE, config.DEFAULT_PREMISE)
            break
        except RedisError:
            await asyncio.sleep(2)
    tasks = [asyncio.create_task(f()) for f in (update_listener, presence_keeper, metrics_refresher)]
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(title="Multi-agent storyteller", lifespan=lifespan)
KNOWN_PATHS = {"/", "/api/story", "/api/stories", "/api/prompt", "/api/stats", "/healthz", "/readyz", "/metrics"}


@app.middleware("http")
async def timing(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    path = request.url.path if request.url.path in KNOWN_PATHS else "other"
    REQ_LATENCY.labels(path=path, method=request.method).observe(time.perf_counter() - start)
    response.headers["X-Served-By"] = config.POD_NAME
    return response


class PromptIn(BaseModel):
    text: str = Field(min_length=1, max_length=300)
    story_id: str = config.DEFAULT_STORY_ID


class StoryIn(BaseModel):
    title: str = Field(min_length=1, max_length=80)
    premise: str = Field(min_length=10, max_length=600)


def clean_id(sid):
    return re.sub(r"[^a-z0-9-]", "", sid.lower())[:40] or config.DEFAULT_STORY_ID


# ---- The API
@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


@app.get("/healthz")
async def healthz():
    return {"ok": True, "pod": config.POD_NAME}


@app.get("/readyz")
async def readyz():
    try:
        await r.ping()
        return {"ready": True}
    except RedisError:
        return JSONResponse({"ready": False}, status_code=503)


@app.get("/api/stories")
async def list_stories():
    out = []
    for sid in sorted(await r.smembers(store.K_STORIES)):
        meta = await r.hgetall(store.k_meta(sid))
        out.append({"story_id": sid, "title": meta.get("title", sid),
                    "version": int(await r.hget(store.k_state(sid), "version") or 0)})
    return out


@app.post("/api/stories", status_code=201)
async def create_story(body: StoryIn):
    sid = clean_id(body.title.replace(" ", "-"))[:30] + "-" + uuid.uuid4().hex[:4]
    await store.ensure_story(r, sid, body.title.strip(), body.premise.strip())
    return {"story_id": sid}


@app.get("/api/story")
async def get_story(story_id: str = config.DEFAULT_STORY_ID):
    sid = clean_id(story_id)
    if not await r.sismember(store.K_STORIES, sid):
        return JSONResponse({"error": "no such story"}, status_code=404)
    data = await store.load_story(r, sid)
    data["served_by"] = config.POD_NAME
    data["leader"] = await r.get(store.LOCK_KEY)
    return data


@app.post("/api/prompt", status_code=202)
async def add_prompt(body: PromptIn):
    sid = clean_id(body.story_id)
    if not await r.sismember(store.K_STORIES, sid):
        return JSONResponse({"error": "no such story"}, status_code=404)
    key = store.k_prompts(sid)
    async with r.pipeline(transaction=True) as p:
        p.rpush(key, body.text.strip())
        p.ltrim(key, -config.MAX_PENDING_PROMPTS, -1)
        p.llen(key)
        _, _, pending = await p.execute()
    await store.publish(r, sid, {"type": "prompt", "text": body.text.strip(), "pending": pending})
    return {"queued": True, "pending": pending, "served_by": config.POD_NAME}


@app.get("/api/stats")
async def stats():
    return await read_stats()


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.websocket("/ws")
async def ws(websocket: WebSocket, story_id: str = config.DEFAULT_STORY_ID):
    sid = clean_id(story_id)
    await websocket.accept()
    clients.setdefault(sid, set()).add(websocket)
    try:
        await r.set(store.k_active(sid), config.POD_NAME, ex=60)
        snapshot = await store.load_story(r, sid)
        snapshot.update(type="snapshot", served_by=config.POD_NAME, leader=await r.get(store.LOCK_KEY))
        await websocket.send_text(json.dumps(snapshot))
        while True:
            await websocket.receive_text()
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        clients.get(sid, set()).discard(websocket)