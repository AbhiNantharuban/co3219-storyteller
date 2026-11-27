import json
import time

import redis.asyncio as redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.exceptions import WatchError

from . import config

K_STORIES = "stories"
LOCK_KEY = "lock:orchestrator"
STATS = "stats"
STATS_COMMITS = "stats:commit_times"

def k_meta(sid):
    return f"story:{sid}:meta"
def k_state(sid):
    return f"story:{sid}:state"
def k_passages(sid):
    return f"story:{sid}:passages"
def k_prompts(sid):
    return f"story:{sid}:prompts"
def k_active(sid):
    return f"story:{sid}:active"
def ch_updates(sid):
    return f"story:{sid}:updates"
def q(role):
    return f"tasks:{role}"
def k_round(rid, phase):
    return f"round:{rid}:{phase}"

def connect():
    return redis.from_url(
        config.REDIS_URL,
        decode_responses=True,
        health_check_interval=10,
        socket_keepalive=True,
        socket_connect_timeout=5,
        retry=Retry(ExponentialBackoff(cap=4, base=0.2), 6),
        retry_on_error=[RedisConnectionError, RedisTimeoutError],
    )

async def ensure_story(r, sid, title, premise):
    """Create a story if it doesn't exist yet. Safe to call many times."""
    await r.hsetnx(k_meta(sid), "title", title)
    await r.hsetnx(k_meta(sid), "premise", premise)
    await r.hsetnx(k_meta(sid), "created_at", str(time.time()))
    await r.hsetnx(k_state(sid), "version", "0")
    await r.hsetnx(k_state(sid), "summary", "")
    await r.hsetnx(k_state(sid), "characters", "[]")
    await r.sadd(K_STORIES, sid)


async def load_story(r, sid, recent=50):
    meta = await r.hgetall(k_meta(sid))
    state = await r.hgetall(k_state(sid))
    raw = await r.lrange(k_passages(sid), -recent, -1)
    return {
        "story_id": sid,
        "title": meta.get("title", sid),
        "premise": meta.get("premise", ""),
        "version": int(state.get("version", 0)),
        "summary": state.get("summary", ""),
        "characters": json.loads(state.get("characters") or "[]"),
        "last_round": json.loads(state.get("last_round") or "null"),
        "passages": [json.loads(p) for p in raw],
        "pending_prompts": await r.llen(k_prompts(sid)),
    }