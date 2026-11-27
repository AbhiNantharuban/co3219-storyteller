import asyncio
import json
import logging
import time

from redis.exceptions import RedisError

from . import config, llm, store

log = logging.getLogger("agent")


def touch_heartbeat():
    with open("/tmp/alive", "w") as f:
        f.write(str(time.time()))


async def propose(role, task):
    if llm.is_mock():
        await llm.mock_delay()
        out = llm.mock_proposal(role, task["context"])
    else:
        raise NotImplementedError("real AI is added in Stage 9")
    return {"id": task["task_id"], "role": role, "text": out["text"], "rationale": out.get("rationale", "")}


async def review(role, task):
    if llm.is_mock():
        await llm.mock_delay()
        out = llm.mock_review(task["proposals"])
    else:
        raise NotImplementedError("real AI is added in Stage 9")
    valid_ids = {p["id"] for p in task["proposals"]}
    scores = {}
    for pid, score in out.get("scores", {}).items():
        try:
            if pid in valid_ids:
                scores[pid] = max(1.0, min(10.0, float(score)))   # keep scores between 1 and 10
        except (TypeError, ValueError):
            continue
    return {"reviewer": role, "scores": scores, "comment": out.get("comment", "")}


async def edit(task):
    if llm.is_mock():
        await llm.mock_delay()
        return llm.mock_edit(task["candidates"], task["context"])
    raise NotImplementedError("real AI is added in Stage 9")


async def worker_loop(queue_role):
    r = store.connect()
    log.info("%s agent started (mode=%s)", queue_role, config.LLM_MODE)
    while True:
        touch_heartbeat()
        try:
            item = await r.blpop([store.q(queue_role)], timeout=5)
        except RedisError as exc:
            log.warning("Redis unavailable (%s), retrying", exc)
            await asyncio.sleep(1)
            continue
        if not item:
            continue
        task = json.loads(item[1])

        if time.time() > task.get("deadline", float("inf")):
            continue

        started = time.time()
        try:
            if task["type"] == "propose":
                result = await propose(queue_role, task)
            elif task["type"] == "review":
                result = await review(queue_role, task)
            elif task["type"] == "edit":
                result = await edit(task)
            else:
                continue
        except Exception as exc:
            log.warning("task %s failed: %s", task.get("task_id"), exc)
            continue

        result.update(agent=config.POD_NAME, task_id=task.get("task_id"),
                      took_ms=int((time.time() - started) * 1000))
        try:
            await r.rpush(task["reply_to"], json.dumps(result))
            await r.expire(task["reply_to"], 600)       # tidy up after 10 minutes
        except RedisError as exc:
            log.warning("could not send result: %s", exc)


async def run_writer(role):
    if role not in config.WRITER_ROLES:
        raise SystemExit(f"WRITER_ROLE must be one of {config.WRITER_ROLES}")
    await worker_loop(role)


async def run_editor():
    await worker_loop("editor")