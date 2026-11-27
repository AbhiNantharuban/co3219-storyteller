import asyncio
import json
import logging
import time
import uuid

from redis.exceptions import RedisError

from . import config, store

log = logging.getLogger("orchestrator")

# Renew the leader lock ONLY if we still own it (done inside Redis, so it's atomic)
RENEW_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('pexpire', KEYS[1], ARGV[2])
else
  return 0
end
"""


def touch_heartbeat():
    with open("/tmp/alive", "w") as f:
        f.write(str(time.time()))


async def elect_leader(r):
    if await r.set(store.LOCK_KEY, config.POD_NAME, nx=True, px=config.LOCK_TTL_MS):
        log.info("%s became LEADER", config.POD_NAME)
        return True
    return bool(await r.eval(RENEW_LUA, 1, store.LOCK_KEY, config.POD_NAME, config.LOCK_TTL_MS))


def plan_fanout(characters, prompt_count):
    fanout = 1 + len(characters) // 4 + min(prompt_count, 2)
    fanout = max(1, min(config.MAX_PROPOSALS_PER_ROLE, fanout))
    return fanout, f"{len(characters)} characters, {prompt_count} reader request(s)"


async def run_round(r, sid):
    rid = uuid.uuid4().hex[:10]
    t0 = time.time()

    story = await store.load_story(r, sid, recent=3)
    base_version = story["version"]
    taken = await r.lpop(store.k_prompts(sid), config.PROMPTS_PER_ROUND) or []
    fanout, reason = plan_fanout(story["characters"], len(taken))
    ctx = {"title": story["title"], "premise": story["premise"], "summary": story["summary"],
           "characters": story["characters"], "recent": story["passages"], "prompts": taken}
    info = {"round_id": rid, "base_version": base_version, "fanout": fanout,
            "fanout_reason": reason, "leader": config.POD_NAME, "prompts": taken}

    async def progress(phase, **extra):
        await store.publish(r, sid, {"type": "round", "phase": phase, **info, **extra})

    async def give_back_prompts():
        if taken:
            await r.lpush(store.k_prompts(sid), *reversed(taken))

    await progress("proposing")
    reply = store.k_round(rid, "proposals")
    deadline = time.time() + config.PROPOSAL_TIMEOUT
    for role in config.WRITER_ROLES:
        for variant in range(fanout):
            await r.rpush(store.q(role), json.dumps({
                "type": "propose", "task_id": f"{role[0].upper()}{variant + 1}-{rid[:4]}",
                "round_id": rid, "story_id": sid, "variant": variant,
                "context": ctx, "reply_to": reply, "deadline": deadline}))
    proposals = await store.collect(r, reply, len(config.WRITER_ROLES) * fanout, deadline)
    if not proposals:
        await give_back_prompts()
        await r.hincrby(store.STATS, "timeouts", 1)
        await progress("failed", error="no proposals arrived in time")
        return

    await progress("reviewing", proposals=[{k: p[k] for k in ("id", "role", "text", "agent")} for p in proposals])
    reply = store.k_round(rid, "reviews")
    deadline = time.time() + config.REVIEW_TIMEOUT
    slim = [{"id": p["id"], "role": p["role"], "text": p["text"]} for p in proposals]
    reviewers = 0
    for role in config.WRITER_ROLES:
        others = [p for p in slim if p["role"] != role]
        if others:
            reviewers += 1
            await r.rpush(store.q(role), json.dumps({
                "type": "review", "task_id": f"review-{role}-{rid[:4]}", "round_id": rid,
                "story_id": sid, "proposals": others, "context": ctx,
                "reply_to": reply, "deadline": deadline}))
    reviews = await store.collect(r, reply, reviewers, deadline)
    votes = {p["id"]: [] for p in proposals}
    for rv in reviews:
        for pid, score in rv.get("scores", {}).items():
            if pid in votes:
                votes[pid].append(score)
    for p in proposals:
        p["score"] = round(sum(votes[p["id"]]) / len(votes[p["id"]]), 2) if votes[p["id"]] else 5.0
        p["votes"] = len(votes[p["id"]])
    ranked = sorted(proposals, key=lambda p: p["score"], reverse=True)

    await progress("editing", proposals=[{k: p[k] for k in ("id", "role", "text", "agent", "score")} for p in ranked])
    reply = store.k_round(rid, "edit")
    deadline = time.time() + config.EDIT_TIMEOUT
    candidates = [{"id": p["id"], "role": p["role"], "text": p["text"], "score": p["score"]} for p in ranked[:2]]
    await r.rpush(store.q("editor"), json.dumps({
        "type": "edit", "task_id": f"edit-{rid[:4]}", "round_id": rid, "story_id": sid,
        "candidates": candidates, "context": ctx, "reply_to": reply, "deadline": deadline}))
    edited = await store.collect(r, reply, 1, deadline)
    top = ranked[0]
    if edited:
        e = edited[0]
        chosen = next((p for p in ranked if p["id"] == e.get("chosen")), top)
        text = str(e.get("text") or chosen["text"]).strip()
        summary = str(e.get("summary") or story["summary"])
        characters = e.get("characters") or story["characters"]
        notes, editor_agent = e.get("notes", ""), e.get("agent")
    else:
        chosen, text = top, top["text"]
        summary, characters = story["summary"], story["characters"]
        notes, editor_agent = "Editor timed out; used the top-voted proposal unchanged.", None

    took_ms = int((time.time() - t0) * 1000)
    round_info = dict(info, proposals=[{k: p.get(k) for k in ("id", "role", "agent", "score", "votes", "text")}
                                       for p in ranked],
                      chosen=chosen["id"], editor_notes=notes, editor_agent=editor_agent,
                      reviews=[{"reviewer": rv.get("reviewer"), "comment": rv.get("comment", "")} for rv in reviews],
                      took_ms=took_ms)
    passage = {"text": text, "role": chosen["role"], "proposal_id": chosen["id"],
               "author_agent": chosen.get("agent"), "score": chosen["score"],
               "round_id": rid, "ts": time.time()}
    new_version = await store.commit_passage(r, sid, base_version, passage, summary, characters, round_info)
    if new_version is None:
        await give_back_prompts()
        await r.hincrby(store.STATS, "conflicts", 1)
        log.warning("round %s CONFLICT: story %s moved past v%s; discarding and re-planning", rid, sid, base_version)
        await progress("conflict", error=f"story changed since v{base_version}; round discarded, will re-plan")
        return

    now = time.time()
    await r.hincrby(store.STATS, "rounds", 1)
    await r.hincrby(store.STATS, "contributions", 1)
    await r.hincrbyfloat(store.STATS, "round_ms_sum", took_ms)
    await r.zadd(store.STATS_COMMITS, {f"{sid}:{new_version}:{rid}": now})
    await r.zremrangebyscore(store.STATS_COMMITS, 0, now - 600)
    await store.publish(r, sid, {
        "type": "passage", "story_id": sid, "version": new_version,
        "passage": dict(passage, version=new_version), "summary": summary,
        "characters": characters, "last_round": round_info})
    log.info("story %s -> v%s in %sms (fanout %s, %s proposals, %s reviews)",
             sid, new_version, took_ms, fanout, len(proposals), len(reviews))


async def should_run(r, sid, last_end):
    if await r.llen(store.k_prompts(sid)) > 0:
        return True
    if config.AUTO_ADVANCE and await r.exists(store.k_active(sid)):
        return time.time() - last_end.get(sid, 0) >= config.ROUND_INTERVAL
    return False


async def run_orchestrator():
    r = store.connect()
    log.info("orchestrator %s started (leader election %s)", config.POD_NAME,
             "ON" if config.LEADER_ELECTION else "OFF")
    running = {}
    last_end = {}
    while True:
        touch_heartbeat()
        try:
            leader = await elect_leader(r) if config.LEADER_ELECTION else True
            if leader:
                for sid in await r.smembers(store.K_STORIES):
                    task = running.get(sid)
                    if task and not task.done():
                        continue
                    if task and task.done():
                        last_end[sid] = time.time()
                        if task.exception():
                            log.error("round for %s crashed: %s", sid, task.exception())
                        running.pop(sid)
                    if await should_run(r, sid, last_end):
                        running[sid] = asyncio.create_task(run_round(r, sid))
        except RedisError as exc:
            log.warning("Redis unavailable (%s), retrying", exc)
        await asyncio.sleep(1)