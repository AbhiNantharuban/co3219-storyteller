import asyncio
import random
import re

from . import config


def is_mock():
    return config.LLM_MODE != "real"


async def mock_delay():
    await asyncio.sleep(random.uniform(config.MOCK_LATENCY_MIN, config.MOCK_LATENCY_MAX))


# Word banks for fake story text (themed to The Leicester Tomb)
_NAMES = ["Priya", "Tom", "Amara", "Leo", "Zara", "Callum", "Nina", "Dev"]
_PLACES = ["the library basement", "the rare books room", "the sealed stairwell",
           "the tomb's inner chamber", "the old reading room", "the archive vault"]
_OBJECTS = ["a rusted iron key", "a Latin inscription", "a 500-year-old diary",
            "a student ID card dated 1926", "a map of tunnels under campus", "a cracked stone tablet"]
_TURNS = ["the lights flickered and died", "footsteps echoed from below",
          "the tomb door slid shut behind them", "a library alarm began to wail",
          "the candle flame bent towards the wall"]
_FEELINGS = ["a fear they refused to name", "a stubborn kind of hope", "guilt they had carried for years",
             "curiosity that outweighed caution", "the old loyalty that always held them back"]
_LINES = ["We shouldn't be down here.", "Whoever built this didn't want it found.",
          "Did you hear that?", "The diary mentions your name.",
          "Security will be here in ten minutes.", "Then we'd better be quick."]


def _pick_two_people(characters):
    known = [c["name"] for c in characters if c.get("name")]
    pool = known + [n for n in _NAMES if n not in known]
    a = random.choice(known) if known and random.random() < 0.7 else random.choice(pool)
    b = random.choice([n for n in pool if n != a])
    return a, b


def mock_proposal(role, context):
    a, b = _pick_two_people(context.get("characters", []))
    prompts = context.get("prompts") or []
    steer = f' (Answering the request: "{prompts[0][:60]}".)' if prompts else ""
    if role == "plot":
        text = (f"In {random.choice(_PLACES)}, {a} found {random.choice(_OBJECTS)}, and as they "
                f"picked it up {random.choice(_TURNS)}. {b} arrived moments later, already pale.{steer}")
        why = "Raises the stakes with a new clue."
    elif role == "character":
        text = (f"{a} stood very still, wrestling with {random.choice(_FEELINGS)}. For the first time "
                f"they admitted to {b} that they had been down here before.{steer}")
        why = "Deepens motivation and changes a relationship."
    else:
        text = (f'"{random.choice(_LINES)}" {a} whispered. {b} kept the torch on the inscription. '
                f'"{random.choice(_LINES)}"{steer}')
        why = "Moves the scene forward through dialogue."
    return {"text": text, "rationale": why}


def mock_review(proposals):
    scores = {}
    for p in proposals:
        score = random.uniform(4, 9)
        if "Answering the request" in p["text"]:
            score += 0.8                      # reward honouring the reader's request
        scores[p["id"]] = round(min(score, 10), 1)
    return {"scores": scores, "comment": "Mock review: scored for continuity and momentum."}


def mock_edit(candidates, context):
    best = candidates[0]
    names = set(re.findall(r"\b(" + "|".join(_NAMES) + r")\b", best["text"]))
    chars = {c["name"]: c for c in context.get("characters", []) if c.get("name")}
    for n in names:
        chars.setdefault(n, {"name": n, "description": "Drawn into the mystery of the tomb."})
    first_sentence = best["text"].split(". ")[0].rstrip(".") + "."
    summary = (context.get("summary", "") + " " + first_sentence).strip()[-700:]
    return {"chosen": best["id"], "text": best["text"], "summary": summary,
            "characters": list(chars.values()),
            "notes": "Mock editor: chose the top-voted proposal and checked names for continuity."}