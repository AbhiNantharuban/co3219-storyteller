import os
import socket

def _bool(name, default):
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")

ROLE =  os.getenv("ROLE", "web").lower()
WRITER_ROLE = os.getenv("WRITER_ROLE", "plot").lower()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

LLM_MODE = os.getenv("LLM_MODE", "mock").lower()
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
LLM_MODEL = os.getenv("LLM_MODEL", "")
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "60"))
MOCK_LATENCY_MIN = float(os.getenv("MOCK_LATENCY_MIN", "0.4"))
MOCK_LATENCY_MAX = float(os.getenv("MOCK_LATENCY_MAX", "1.5"))
ROUND_INTERVAL = float(os.getenv("ROUND_INTERVAL", "15"))
MAX_PROPOSALS_PER_ROLE = int(os.getenv("MAX_PROPOSALS_PER_ROLE", "3"))
AUTO_ADVANCE = _bool("AUTO_ADVANCE", "true")
PROPOSAL_TIMEOUT = float(os.getenv("PROPOSAL_TIMEOUT", "45"))
REVIEW_TIMEOUT = float(os.getenv("REVIEW_TIMEOUT", "30"))
EDIT_TIMEOUT = float(os.getenv("EDIT_TIMEOUT", "45"))
PROMPTS_PER_ROUND = int(os.getenv("PROMPTS_PER_ROUND", "3"))
MAX_PENDING_PROMPTS = int(os.getenv("MAX_PENDING_PROMPTS", "50"))
LEADER_ELECTION = _bool("LEADER_ELECTION", "true")
LOCK_TTL_MS = int(os.getenv("LOCK_TTL_MS", "5000"))

POD_NAME = os.getenv("POD_NAME", socket.gethostname())

WRITER_ROLES = ["plot", "character", "dialogue"]
DEFAULT_STORY_ID = "main"
DEFAULT_TITLE = "The Leicester Tomb"
DEFAULT_PREMISE = ("A student discovers a tomb hidden underneath the university library")

