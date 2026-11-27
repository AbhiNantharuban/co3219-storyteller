import asyncio
import logging

from . import config


def main():
    logging.basicConfig(level=logging.INFO,
                        format=f"%(asctime)s %(levelname)s [{config.ROLE}:{config.POD_NAME}] %(message)s")
    if config.ROLE == "web":
        import uvicorn
        uvicorn.run("app.web:app", host="0.0.0.0", port=8000)
    elif config.ROLE == "orchestrator":
        from .orchestrator import run_orchestrator
        asyncio.run(run_orchestrator())
    elif config.ROLE == "writer":
        from .agents import run_writer
        asyncio.run(run_writer(config.WRITER_ROLE))
    elif config.ROLE == "editor":
        from .agents import run_editor
        asyncio.run(run_editor())
    else:
        raise SystemExit(f"Unknown ROLE {config.ROLE!r}")


if __name__ == "__main__":
    main()