import asyncio
import logging
from pathlib import Path

from .adapter import main


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
asyncio.run(main(Path(__file__).resolve().parent / "config.yaml"))
