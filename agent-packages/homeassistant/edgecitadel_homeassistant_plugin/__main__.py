import asyncio
import logging

from .adapter import main


logging.basicConfig(level=logging.INFO)
asyncio.run(main())
