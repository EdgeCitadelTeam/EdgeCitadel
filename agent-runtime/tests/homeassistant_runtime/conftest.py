import sys
from pathlib import Path


PLUGIN_ROOT = Path(__file__).parents[3] / "agent-packages" / "homeassistant"
if str(PLUGIN_ROOT) not in sys.path:
    sys.path.insert(0, str(PLUGIN_ROOT))
