import json
import os
from typing import List, Dict

HISTORY_FILE = os.path.join(os.path.dirname(__file__), "migration_history.json")

def _load_history() -> List[Dict]:
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return []

def _save_history(history: List[Dict]):
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)

def get_history() -> List[Dict]:
    return _load_history()

def append_history(entry: Dict):
    history = _load_history()
    # Insert at the beginning so newest is first
    history.insert(0, entry)
    _save_history(history)
