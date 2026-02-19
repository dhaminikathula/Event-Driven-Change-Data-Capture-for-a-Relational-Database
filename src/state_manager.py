import json
import os
from datetime import datetime

STATE_FILE = "state.json"


class StateManager:

    @staticmethod
    def load_state():
        """
        Load watermark state safely.
        Returns:
            {
                "last_updated": str | None,
                "last_id": int
            }
        """
        if not os.path.exists(STATE_FILE):
            return {
                "last_updated": None,
                "last_id": 0
            }

        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)

                # Backward compatibility
                return {
                    "last_updated": data.get("last_updated"),
                    "last_id": data.get("last_id", 0)
                }

        except (json.JSONDecodeError, ValueError):
            # Corrupted state file recovery
            return {
                "last_updated": None,
                "last_id": 0
            }

    @staticmethod
    def save_state(last_updated, last_id):
        """
        Save watermark atomically.
        """
        if isinstance(last_updated, datetime):
            last_updated = last_updated.isoformat()

        state = {
            "last_updated": last_updated,
            "last_id": last_id
        }

        temp_file = STATE_FILE + ".tmp"

        with open(temp_file, "w") as f:
            json.dump(state, f)

        os.replace(temp_file, STATE_FILE)
