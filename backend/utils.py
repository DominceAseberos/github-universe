# Shared utility functions for backend

def build_ascii_progress_bar(done: int, total: int, width: int = 48) -> str:
    total = max(1, total)
    done = max(0, min(done, total))
    filled = round((done / total) * width)
    return "[" + ("█" * filled) + ("░" * (width - filled)) + "]"
