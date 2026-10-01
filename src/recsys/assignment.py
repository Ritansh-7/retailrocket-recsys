import hashlib


def assign_variant(visitor_id: int | str, experiment_id: str, salt: str) -> str:
    """Return a stable, evenly distributed assignment for one experiment."""
    key = f"{experiment_id}:{visitor_id}:{salt}".encode()
    bucket = int.from_bytes(hashlib.sha256(key).digest()[:8], "big") % 10_000
    return "treatment" if bucket < 5_000 else "control"
