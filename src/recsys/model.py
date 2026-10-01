import json
from pathlib import Path


class Recommender:
    def __init__(self, artifact_dir: Path):
        with (artifact_dir / "popular_items.json").open(encoding="utf-8") as f:
            self.popular_items: list[int] = json.load(f)
        with (artifact_dir / "item_neighbors.json").open(encoding="utf-8") as f:
            self.neighbors: dict[str, list[int]] = json.load(f)

    def recommend(self, recent_item_ids: list[int], variant: str, limit: int = 10) -> list[int]:
        seen = set(recent_item_ids)
        if variant == "control":
            candidates = self.popular_items
        else:
            scores: dict[int, float] = {}
            for recency, item_id in enumerate(reversed(recent_item_ids[-20:]), start=1):
                for rank, candidate in enumerate(self.neighbors.get(str(item_id), []), start=1):
                    if candidate not in seen:
                        scores[candidate] = scores.get(candidate, 0.0) + 1.0 / (recency * rank)
            candidates = [
                item for item, _ in sorted(scores.items(), key=lambda pair: pair[1], reverse=True)
            ]
            candidates += self.popular_items
        output: list[int] = []
        chosen = set()
        for item in candidates:
            if item not in seen and item not in chosen:
                output.append(int(item))
                chosen.add(item)
                if len(output) == limit:
                    break
        return output
