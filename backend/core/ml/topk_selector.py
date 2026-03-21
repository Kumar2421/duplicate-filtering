from __future__ import annotations

from typing import Iterable, List, Tuple


def select_topk_by_quality(items: Iterable[Tuple[float, object]], k: int) -> List[object]:
    sorted_items = sorted(items, key=lambda x: x[0], reverse=True)
    return [obj for _, obj in sorted_items[: max(0, int(k))]]
