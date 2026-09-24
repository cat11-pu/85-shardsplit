"""shardsplit.py：分片（基线：固定分片数，不能劈）。"""
from __future__ import annotations


class Shards:
    def __init__(self):
        self.shards = {0: {}}
        self.splits = 0
        self.dual_writes = 0

    def put(self, key: str, value: str) -> dict:
        """基线：全放 0 号分片。"""
        self.shards[0][key] = value
        return {"shards": len(self.shards)}

    def get(self, key: str) -> dict:
        for index, data in sorted(self.shards.items()):
            if key in data:
                return {"value": data[key], "shard": index}
        return {"value": None, "shard": None}

    def split(self, shard: int, at: str) -> dict:
        raise NotImplementedError("分片分裂还没实现")

    def migrate_step(self, count: int) -> dict:
        raise NotImplementedError("迁移还没实现")

    def cutover(self, shard: int) -> dict:
        raise NotImplementedError("原子切换还没实现")

    def persist(self) -> bytes:
        raise NotImplementedError("快照还没实现")

    def restore(self, blob: bytes = None) -> dict:
        raise NotImplementedError("重启恢复还没实现")

    def stats(self) -> dict:
        return {"shards": len(self.shards), "splits": self.splits,
                "dual_writes": self.dual_writes,
                "sizes": {str(index): len(data) for index, data in sorted(self.shards.items())}}
