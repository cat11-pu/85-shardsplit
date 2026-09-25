"""shardsplit.py：可分裂的分片服务。

分裂按点切分区间；迁移期间新写入双写旧/新分片，migrate_step 分批
把旧分片上的尾部更新搬走；cutover 原子地把读路由切到按分裂点分片。
"""
from __future__ import annotations

import json
import os
import tempfile
from collections import deque


def _owns(lo: str, key: str, at, hi) -> bool:
    """键是否属于区间 [lo, at)/[at, hi)：at 为 None 表示无上界。"""
    if lo is not None and key < lo:
        return False
    if at is not None and not (key < at):
        return False
    return True


class Shards:
    def __init__(self):
        self.shards = {0: {}}
        self.splits = 0
        self.dual_writes = 0
        # 区间：shard -> [lo, hi)，None 端点开区间。
        self.meta = {0: {"lo": None, "hi": None}}
        # 迁移中：new_shard -> {"old": 旧分片, "at": 分裂点, "pending": [键...]}
        self.migrating = {}
        # 落盘目录（每个实例独立，避免并发服务互相踩快照）。
        self.snapdir = os.path.join(tempfile.gettempdir(),
                                   "shardsplit_%d" % id(self))

    # ---- 内部工具 -------------------------------------------------------

    def _active_split(self, shard):
        for new_index, item in self.migrating.items():
            if item["old"] == shard:
                return new_index, item
        return None, None

    def _auto_persist(self):
        try:
            self.persist()
        except OSError:
            pass

    # ---- 键值 -----------------------------------------------------------

    def _find_migration(self, key: str):
        """按键所在（分裂后旧+新合并的）区间找进行中的迁移。"""
        for new_index, item in self.migrating.items():
            lo = self.meta[item["old"]]["lo"]
            hi = self.meta[new_index]["hi"]
            if (lo is None or lo <= key) and (hi is None or key < hi):
                return item["old"], new_index, item
        return None, None, None

    def put(self, key: str, value: str) -> dict:
        old_index, new_index, item = self._find_migration(key)
        if item is None:
            self.shards[self._shard_of(key)][key] = value
        else:
            # 迁移期间：双写两边，尾部更新登记后分批搬走。
            self.shards[old_index][key] = value
            self.shards[new_index][key] = value
            if key not in item["pending_set"]:
                item["pending"].append(key)
                item["pending_set"].add(key)
            self.dual_writes += 1
        return {"shards": len(self.shards)}

    def _shard_of(self, key: str) -> int:
        """cutover 后按分裂点（区间）路由。"""
        for index, bounds in sorted(self.meta.items()):
            if _owns(bounds["lo"], key, bounds["hi"], None):
                return index
        return 0

    def get(self, key: str) -> dict:
        # 新分片优先、旧分片兜底（倒序：分片号越大越新）。
        for index in sorted(self.shards, reverse=True):
            data = self.shards[index]
            if key in data:
                return {"value": data[key], "shard": index}
        return {"value": None, "shard": None}

    # ---- 分裂 / 迁移 / 切换 ---------------------------------------------

    def split(self, shard: int, at: str) -> dict:
        if shard not in self.shards:
            raise ValueError("分片不存在: %r" % shard)
        if self._active_split(shard)[0] is not None:
            raise ValueError("分片 %r 正在迁移中" % shard)
        bounds = self.meta[shard]
        lo, hi = bounds["lo"], bounds["hi"]
        if hi is not None and not (at < hi):
            raise ValueError("分裂点必须落在分片区间内")
        if lo is not None and not (lo < at):
            raise ValueError("分裂点必须落在分片区间内")

        old_data = self.shards[shard]
        keep = {}
        moved = {}
        for key, value in old_data.items():
            (moved if key >= at else keep)[key] = value
        new_index = max(self.shards) + 1
        self.shards[shard] = keep
        self.shards[new_index] = moved
        self.meta[shard] = {"lo": lo, "hi": at}
        self.meta[new_index] = {"lo": at, "hi": hi}
        self.migrating[new_index] = {"old": shard, "at": at,
                                  "pending": deque(), "pending_set": set()}
        self.splits += 1
        self._auto_persist()
        return {"shards": len(self.shards), "at": at}

    def migrate_step(self, count: int) -> dict:
        migrated = 0
        for new_index, item in self.migrating.items():
            old_data = self.shards[item["old"]]
            pending = item["pending"]
            while pending and migrated < count:
                key = pending.popleft()
                item["pending_set"].discard(key)
                if key in old_data and key >= item["at"]:
                    self.shards[new_index][key] = old_data.pop(key)
                    migrated += 1
        self._auto_persist()
        return {"migrated": migrated}

    def cutover(self, shard: int) -> dict:
        new_index, item = self._active_split(shard)
        if item is None:
            raise ValueError("分片 %r 没有进行中的迁移" % shard)
        if item["pending"]:
            raise ValueError("迁移尚未完成，剩余 %d 个键待搬" % len(item["pending"]))
        at = item["at"]
        old_data = self.shards[shard]
        new_data = self.shards[new_index]
        # 清掉双写期间误入新分片、本属旧分片的键。
        for key in [key for key, value in new_data.items() if key < at]:
            del new_data[key]
        del self.migrating[new_index]
        self._auto_persist()
        return {"remaining": len(old_data)}

    # ---- 快照 / 恢复 -----------------------------------------------------

    def persist(self) -> bytes:
        blob = json.dumps({
            "version": 1,
            "shards": {str(index): data for index, data in self.shards.items()},
            "meta": {str(index): bounds for index, bounds in self.meta.items()},
            "migrating": {str(index): {"old": item["old"], "at": item["at"],
                       "pending": list(item["pending"])}
                       for index, item in self.migrating.items()},
            "splits": self.splits,
            "dual_writes": self.dual_writes,
        }, ensure_ascii=False, sort_keys=True).encode("utf-8")
        os.makedirs(self.snapdir, exist_ok=True)
        path = os.path.join(self.snapdir, "snapshot.json")
        fd, tmp = tempfile.mkstemp(dir=self.snapdir)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(blob)
            os.replace(tmp, path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise
        return blob

    def restore(self, blob: bytes = None) -> dict:
        if blob is None:
            path = os.path.join(self.snapdir, "snapshot.json")
            if not os.path.exists(path):
                return {"shards": len(self.shards)}
            with open(path, "rb") as handle:
                blob = handle.read()
        snapshot = json.loads(blob.decode("utf-8") if isinstance(blob, (bytes, bytearray)) else blob)
        self.shards = {int(index): dict(data) for index, data in snapshot["shards"].items()}
        self.meta = {int(index): dict(bounds) for index, bounds in snapshot["meta"].items()}
        self.migrating = {
            int(index): {"old": item["old"], "at": item["at"],
                         "pending": deque(item["pending"]),
                         "pending_set": set(item["pending"])}
            for index, item in snapshot.get("migrating", {}).items()
        }
        self.splits = snapshot.get("splits", 0)
        self.dual_writes = snapshot.get("dual_writes", 0)
        return {"shards": len(self.shards)}

    def recover(self) -> dict:
        return self.restore()

    # ---- 状态 -----------------------------------------------------------

    def stats(self) -> dict:
        return {"shards": len(self.shards), "splits": self.splits,
                "dual_writes": self.dual_writes,
                "sizes": {str(index): len(data) for index, data in sorted(self.shards.items())}}
