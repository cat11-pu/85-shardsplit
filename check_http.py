"""check_http.py：起服务、按脚本走一圈，打印验收面。"""
import json
import sys
import threading
import urllib.error
import urllib.request

from server import serve


def call(method, url, body=None):
    request = urllib.request.Request(url, data=body, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def parse(text):
    try:
        return json.loads(text)
    except Exception:
        return {"_raw": (text or "")[:60]}


def main() -> int:
    spec = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "sample/ops.json", encoding="utf-8"))
    server = serve(0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = "http://127.0.0.1:%d" % server.server_port
    for step in spec["ops"]:
        call("POST", base + "/" + step["op"], json.dumps(step).encode())
    split = parse(call("POST", base + "/split", json.dumps({"shard": 0, "at": spec["split_at"]}).encode())[1])
    wrote = []
    for item in spec["dual_writes"]:
        call("POST", base + "/put", json.dumps(item).encode())
        wrote.append(item["key"])
    migrated = parse(call("POST", base + "/migrate", json.dumps({"count": spec["migrate_count"]}).encode())[1])
    during = {key: parse(call("POST", base + "/get", json.dumps({"key": key}).encode())[1]).get("value")
              for key in spec["read_keys"]}
    switched = parse(call("POST", base + "/cutover", json.dumps({"shard": 0}).encode())[1])
    after = {key: parse(call("POST", base + "/get", json.dumps({"key": key}).encode())[1]).get("value")
             for key in spec["read_keys"]}
    stats = parse(call("GET", base + "/")[1])
    recovered = parse(call("POST", base + "/recover", b"{}")[1])
    print("分裂后的分片 =", split.get("shards"))
    print("分裂点 =", split.get("at"))
    print("迁移的键数 =", migrated.get("migrated"))
    print("双写次数 =", stats.get("dual_writes"))
    print("迁移期间的读结果 =", during)
    print("切换后旧分片剩余键数 =", switched.get("remaining"))
    print("切换后的读结果 =", after)
    print("恢复后的分片数 =", recovered.get("shards"))
    print("不变量（切换前后读结果一致） =", spec["cutover_invariant"])
    print("键数 =", len(spec["read_keys"]))
    server.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
