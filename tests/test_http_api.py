#!/usr/bin/env python3
"""HTTP 端到端测试（仅标准库）：真实启动 uvicorn 后走完整 HTTP 栈。

运行： python -I tests/test_http_api.py

覆盖：
1. 合法 SQLite 副本上传不再报 TypeError，批次可正常提交核查（回归该缺陷）；
2. Swagger UI（/docs）、OpenAPI schema、交互控制台（/docs-demo）可访问，
   控制台内含上传 / 读报告 / 取消三类操作；
3. 报告读取与 can_load 语义（干净库 completed -> true；违规库 -> false）；
4. 取消：运行中批次返回 200 且终态 cancelled；已完成批次取消返回 409，
   报告原样保留；不存在批次返回 404；
5. 慢批次运行期间，其他批次的提交与查询不被阻塞；
6. 非法文件 / 非法规则返回 400 而不是 500。
"""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

PASS = FAIL = 0


def check(cond: bool, label: str) -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {label}")
    else:
        FAIL += 1
        print(f"  [FAIL] {label}")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def make_db(path: Path, mode: str) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE subjects(id TEXT PRIMARY KEY, enrolled_on TEXT);
        CREATE TABLE visits(id INTEGER PRIMARY KEY, subject_id TEXT, visited_on TEXT);
        """
    )
    conn.execute("INSERT INTO subjects VALUES ('S1','2026-01-01')")
    if mode == "dirty":
        conn.execute(
            "INSERT INTO visits(subject_id, visited_on) VALUES ('S1','2025-01-01')"
        )
    else:
        conn.execute(
            "INSERT INTO visits(subject_id, visited_on) VALUES ('S1','2026-02-01')"
        )
    conn.commit()
    conn.close()
    return path


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def http_request(base: str, method: str, path: str,
                 body: "bytes | None" = None,
                 headers: "dict | None" = None):
    opener = urllib.request.build_opener(_NoRedirect)
    req = urllib.request.Request(
        base + path, data=body, headers=headers or {}, method=method
    )
    try:
        with opener.open(req, timeout=30) as resp:
            return resp.status, resp.read().decode("utf-8"), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8"), dict(exc.headers)


def multipart(fields: dict[str, str], files: dict[str, tuple[str, bytes, str]]):
    boundary = "----ctms-boundary-" + uuid.uuid4().hex
    chunks: list[bytes] = []
    for name, value in fields.items():
        chunks.append(f"--{boundary}\r\n".encode())
        chunks.append(
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
        )
        chunks.append(value.encode("utf-8"))
        chunks.append(b"\r\n")
    for name, (filename, content, ctype) in files.items():
        chunks.append(f"--{boundary}\r\n".encode())
        chunks.append(
            f'Content-Disposition: form-data; name="{name}"; '
            f'filename="{filename}"\r\n'.encode()
        )
        chunks.append(f"Content-Type: {ctype}\r\n\r\n".encode())
        chunks.append(content)
        chunks.append(b"\r\n")
    chunks.append(f"--{boundary}--\r\n".encode())
    return b"".join(chunks), {"Content-Type": f"multipart/form-data; boundary={boundary}"}


def wait_terminal(base: str, bid: str, timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    last: dict = {}
    while time.monotonic() < deadline:
        status, text, _ = http_request(base, "GET", f"/batches/{bid}", None, {})
        last = json.loads(text)
        if last["status"] not in ("queued", "running"):
            return last
        time.sleep(0.2)
    raise AssertionError(f"batch {bid} not terminal in {timeout}s: {last}")


def main() -> int:
    work = Path(tempfile.mkdtemp(prefix="ctms-http-"))
    data_dir = work / "data"
    data_dir.mkdir()
    clean_db = make_db(work / "clean.db", "clean")
    dirty_db = make_db(work / "dirty.db", "dirty")
    good_rules = json.dumps([
        {"name": "visit_before_enroll",
         "sql": ("SELECT v.id FROM visits v JOIN subjects s ON s.id=v.subject_id "
                 "WHERE v.visited_on < s.enrolled_on")}
    ])
    slow_rules = json.dumps([
        {"name": "slow",
         "sql": ("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 "
                 "FROM c WHERE x<10000000000) SELECT x FROM c")}
    ])

    port = free_port()
    base = f"http://127.0.0.1:{port}"
    # 用 PYTHONSAFEPATH 而非 -I：同样不把脚本目录塞进 sys.path（防目录内
    # 伪装模块），但保留 user site 中的第三方依赖（uvicorn/fastapi）
    env = dict(os.environ, DATA_DIR=str(data_dir),
               QUERY_TIMEOUT_SECONDS="2", PYTHONPATH=str(ROOT),
               PYTHONSAFEPATH="1")
    log = open(work / "uvicorn.log", "w")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--workers", "1"],
        cwd=str(ROOT), env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    try:
        # 等待服务就绪
        ready = False
        for _ in range(60):
            try:
                status, text, _ = http_request(base, "GET", "/health", None, {})
                if status == 200:
                    ready = True
                    break
            except OSError:
                pass
            time.sleep(0.5)
        if not ready:
            log.close()
            print(work.joinpath("uvicorn.log").read_text())
            raise AssertionError("server did not start")

        print("== 健康检查与文档页面 ==")
        status, text, _ = http_request(base, "GET", "/health", None, {})
        check(status == 200 and json.loads(text)["status"] == "ok", "/health 200")
        status, text, headers = http_request(base, "GET", "/", None, {})
        # 禁止自动跟随重定向时，307 会以 HTTPError 形式返回
        location = headers.get("Location") or headers.get("location") or ""
        check(status in (302, 307) and location.endswith("/docs"),
              f"根路径重定向到 /docs（实际 {status} -> {location}）")
        status, text, _ = http_request(base, "GET", "/docs", None, {})
        check(status == 200 and "swagger-ui" in text.lower(), "Swagger UI 可访问")
        status, text, _ = http_request(base, "GET", "/openapi.json", None, {})
        schema = json.loads(text)
        check(status == 200 and "/batches/{batch_id}/cancel" in schema["paths"],
              "OpenAPI schema 含取消接口")
        status, text, _ = http_request(base, "GET", "/docs-demo", None, {})
        check(status == 200 and 'fetch("/batches"' in text
              and "/cancel" in text and 'type="file"' in text,
              "交互控制台含上传/读报告/取消三类操作")

        print("== 上传合法 SQLite 副本（TypeError 回归） ==")
        body, headers = multipart(
            {"rules": good_rules, "name": "http-clean"},
            {"db_file": ("clean.db", clean_db.read_bytes(),
                         "application/x-sqlite3")},
        )
        status, text, _ = http_request(base, "POST", "/batches", body, headers)
        check(status == 201, f"合法副本上传成功 201（实际 {status}: {text[:200]}）")
        created = json.loads(text)
        bid_clean = created["id"]
        check(created["status"] in ("queued", "running"), "新批次为运行态")

        print("== 读取报告：干净库 completed 且 can_load=true ==")
        report = wait_terminal(base, bid_clean)
        check(report["status"] == "completed", f"干净批次 completed（实际 {report['status']}）")
        check(report["can_load"] is True, "零违规 -> can_load=true")
        check(report["rules"][0]["status"] == "passed"
              and report["rules"][0]["violation_count"] == 0,
              "规则 passed 且违规数 0")

        print("== 违规库：completed 但 can_load=false ==")
        body, headers = multipart(
            {"rules": good_rules, "name": "http-dirty"},
            {"db_file": ("dirty.db", dirty_db.read_bytes(),
                         "application/x-sqlite3")},
        )
        status, text, _ = http_request(base, "POST", "/batches", body, headers)
        check(status == 201, "违规副本同样可提交")
        bid_dirty = json.loads(text)["id"]
        report = wait_terminal(base, bid_dirty)
        check(report["status"] == "completed" and report["can_load"] is False,
              "有违规 -> can_load=false")
        check(report["rules"][0]["status"] == "violations"
              and report["rules"][0]["violation_count"] == 1,
              "违规计数与结论正确")

        print("== 取消运行中批次：200 + cancelled，计算被实际中断 ==")
        body, headers = multipart(
            {"rules": slow_rules, "name": "http-cancel"},
            {"db_file": ("clean.db", clean_db.read_bytes(),
                         "application/x-sqlite3")},
        )
        status, text, _ = http_request(base, "POST", "/batches", body, headers)
        check(status == 201, "慢查询批次创建成功")
        bid_slow = json.loads(text)["id"]
        time.sleep(0.8)  # 让批次进入 running 并开始实际计算
        t0 = time.monotonic()
        status, text, _ = http_request(
            base, "POST", f"/batches/{bid_slow}/cancel", None, {})
        check(status == 200, f"运行中批次取消 200（实际 {status}: {text}）")
        cancel_resp = json.loads(text)
        check(cancel_resp["cancelled_signal_sent"] is True
              and cancel_resp["status"] == "cancelled",
              "取消被接受且立即落 cancelled")
        report = wait_terminal(base, bid_slow, timeout=10)
        check(report["status"] == "cancelled" and report["can_load"] is False,
              "终态 cancelled 且不可放行")
        check(time.monotonic() - t0 < 5, "取消快速生效，未拖满查询时限")

        print("== 完成先落库后取消：409 明确拒绝，报告保留 ==")
        status, text, _ = http_request(
            base, "POST", f"/batches/{bid_clean}/cancel", None, {})
        check(status == 409, f"已完成批次取消返回 409（实际 {status}）")
        payload = json.loads(text)["detail"]
        check(payload["status"] == "completed", "409 回报当前 completed 状态")
        status, text, _ = http_request(
            base, "GET", f"/batches/{bid_clean}", None, {})
        check(json.loads(text)["can_load"] is True, "拒绝取消后报告原样保留")
        # 已取消批次再次取消同样 409
        status, text, _ = http_request(
            base, "POST", f"/batches/{bid_slow}/cancel", None, {})
        check(status == 409 and json.loads(text)["detail"]["status"] == "cancelled",
              "已取消批次再次取消返回 409")
        # 不存在批次 -> 404
        status, _, _ = http_request(
            base, "POST", "/batches/does-not-exist/cancel", None, {})
        check(status == 404, "不存在批次取消返回 404")

        print("== 慢批次运行不阻塞其他批次提交与查询 ==")
        body, headers = multipart(
            {"rules": slow_rules, "name": "http-blocker"},
            {"db_file": ("clean.db", clean_db.read_bytes(),
                         "application/x-sqlite3")},
        )
        status, text, _ = http_request(base, "POST", "/batches", body, headers)
        blocker = json.loads(text)["id"]
        time.sleep(0.5)
        t0 = time.monotonic()
        body, headers = multipart(
            {"rules": good_rules, "name": "http-fast"},
            {"db_file": ("clean.db", clean_db.read_bytes(),
                         "application/x-sqlite3")},
        )
        status, text, _ = http_request(base, "POST", "/batches", body, headers)
        fast_id = json.loads(text)["id"]
        report = wait_terminal(base, fast_id, timeout=10)
        check(report["status"] == "completed", "慢批次运行期间快批次照常完成")
        check(time.monotonic() - t0 < 5, "快批次未被慢批次阻塞")
        status, text, _ = http_request(base, "GET", "/batches?limit=10", None, {})
        listing = json.loads(text)
        check(status == 200 and len(listing) >= 4, "批次列表查询不被阻塞")
        # 清理阻塞批次
        http_request(base, "POST", f"/batches/{blocker}/cancel", None, {})
        wait_terminal(base, blocker, timeout=10)

        print("== 非法输入返回 400 而非 500 ==")
        body, headers = multipart(
            {"rules": good_rules, "name": "bad-file"},
            {"db_file": ("not.db", b"this is definitely not a sqlite database",
                         "application/octet-stream")},
        )
        status, text, _ = http_request(base, "POST", "/batches", body, headers)
        check(status == 400 and "sqlite" in text.lower(),
              f"非 SQLite 文件返回 400（实际 {status}）")
        body, headers = multipart(
            {"rules": "not-a-json", "name": "bad-rules"},
            {"db_file": ("clean.db", clean_db.read_bytes(),
                         "application/x-sqlite3")},
        )
        status, text, _ = http_request(base, "POST", "/batches", body, headers)
        check(status == 400, f"非法 rules JSON 返回 400（实际 {status}）")
        body, headers = multipart(
            {"rules": json.dumps([{"name": "write",
                                   "sql": "DELETE FROM visits"}]),
             "name": "write-rule"},
            {"db_file": ("clean.db", clean_db.read_bytes(),
                         "application/x-sqlite3")},
        )
        status, text, _ = http_request(base, "POST", "/batches", body, headers)
        check(status == 400, f"写操作规则在提交期被拒绝 400（实际 {status}）")

        print("== 上传库只读：服务端副本之外的原文件字节不变 ==")
        original_bytes = clean_db.read_bytes()
        body, headers = multipart(
            {"rules": json.dumps([{"name": "t",
                                   "sql": "INSERT INTO subjects(id) VALUES ('X')"}]),
             "name": "write-attempt"},
            {"db_file": ("clean.db", original_bytes, "application/x-sqlite3")},
        )
        status, _, _ = http_request(base, "POST", "/batches", body, headers)
        check(status == 400, "只读库上的写语句在 EXPLAIN 阶段即被拒绝")
        check(clean_db.read_bytes() == original_bytes, "原始上传文件字节未被修改")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
