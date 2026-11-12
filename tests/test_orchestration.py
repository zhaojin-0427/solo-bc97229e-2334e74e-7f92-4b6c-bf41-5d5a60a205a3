#!/usr/bin/env python3
"""端到端编排测试（标准库）：绕过 HTTP 层，直接驱动 storage + runner。

运行： python -I tests/test_orchestration.py
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

_TMP = Path(tempfile.mkdtemp(prefix="ctms-e2e-"))
os.environ["DATA_DIR"] = str(_TMP)

from app import runner, storage  # noqa: E402
from app.runner import registry  # noqa: E402

PASS = FAIL = 0


def check(cond: bool, label: str) -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {label}")
    else:
        FAIL += 1
        print(f"  [FAIL] {label}")


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
        # 访视早于入组 -> 必有违规
        conn.execute("INSERT INTO visits(subject_id, visited_on) VALUES ('S1','2025-01-01')")
    else:
        conn.execute("INSERT INTO visits(subject_id, visited_on) VALUES ('S1','2026-02-01')")
    conn.commit()
    conn.close()
    return path


def wait_terminal(bid: str, timeout: float = 20.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = storage.get_batch_row(bid)
        if row["status"] not in (storage.BATCH_QUEUED, storage.BATCH_RUNNING):
            return row["status"]
        time.sleep(0.1)
    raise AssertionError(f"batch {bid} did not finish in {timeout}s")


def main() -> int:
    storage.init_db()
    db_clean = make_db(_TMP / "clean.db", "clean")
    db_dirty = make_db(_TMP / "dirty.db", "dirty")

    good_rule = [{"name": "visit_before_enroll",
                  "sql": ("SELECT v.id FROM visits v JOIN subjects s ON s.id=v.subject_id "
                          "WHERE v.visited_on < s.enrolled_on")}]
    slow_rule = [{"name": "slow",
                  "sql": ("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 "
                          "FROM c WHERE x<10000000000) SELECT x FROM c")}]

    print("== 干净批次：零违规 -> completed 且 can_load 语义 ==")
    b1 = "e2e-clean"
    storage.insert_batch(b1, "clean", "clean.db", str(db_clean), good_rule)
    runner.submit_batch(b1)
    check(wait_terminal(b1) == storage.BATCH_COMPLETED, "干净批次完成")
    rules = storage.get_rule_rows(b1)
    check(rules[0]["status"] == storage.RULE_PASSED, "规则 passed")
    check(rules[0]["violation_count"] == 0, "违规数为 0")

    print("== 脏批次：有违规 -> completed 但不可入仓 ==")
    b2 = "e2e-dirty"
    storage.insert_batch(b2, "dirty", "dirty.db", str(db_dirty), good_rule)
    runner.submit_batch(b2)
    check(wait_terminal(b2) == storage.BATCH_COMPLETED, "脏批次同样走到 completed")
    rules = storage.get_rule_rows(b2)
    check(rules[0]["status"] == storage.RULE_VIOLATIONS, "规则 violations")
    check(rules[0]["violation_count"] == 1, "违规数为 1")
    sample = json.loads(rules[0]["sample_rows"])
    check(len(sample) == 1 and sample[0]["id"] == 1, "违规明细持久化且带列名")

    print("== 超时批次：timeout 结论、批次完成但不可入仓 ==")
    os.environ["QUERY_TIMEOUT_SECONDS"] = "1"
    # 直接用模块内常量的当前值构造（默认 5 秒，这里等 6 秒也可；为省时用引擎默认 5s 太长，
    # 故通过 runner 的常量注入：重新 import 不可行，改为提交后用 cancel 之外的方式）
    b3 = "e2e-timeout"
    storage.insert_batch(b3, "slow", "clean.db", str(db_clean), slow_rule)
    # runner 使用 config.QUERY_TIMEOUT_SECONDS；临时改模块常量
    runner.QUERY_TIMEOUT_SECONDS = 1
    runner.submit_batch(b3)
    check(wait_terminal(b3, timeout=15) == storage.BATCH_COMPLETED, "超时批次仍完成其他裁决流程")
    rules = storage.get_rule_rows(b3)
    check(rules[0]["status"] == storage.RULE_TIMEOUT, f"规则 timeout（实际 {rules[0]['status']}）")
    check(rules[0]["violation_count"] is None, "超时规则无违规计数")
    runner.QUERY_TIMEOUT_SECONDS = 5

    print("== 取消批次：cancelled，不可入仓，迟到结果不改写 ==")
    b4 = "e2e-cancel"
    storage.insert_batch(b4, "cancel", "clean.db", str(db_clean),
                         slow_rule + good_rule)
    runner.QUERY_TIMEOUT_SECONDS = 30
    runner.submit_batch(b4)
    time.sleep(0.5)
    verdict = runner.request_cancel(b4)
    check(verdict["outcome"] == runner.CANCEL_ACCEPTED,
          f"取消信号送达运行中批次并被接受（实际 {verdict}）")
    check(verdict["status"] == storage.BATCH_CANCELLED, "接受即落 cancelled 终态")
    check(wait_terminal(b4, timeout=10) == storage.BATCH_CANCELLED, "批次终态 cancelled")
    rules = storage.get_rule_rows(b4)
    statuses = [r["status"] for r in rules]
    check(storage.RULE_CANCELLED in statuses, "未完成规则标记 cancelled")
    check(all(s != storage.RULE_VIOLATIONS for s in statuses),
          "取消后不会出现迟到的 violations 完成结论")
    # 对已终态批次再次取消 -> 明确拒绝，不伪造
    again = runner.request_cancel(b4)
    check(again["outcome"] == runner.CANCEL_REJECTED_TERMINAL
          and again["status"] == storage.BATCH_CANCELLED,
          f"终态批次再次取消被拒绝（实际 {again}）")

    print("== 并发：两个批次同时运行互不阻塞 ==")
    bx, by = "e2e-par-1", "e2e-par-2"
    storage.insert_batch(bx, "p1", "clean.db", str(db_clean), slow_rule)
    storage.insert_batch(by, "p2", "clean.db", str(db_clean), good_rule)
    runner.QUERY_TIMEOUT_SECONDS = 2
    t0 = time.monotonic()
    runner.submit_batch(bx)
    runner.submit_batch(by)
    check(wait_terminal(by, timeout=10) == storage.BATCH_COMPLETED, "快批次在慢批次运行期间完成")
    check(time.monotonic() - t0 < 3, "快批次未被慢批次阻塞（并行生效）")
    check(wait_terminal(bx, timeout=10) == storage.BATCH_COMPLETED, "慢批次随后完成")
    check(storage.get_rule_rows(bx)[0]["status"] == storage.RULE_TIMEOUT, "慢批次规则 timeout")
    runner.QUERY_TIMEOUT_SECONDS = 5

    print("== 重启恢复：running 批次 -> interrupted，报告保留 ==")
    b5 = "e2e-restart"
    storage.insert_batch(b5, "restart", "clean.db", str(db_clean),
                         good_rule + good_rule)
    storage.set_batch_status(b5, storage.BATCH_RUNNING)
    storage.update_rule_result(b5, 0, status=storage.RULE_PASSED,
                               violation_count=0, sample_rows=[])
    marked = storage.reset_unfinished_on_startup()
    check(b5 in marked, "重启识别未完成批次")
    check(storage.get_batch_row(b5)["status"] == storage.BATCH_INTERRUPTED,
          "批次标记 interrupted")
    rr = storage.get_rule_rows(b5)
    check(rr[0]["status"] == storage.RULE_PASSED and rr[1]["status"] == storage.RULE_INTERRUPTED,
          "已完成结论保留、未完成标 interrupted")

    print("== 报告汇总字段 ==")
    rows = storage.list_batch_rows()
    check(len(rows) >= 6, f"批次列表持久化完整（{len(rows)} 条）")

    _test_window_cancel_before_finish(db_clean, good_rule)
    _test_window_completion_committed_before_cancel(db_clean, good_rule)
    _test_restart_keeps_cancelled_verdict(db_clean, good_rule)

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


def _wait_registry_clear(bid: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if registry.lifecycle_lock(bid) is None:
            return
        time.sleep(0.02)
    raise AssertionError(f"registry for {bid} not cleared in {timeout}s")


def _test_window_cancel_before_finish(db_clean: Path, good_rule: list) -> None:
    """窗口 A：零违规批次通过最后一次取消检查、尚未提交完成状态时取消。

    通过 runner.on_before_finish 钩子把取消精确插入“收尾临界区之前”，
    时序 100% 确定、不依赖 sleep。预期：取消返回 accepted，批次最终
    cancelled，can_load 语义为 false，迟到完成不得改写。
    """
    print("== 竞态窗口A：最后一次取消检查后、完成落库前收到取消 ==")
    b6 = "e2e-race-cancel-first"
    storage.insert_batch(b6, "race-cancel-first", "clean.db",
                         str(db_clean), good_rule)

    inserted: dict = {}

    def hook(bid: str) -> None:
        if bid != b6:
            return
        # 此刻批次仍为 running，规则已全部落库，但完成状态尚未提交
        row = storage.get_batch_row(bid)
        inserted["running_at_hook"] = row["status"]
        verdict = runner.request_cancel(bid)
        inserted["verdict"] = verdict
        # 取消必须在工作线程进入收尾临界区之前就完成裁决
        inserted["status_after_cancel"] = storage.get_batch_row(bid)["status"]

    runner.on_before_finish = hook
    try:
        runner.submit_batch(b6)
        check(wait_terminal(b6) == storage.BATCH_CANCELLED,
              "取消先被接受：批次终态必须为 cancelled")
    finally:
        runner.on_before_finish = None
    _wait_registry_clear(b6)

    check(inserted.get("running_at_hook") == storage.BATCH_RUNNING,
          "钩子触发时批次确实仍在 running（窗口被精确钉住）")
    check(inserted.get("verdict", {}).get("outcome") == runner.CANCEL_ACCEPTED,
          f"窗口内取消请求被接受（实际 {inserted.get('verdict')}）")
    check(inserted.get("status_after_cancel") == storage.BATCH_CANCELLED,
          "取消接受后控制库立即为 cancelled，工作线程尚未收尾")
    final = storage.get_batch_row(b6)
    check(final["status"] == storage.BATCH_CANCELLED, "迟到完成没有改写终态")
    rule_rows = storage.get_rule_rows(b6)
    # 规则在取消前已合法落库的结论按“迟到结果不得改写”原则保留；
    # 不可放行由批次 cancelled 终态保证（can_load 要求 completed）。
    check(all(r["status"] in (storage.RULE_PASSED, storage.RULE_VIOLATIONS,
                              storage.RULE_CANCELLED) for r in rule_rows),
          "规则结论仅来自真实执行或取消回收，无伪造状态（规则状态: "
          + ",".join(r["status"] for r in rule_rows) + ")")
    check(final["status"] == storage.BATCH_CANCELLED,
          "批次级 cancelled 终态保证 can_load=false")

    # 存储层回收语义：已落库结论不得改写，仍排队的规则被回收为 cancelled
    b6c = "e2e-cancel-reclaim"
    storage.insert_batch(b6c, "reclaim", "clean.db", str(db_clean),
                         good_rule + good_rule)
    storage.set_batch_status(b6c, storage.BATCH_RUNNING)
    storage.update_rule_result(b6c, 0, status=storage.RULE_VIOLATIONS,
                               violation_count=3, sample_rows=[{"id": 1}])
    # seq=1 仍为 queued
    reclaimed = storage.mark_pending_rules_cancelled(b6c)
    check(reclaimed == 1, f"仅回收 1 条未裁决规则（实际 {reclaimed}）")
    rr = storage.get_rule_rows(b6c)
    check(rr[0]["status"] == storage.RULE_VIOLATIONS
          and rr[0]["violation_count"] == 3,
          "已落库结论（含违规计数与明细）不被取消改写")
    check(rr[1]["status"] == storage.RULE_CANCELLED, "排队规则被回收为 cancelled")
    # 幂等：再次回收不再产生变化
    check(storage.mark_pending_rules_cancelled(b6c) == 0, "取消回收幂等")
    # 终态再取消：明确拒绝
    again = runner.request_cancel(b6)
    check(again["outcome"] == runner.CANCEL_REJECTED_TERMINAL,
          f"终态取消被拒绝（实际 {again}）")


def _test_window_completion_committed_before_cancel(db_clean: Path,
                                                    good_rule: list) -> None:
    """窗口 B：完成已落库、执行登记尚未清除时收到取消。

    通过 runner.on_after_completion 钩子，在“完成条件 UPDATE 成功之后、
    生命周期锁释放与执行登记清除之前”精确发起取消，时序 100% 确定。
    预期：取消明确 rejected_terminal，报告保留 completed 结论；
    登记清除后的再次取消同样被拒绝。
    """
    print("== 竞态窗口B：完成已落库、执行登记未清除时取消被拒绝 ==")
    b7 = "e2e-race-complete-first"
    storage.insert_batch(b7, "race-complete-first", "clean.db",
                         str(db_clean), good_rule)
    inserted: dict = {}

    def completion_hook(bid: str) -> None:
        if bid != b7:
            return
        inserted["status_at_hook"] = storage.get_batch_row(bid)["status"]
        inserted["registered_at_hook"] = registry.lifecycle_lock(bid) is not None
        inserted["verdict"] = runner.request_cancel(bid)

    runner.on_after_completion = completion_hook
    try:
        runner.submit_batch(b7)
        check(wait_terminal(b7) == storage.BATCH_COMPLETED, "批次正常完成")
    finally:
        runner.on_after_completion = None
    _wait_registry_clear(b7)

    check(inserted.get("status_at_hook") == storage.BATCH_COMPLETED,
          "钩子触发时完成确实已落库（窗口被精确钉住）")
    check(inserted.get("registered_at_hook") is True,
          "钩子触发时执行登记确实尚未清除")
    verdict = inserted.get("verdict")
    check(verdict is not None
          and verdict["outcome"] == runner.CANCEL_REJECTED_TERMINAL,
          f"窗口内取消被明确拒绝（实际 {verdict}）")
    check(verdict is not None and verdict["status"] == storage.BATCH_COMPLETED,
          f"拒绝回报的状态为 completed（实际 {None if verdict is None else verdict.get('status')}）")
    rules = storage.get_rule_rows(b7)
    check(all(r["status"] == storage.RULE_PASSED for r in rules),
          "完成报告原样保留，规则结论不被取消改写")

    # 登记清除后的终态取消：同样拒绝，状态保持
    verdict2 = runner.request_cancel(b7)
    check(verdict2["outcome"] == runner.CANCEL_REJECTED_TERMINAL
          and verdict2["status"] == storage.BATCH_COMPLETED,
          f"登记清除后取消仍被拒绝且报告保留（实际 {verdict2}）")
    check(storage.get_batch_row(b7)["status"] == storage.BATCH_COMPLETED,
          "拒绝取消后状态仍为 completed")


def _test_restart_keeps_cancelled_verdict(db_clean: Path, good_rule: list) -> None:
    """取消被接受后重启：cancelled 终态与规则结论必须原样保留。"""
    print("== 取消结论的重启持久性 ==")
    b9 = "e2e-race-restart"
    storage.insert_batch(b9, "race-restart", "clean.db",
                         str(db_clean), good_rule)

    def hook(bid: str) -> None:
        if bid == b9:
            runner.request_cancel(bid)

    runner.on_before_finish = hook
    try:
        runner.submit_batch(b9)
        check(wait_terminal(b9) == storage.BATCH_CANCELLED, "取消批次落 cancelled")
    finally:
        runner.on_before_finish = None
    _wait_registry_clear(b9)

    before = storage.get_batch_row(b9)
    rules_before = [(r["seq"], r["status"]) for r in storage.get_rule_rows(b9)]
    # 模拟重启：终态批次不在恢复范围内，结论保持
    marked = storage.reset_unfinished_on_startup()
    check(b9 not in marked, "已取消批次不被重启恢复逻辑触碰")
    after = storage.get_batch_row(b9)
    rules_after = [(r["seq"], r["status"]) for r in storage.get_rule_rows(b9)]
    check(after["status"] == storage.BATCH_CANCELLED
          and after["finished_at"] == before["finished_at"],
          "重启后 cancelled 终态与 finished_at 保持")
    check(rules_after == rules_before, "重启后规则结论逐行保持，迟到结果无从改写")


if __name__ == "__main__":
    raise SystemExit(main())
