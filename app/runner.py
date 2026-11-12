"""后台批次编排。

- 每个批次一个工作线程，批次间并行；批次内规则顺序执行；
- 取消注册表保存在内存：cancel_event 控制本进程内的实际计算中断；
- 终态（completed/cancelled/interrupted）由控制库持久化，重启后依然可查；
- 取消 / 完成的竞态由两层机制共同裁决：
  1. 每批次一把生命周期锁（_CancelRegistry._locks），使“收尾落库”与
     “取消裁决”在同一批次上不可能交错执行；
  2. 控制库内的条件 UPDATE（storage.mark_batch_cancelled /
     mark_batch_completed）只允许从运行态迁移到终态，即使锁外存在并发，
     迟到的完成或取消也无法覆盖已落库的终态。

取消成功的承诺：请求被接受（CANCEL_ACCEPTED）后，批次最终一定是
cancelled 且 can_load=false，迟到结果丢弃且不得改写；完成先落库时取消被
明确拒绝（CANCEL_REJECTED_TERMINAL），报告原样保留。
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional

from . import storage
from .config import QUERY_TIMEOUT_SECONDS, SAMPLE_ROW_LIMIT
from .engine import RuleOutcome, execute_rule

# request_cancel 的裁决结果
CANCEL_ACCEPTED = "accepted"
# 批次不存在（路由层据此返回 404）
CANCEL_NOT_FOUND = "not_found"
# 批次已到终态（completed/cancelled/interrupted），取消被明确拒绝
CANCEL_REJECTED_TERMINAL = "rejected_terminal"
# 批次记录在控制库但本进程没有执行登记（如重启后的遗留批次），无法远程中断
CANCEL_REJECTED_UNREGISTERED = "rejected_unregistered"

_RUN_STATES = (storage.BATCH_QUEUED, storage.BATCH_RUNNING)


class _ReentrantGuard:
    """同线程可重入、跨线程互斥的生命周期锁。

    重入能力服务于一个真实场景：完成收尾临界区内的测试钩子
    （on_after_completion）同步调用 request_cancel 时，当前线程已经持有
    本锁，重入直接放行，避免普通 Lock 自死锁；不同线程之间仍然严格互斥，
    保证取消裁决与收尾不会交错。
    """

    def __init__(self) -> None:
        self._cond = threading.Condition(threading.Lock())
        self._owner: Optional[int] = None
        self._depth = 0

    def acquire(self) -> None:
        tid = threading.get_ident()
        with self._cond:
            if self._owner == tid:
                self._depth += 1
                return
            while self._owner is not None:
                self._cond.wait()
            self._owner = tid
            self._depth = 1

    def release(self) -> None:
        tid = threading.get_ident()
        with self._cond:
            if self._owner != tid:
                raise RuntimeError("release called by non-owner thread")
            self._depth -= 1
            if self._depth == 0:
                self._owner = None
                self._cond.notify_all()


class _CancelRegistry:
    """批次的进程内执行登记：取消事件 + 生命周期锁。

    - cancel_event：驱动 engine 内的 interrupt / progress handler，真正停止计算；
    - lifecycle_lock：串行化同批次的“完成收尾”与“取消裁决”两个临界区，
      消除“取消先被接受、完成随后覆盖”之类的交错窗口。
    """

    def __init__(self) -> None:
        self._events: dict[str, threading.Event] = {}
        self._locks: dict[str, _ReentrantGuard] = {}
        self._guard = threading.Lock()

    def register(self, batch_id: str) -> threading.Event:
        event = threading.Event()
        with self._guard:
            self._events[batch_id] = event
            # 正常情况下不存在同名残留；保留旧锁只会在极端重提场景造成不必要互斥
            self._locks[batch_id] = _ReentrantGuard()
        return event

    def lifecycle_lock(self, batch_id: str) -> Optional[_ReentrantGuard]:
        with self._guard:
            return self._locks.get(batch_id)

    @contextmanager
    def hold_lifecycle_lock(self, batch_id: str) -> Iterator[None]:
        lock = self.lifecycle_lock(batch_id)
        if lock is None:
            # 兜底：登记被提前清理时使用一把临时锁，绝不裸奔
            lock = _ReentrantGuard()
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    def cancel(self, batch_id: str) -> bool:
        with self._guard:
            event = self._events.get(batch_id)
        if event is None:
            return False
        event.set()
        return True

    def is_cancelled(self, batch_id: str) -> bool:
        with self._guard:
            event = self._events.get(batch_id)
        return event.is_set() if event else False

    def unregister(self, batch_id: str) -> None:
        with self._guard:
            self._events.pop(batch_id, None)
            self._locks.pop(batch_id, None)


registry = _CancelRegistry()

# 批次级并行线程池；max_workers 可按部署需要调整。
# 规则执行内部使用独立工作线程 + 看门狗，不占用该池的线程做阻塞等待之外的事。
_executor = ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="batch-runner"
)

# ---- 测试钩子：默认空操作，回归测试可替换以精确钉住竞态交错点 ----
# 最后一条规则落库后、进入“收尾裁决临界区”之前调用（锁外，取消可在此刻插入）
on_before_finish: Optional[Callable[[str], None]] = None
# request_cancel 进入生命周期锁之后、执行条件 UPDATE 之前调用
on_cancel_inside_lock: Optional[Callable[[str], None]] = None
# 完成条件 UPDATE 成功之后、释放生命周期锁 / 清除执行登记之前调用。
# 精确对应“完成已落库但执行登记尚未清除”的收尾窗口
on_after_completion: Optional[Callable[[str], None]] = None


def submit_batch(batch_id: str) -> None:
    """登记取消事件并把批次投入后台线程池，立即返回，不阻塞 API 请求。"""
    event = registry.register(batch_id)
    _executor.submit(_run_batch, batch_id, event)


def request_cancel(batch_id: str) -> dict[str, Any]:
    """请求取消整批并给出明确裁决。

    返回 dict::

        {"outcome": CANCEL_* , "status": <控制库中的当前批次状态>}

    裁决规则：
    - 批次仍在运行且本进程持有执行登记：置取消事件 -> 进入生命周期锁 ->
      条件 UPDATE 抢占终态。抢到则 accepted，批次保证以 cancelled 收尾；
    - 终态（completed/cancelled/interrupted）：明确拒绝，保留既有报告；
    - 重启后失去执行登记的遗留批次：拒绝（无法远程中断实际计算）。
    """
    batch_row = storage.get_batch_row(batch_id)
    if batch_row is None:
        return {"outcome": CANCEL_NOT_FOUND, "status": None}

    current_status = batch_row["status"]
    if current_status not in _RUN_STATES:
        return {"outcome": CANCEL_REJECTED_TERMINAL, "status": current_status}

    # 先尝试置位取消事件：没有执行登记意味着本进程无法停止任何实际计算
    if not registry.cancel(batch_id):
        return {
            "outcome": CANCEL_REJECTED_UNREGISTERED,
            "status": current_status,
        }

    lock = registry.lifecycle_lock(batch_id)
    if lock is None:
        # 极端竞态：登记在上面两步之间被清理。以控制库条件更新做最终裁决
        return _adjudicate_cancel(batch_id)

    with registry.hold_lifecycle_lock(batch_id):
        if on_cancel_inside_lock is not None:
            on_cancel_inside_lock(batch_id)
        return _adjudicate_cancel(batch_id)


def _adjudicate_cancel(batch_id: str) -> dict[str, Any]:
    """在生命周期锁内执行取消的最终裁决（可被两个调用路径复用）。"""
    batch_row = storage.get_batch_row(batch_id)
    if batch_row is None:
        return {"outcome": CANCEL_NOT_FOUND, "status": None}
    status = batch_row["status"]
    if status not in _RUN_STATES:
        # 完成先落库：明确拒绝取消，报告原样保留
        return {"outcome": CANCEL_REJECTED_TERMINAL, "status": status}

    won = storage.mark_batch_cancelled(
        batch_id,
        error="用户取消整批，未完成规则的结果无效，不可放行",
        expect_statuses=_RUN_STATES,
    )
    if not won:
        # 条件更新落空说明并发收尾已先落终态，重新读取以如实回报
        fresh = storage.get_batch_row(batch_id)
        return {
            "outcome": CANCEL_REJECTED_TERMINAL,
            "status": fresh["status"] if fresh else status,
        }

    # 终态已落 cancelled；回收仍未裁决的规则，已落库结论保持不变
    storage.mark_pending_rules_cancelled(batch_id)
    return {"outcome": CANCEL_ACCEPTED, "status": storage.BATCH_CANCELLED}


def _run_batch(batch_id: str, cancel_event: threading.Event) -> None:
    try:
        # 取消可能先于线程调度到达：已 cancelled 的批次不得被重新激活
        if not storage.mark_batch_running(batch_id):
            storage.mark_pending_rules_cancelled(batch_id)
            return

        batch_row = storage.get_batch_row(batch_id)
        db_path = batch_row["db_path"]
        rules = storage.get_rule_rows(batch_id)
        all_passed = True

        for row in rules:
            seq = row["seq"]

            # 取消点 1：开始下一条规则前
            if cancel_event.is_set():
                _settle_cancelled(batch_id, seq)
                return

            outcome: RuleOutcome = execute_rule(
                db_path=db_path,
                sql_text=row["sql_text"],
                cancel_event=cancel_event,
                timeout_seconds=QUERY_TIMEOUT_SECONDS,
                sample_limit=SAMPLE_ROW_LIMIT,
            )

            # 取消点 2：规则返回后、落库前复核，防止迟到结果改写成完成
            if cancel_event.is_set():
                _settle_cancelled(batch_id, seq)
                return
            current = storage.get_batch_row(batch_id)
            if current is None:
                return
            if current["status"] != storage.BATCH_RUNNING:
                # 控制库已被取消 / 中断裁决：本规则及后续规则一律作废
                storage.mark_pending_rules_cancelled(batch_id, seq)
                return

            storage.update_rule_result(
                batch_id,
                seq,
                status=outcome.status,
                violation_count=outcome.violation_count,
                sample_rows=outcome.sample_rows,
                error_message=outcome.error_message,
            )

            if outcome.status != storage.RULE_PASSED:
                all_passed = False

        # 测试钩子（锁外）：让回归测试能在“最后一次取消检查通过之后、
        # 收尾裁决之前”稳定插入取消请求，精确复现收尾窗口竞态
        if on_before_finish is not None:
            on_before_finish(batch_id)

        # 收尾临界区：与 request_cancel 互斥，并以条件 UPDATE 终裁
        with registry.hold_lifecycle_lock(batch_id):
            if cancel_event.is_set():
                _settle_cancelled_locked(batch_id)
                return
            won = storage.mark_batch_completed(
                batch_id,
                error=None if all_passed else "存在未通过规则，不可入仓",
            )
            if not won:
                # 取消在临界区外赢得了终态：不覆盖，按取消收尾剩余规则
                storage.mark_pending_rules_cancelled(batch_id)
            elif on_after_completion is not None:
                # 完成已落库、仍持有生命周期锁且执行登记尚未清除：
                # 回归测试在此刻发起取消，必须被明确拒绝且报告保留
                on_after_completion(batch_id)
    except Exception as exc:  # 编排层兜底：异常批次绝不放行
        # 仅当批次仍在运行态时才落 interrupted；若取消已先赢得终态，
        # 条件更新落空，cancelled 结论不被异常路径覆盖
        storage.mark_batch_interrupted(
            batch_id,
            error=f"执行过程中断: {type(exc).__name__}: {exc}",
        )
        storage.mark_pending_rules_cancelled(batch_id)
    finally:
        registry.unregister(batch_id)


def _settle_cancelled(batch_id: str, from_seq: int) -> None:
    """取消点触发：进入生命周期锁后完成 cancelled 裁决。"""
    with registry.hold_lifecycle_lock(batch_id):
        storage.mark_pending_rules_cancelled(batch_id, from_seq)
        _settle_cancelled_locked(batch_id)


def _settle_cancelled_locked(batch_id: str) -> None:
    """调用方已持有生命周期锁时的终态落库（幂等，不覆盖任何终态）。"""
    storage.mark_batch_cancelled(
        batch_id,
        error="用户取消整批，迟到结果已丢弃，不可放行",
        expect_statuses=_RUN_STATES,
    )
