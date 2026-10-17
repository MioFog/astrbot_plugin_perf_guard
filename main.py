"""AstrBot 性能守护插件 (astrbot_plugin_perf_guard)

面向长期运行的 AstrBot（尤其是 Windows 直接安装、无人值守的场景）提供：

1. 内存监控：周期性读取 AstrBot 进程内存，超过阈值先自动释放，连续超标可自动重启
2. 定时重启：可配置每天固定时间重启（例如凌晨 4 点），避免长期运行内存膨胀
3. 日志清理：按保留天数 + 目录总大小上限自动清理日志，删最旧的文件
4. 内存释放：定期 gc.collect() 并裁剪进程工作集（Windows 上可把物理内存真正还给系统）
5. 指令与 LLM 工具：/perf 查询与控制；LLM 工具默认仅管理员可调用，重启默认不对 LLM 开放

重启机制：
    优先调用 AstrBot 官方内部重启接口；若探测不到，则退出进程（退出码 0），
    由 watchdog/run_with_watchdog.bat 看门狗脚本自动重新拉起。
"""

from __future__ import annotations

import asyncio
import gc
import inspect
import json
import os
import re
import shutil
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, StarTools

PLUGIN_NAME = "astrbot_plugin_perf_guard"


def _apply_optional_hook(deco: Any, fn: Any) -> Any:
    """把“可能不存在”的钩子装饰器套到方法上。

    AstrBot 不同版本的钩子集合有差异，这里做兼容处理：
    钩子不存在（None）或调用签名不符时，原样返回函数，保证插件仍能正常加载。
    """
    if deco is None:
        return fn
    try:
        return deco()(fn)
    except Exception:
        pass
    try:
        return deco(fn)
    except Exception as e:  # pragma: no cover - 取决于 AstrBot 版本
        logger.debug(f"[perf_guard] 钩子 {getattr(deco, '__name__', deco)} 不可用: {e}")
        return fn


# 可选的性能埋点钩子（缺失时自动降级：只少了耗时统计，其余功能不受影响）
_HOOK_EVENT_MESSAGE_TYPE = getattr(filter, "event_message_type", None)
_HOOK_ON_LLM_REQUEST = getattr(filter, "on_llm_request", None)
_HOOK_ON_LLM_RESPONSE = getattr(filter, "on_llm_response", None)
_HOOK_AFTER_MESSAGE_SENT = getattr(filter, "after_message_sent", None)
_HOOK_ON_PLUGIN_ERROR = getattr(filter, "on_plugin_error", None)
"""插件处理消息抛异常时的钩子（4.27.4: on_plugin_error）。

这是本插件最有价值的钩子之一：别人家插件报错时，AstrBot 只是往日志里写一行，
用户往往过了很久才发现（或者发现不了，只看到"机器人有时不回话"）。
接上它就能点名"是哪个插件的哪个处理函数在报错"。
"""
_EVENT_MSG_TYPE_ALL = getattr(getattr(filter, "EventMessageType", None), "ALL", None)

STATUS_WORDS = {"", "状态", "状态查询", "信息", "详情", "status", "info", "stat"}
DIAG_WORDS = {"诊断", "检测", "为什么慢", "慢", "延迟", "延迟诊断", "diag", "diagnose", "why", "latency"}
HEALTH_WORDS = {"体检", "健康", "健康检查", "全面检查", "自检", "doctor", "health", "check"}
ERROR_WORDS = {"报错", "错误", "异常", "插件报错", "err", "error", "errors", "exception"}
DB_WORDS = {"数据库", "db", "database", "sqlite", "db体积", "数据库整理"}
DB_VACUUM_WORDS = {"整理", "优化", "压缩", "vacuum", "yes", "确认"}
CLEAN_WORDS = {"清理", "清理日志", "清日志", "日志", "日志清理", "清空日志", "clean", "cleanlog", "logs"}
RELEASE_WORDS = {"释放", "释放内存", "内存", "内存释放", "回收", "gc", "release", "trim", "free"}
RESTART_WORDS = {"重启", "重新启动", "重载", "restart", "reboot", "reload"}
HELP_WORDS = {"帮助", "help", "?", "？", "说明"}

# ---- 诊断增强 ----
REPORT_WORDS = {"报告", "趋势", "日报", "周报", "历史", "report", "trend", "history"}
SLOWEST_WORDS = {"最慢", "慢消息", "最慢消息", "排行", "slow", "slowest", "top"}
SNAPSHOT_WORDS = {"快照", "现场", "故障", "snapshot", "dump"}
TRACE_WORDS = {"内存追踪", "追踪", "内存泄漏", "泄漏", "tracemalloc", "trace", "leak"}
STOP_WORDS = {"结束", "停止", "停", "stop", "end", "off"}

# ---- AI 自动化管理相关 ----
AI_WORDS = {"ai", "智能", "智能管理", "自动管理", "评审", "评估", "复查"}
AI_MODEL_WORDS = {"模型", "模型列表", "ai模型", "aimodel", "provider", "提供商"}
APPROVE_WORDS = {"批准", "同意", "通过", "执行", "approve", "yes", "ok"}
REJECT_WORDS = {"拒绝", "取消", "驳回", "reject", "no", "cancel"}
PENDING_WORDS = {"待审", "待审核", "审核列表", "pending", "审核"}

AI_ACTIONS = {
    "release_memory": "执行垃圾回收，释放运行内存",
    "clean_logs": "按配置清理日志文件",
    "restart": "重启 AstrBot（需 ai_allow_restart=true，且必须管理员批准）",
    "set_context_limit": "调整上下文条数上限（越小回复越快，0=不裁剪）",
    "noop": "不执行任何操作（状态正常时选它）",
}

AI_SYSTEM_PROMPT = """你是 AstrBot（一个 QQ/微信机器人框架）的运维助手。
你会收到一份机器人的实时运行状态，请判断是否需要执行**一个**运维动作。

只能从下面这些动作里选一个：
- release_memory：执行垃圾回收释放内存
- clean_logs：清理日志文件
- restart：重启机器人（只有卡顿/异常非常严重时才考虑）
- set_context_limit：调整上下文条数上限（参数 value 取 0~200，越小回复越快）
- noop：什么都不做

严格遵守：
1. 状态正常时必须选 noop。不要为了显得有用而执行动作。
2. 内存明显偏高（接近或超过阈值）才 release_memory；日志占用明显偏大才 clean_logs；
   响应非常慢且卡顿严重才 restart；上下文条数明显偏大才 set_context_limit。
3. 不确定就选 noop。
4. 只输出一行 JSON，不要输出解释文字，不要用 markdown 代码块。

JSON 格式：
{"action": "动作名", "args": {"value": 40}, "reason": "不超过 60 字的中文理由", "confidence": 0.8}
其中 args 只在 set_context_limit 时需要，其它动作给空对象 {}。"""

AI_REVIEW_FOOTER = """
请只输出那行 JSON。"""

HELP_TEXT = """📊 AstrBot 性能守护 · 使用说明

/perf            查看性能状态（内存、运行时长、日志占用、磁盘、累计重启）
/perf 体检        一键健康检查：插件加载失败、插件报错、模型可用性、磁盘、数据库 🩺
/perf 诊断        回复慢的原因：卡顿统计 + 阻塞代码定位 + 加速建议 ⭐
/perf 报错        插件报错统计：哪个插件的哪个处理函数在报错 🧯
/perf 数据库      数据库体积与可回收空间；加“整理 yes”执行 VACUUM 回收 🗄
/perf 释放内存    立即执行垃圾回收 + 裁剪工作集，归还物理内存
/perf 清理日志    立即按“保留天数 / 总大小上限”清理日志文件
/perf 重启        重启 AstrBot（需要看门狗或官方重启接口，见 README）
/perf 最慢 [n]     列出最近最慢的几条消息，并标明慢在哪一段（预处理/模型/后处理）🐌
/perf 报告 [天]    性能趋势报告：内存走势图、耗时变化、卡顿与消息统计 📈
/perf 快照 [n]     查看自动保存的故障现场快照（卡顿/内存告警时自动生成）🗂
/perf 内存追踪     开启按需内存追踪；再发一次（或加“结束”）出泄漏报告 🔬
/perf AI          让 AI 评估一次当前状态，并给出待审核的运维提议 🤖
/perf 待审        查看正在等待管理员审核的 AI 提议
/perf 批准 [编号]  执行某条待审核提议（不填编号=只有一条时直接执行）
/perf 拒绝 [编号]  取消某条待审核提议
/perf AI模型      列出可用的对话模型提供商（用于配置 ai_provider_id）
/perf 帮助        显示本帮助

🤖 AI 自动化管理（需在配置里开启 ai_enabled）：
  AI 只负责**提议**，任何动作都必须由管理员回复「批准」后才会执行；
  超过 ai_approval_timeout_seconds 未处理会自动作废；每小时执行次数受
  ai_max_actions_per_hour 限制；重启默认不允许由 AI 提议（ai_allow_restart）。

⚙️ 自动功能（在插件配置里开关）：
  · 事件循环卡顿监测 + 阻塞插件定位（lag_threshold_ms）
  · 消息响应耗时统计（/perf 诊断 查看 p50 / p95 / 最慢）
  · 插件报错监测：别的插件抛异常时点名，并在连续报错时通知管理员（/perf 报错）
  · 模型调用失败监测：识别“机器人不回话”是模型/中转挂了（/perf 状态 可见）
  · 磁盘余量保护：低于 disk_free_warn_gb 时告警并自动清日志
  · 内存超阈值自动释放（release_interval_seconds / release_threshold_mb）
  · 连续超标自动重启（auto_restart_enabled / memory_threshold_mb / memory_trigger_times）
  · 每天定时重启（scheduled_restart_enabled / scheduled_restart_time）
  · 日志自动清理（log_cleanup_enabled / log_keep_days / log_max_total_mb）

ℹ️ 默认仅管理员可用；LLM 工具默认也仅管理员可调用。
"""


# ----------------------------------------------------------------------
# 进程内存 / 系统信息工具（无第三方依赖也能工作）
# ----------------------------------------------------------------------
def _read_memory_mb() -> tuple[float, float] | None:
    """读取当前进程内存，返回 (当前工作集MB, 峰值MB)；全部手段失败时返回 None。"""
    # 1) psutil（AstrBot 环境通常自带）
    try:
        import psutil  # type: ignore

        info = psutil.Process(os.getpid()).memory_info()
        cur = float(getattr(info, "rss", 0) or 0) / 1048576.0
        peak = float(getattr(info, "peak_wset", 0) or 0) / 1048576.0
        if cur > 0:
            return (cur, peak or cur)
    except Exception:
        pass

    # 2) Windows API
    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = PROCESS_MEMORY_COUNTERS()
            counters.cb = ctypes.sizeof(counters)
            PROCESS_QUERY_INFORMATION = 0x0400
            PROCESS_VM_READ = 0x0010
            opened = ctypes.windll.kernel32.OpenProcess(
                PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, False, os.getpid()
            )
            handle = opened or ctypes.windll.kernel32.GetCurrentProcess()
            try:
                ok = ctypes.windll.psapi.GetProcessMemoryInfo(
                    handle, ctypes.byref(counters), counters.cb
                )
            finally:
                if opened:
                    try:
                        ctypes.windll.kernel32.CloseHandle(opened)
                    except Exception:
                        pass
            if ok:
                return (
                    counters.WorkingSetSize / 1048576.0,
                    counters.PeakWorkingSetSize / 1048576.0,
                )
        except Exception:
            pass

    # 3) Linux /proc
    try:
        with open("/proc/self/statm", "r", encoding="utf-8") as f:
            pages = int(f.read().split()[1])
        return (pages * os.sysconf("SC_PAGE_SIZE") / 1048576.0, 0.0)
    except Exception:
        return None


_trim_api: Any = None
"""Windows 工作集裁剪 API 的懒加载缓存：(取句柄函数, [裁剪函数...])"""


def _trim_working_set() -> bool:
    """Windows：裁剪当前进程工作集，把空闲物理内存归还系统。

    优先使用 psapi.EmptyWorkingSet；不可用时回退 kernel32.SetProcessWorkingSetSize。
    注意必须显式声明 argtypes，否则 ctypes 的参数转换会导致调用静默失败。
    """
    if sys.platform != "win32":
        return False
    global _trim_api
    try:
        import ctypes

        if _trim_api is None:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            get_current = kernel32.GetCurrentProcess
            get_current.argtypes = []
            get_current.restype = ctypes.c_void_p

            funcs: list[Callable[[Any], int]] = []
            try:
                psapi = ctypes.WinDLL("psapi", use_last_error=True)
                psapi.EmptyWorkingSet.argtypes = [ctypes.c_void_p]
                psapi.EmptyWorkingSet.restype = ctypes.c_int
                funcs.append(psapi.EmptyWorkingSet)
            except Exception:
                pass
            try:
                kernel32.SetProcessWorkingSetSize.argtypes = [
                    ctypes.c_void_p,
                    ctypes.c_size_t,
                    ctypes.c_size_t,
                ]
                kernel32.SetProcessWorkingSetSize.restype = ctypes.c_int

                def _set_ws(handle: Any) -> int:
                    return kernel32.SetProcessWorkingSetSize(
                        handle, ctypes.c_size_t(-1), ctypes.c_size_t(-1)
                    )

                funcs.append(_set_ws)
            except Exception:
                pass

            if not funcs:
                return False
            _trim_api = (get_current, funcs)

        get_current, funcs = _trim_api
        handle = get_current()
        for fn in funcs:
            try:
                if fn(handle):
                    return True
            except Exception:
                continue
        return False
    except Exception:
        return False


def _process_uptime_seconds() -> float | None:
    """进程已运行秒数（依赖 psutil，取不到返回 None）。"""
    try:
        import psutil  # type: ignore

        return max(0.0, time.time() - psutil.Process(os.getpid()).create_time())
    except Exception:
        return None


def _fmt_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d} 天 {h} 小时 {m} 分"
    if h:
        return f"{h} 小时 {m} 分"
    if m:
        return f"{m} 分 {s} 秒"
    return f"{s} 秒"


def _fmt_ts(ts: float | None) -> str:
    if not ts:
        return "从未"
    try:
        return datetime.fromtimestamp(ts).strftime("%m-%d %H:%M:%S")
    except Exception:
        return "未知"


def _safe_size(p: Path) -> int:
    """读文件大小，失败算 0（体检时不该因为一个坏文件整体失败）。"""
    try:
        return int(p.stat().st_size)
    except Exception:
        return 0


class _StackSampler:
    """后台线程周期采样主线程调用栈，用于定位"把事件循环卡住"的代码。

    原理：事件循环跑在主线程；当某个插件在协程里做了同步阻塞操作（requests、
    大文件读写、CPU 密集计算等），循环无法调度其它任务，所有消息都会排队变慢。
    本采样器在子线程里周期抓一次主线程栈写入环形缓冲，插件检测到卡顿时
    再按时间戳回捞对应样本，从而指出是哪段代码、哪个插件造成的阻塞。

    低开销设计：采样时**只记录 (文件名, 行号, 函数名) 元组**，不做字符串格式化、
    不调 traceback 模块（那是每帧一次字符串拼接，10Hz 下会白烧 CPU）；只有真的
    发生卡顿时才把选中的那几帧格式化成文本。
    """

    def __init__(self, interval: float = 0.1, capacity: int = 80, depth: int = 8) -> None:
        self._interval = max(0.02, float(interval))
        self._depth = depth
        self._buf: deque[tuple[float, str, tuple[tuple[str, int, str], ...]]] = deque(
            maxlen=capacity
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._main_ident = threading.main_thread().ident
        self.cpu_seconds: float = 0.0
        """采样线程累计消耗的 CPU 时间（秒），用于向用户证明本插件开销极低"""
        self.samples: int = 0
        self.started_ts: float = 0.0

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()  # 必须清掉：否则插件热重载后线程会立刻退出
        self.started_ts = time.time()
        self._thread = threading.Thread(
            target=self._run, name="perf-guard-sampler", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread = None

    def set_interval(self, seconds: float) -> None:
        self._interval = max(0.02, float(seconds))

    @property
    def interval(self) -> float:
        return self._interval

    def samples_since(self, ts: float) -> list[tuple[float, str, str]]:
        """返回时间戳 >= ts 的样本，每项为 (时间戳, 嫌疑插件名, 栈文本)。"""
        try:
            out: list[tuple[float, str, str]] = []
            for item in list(self._buf):
                if item[0] >= ts:
                    out.append((item[0], item[1], self._format(item[2])))
            return out
        except Exception:
            return []

    @staticmethod
    def _format(frames: tuple[tuple[str, int, str], ...]) -> str:
        try:
            return "".join(
                f'  File "{fn}", line {ln}, in {name}\n' for fn, ln, name in frames
            )
        except Exception:
            return ""

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                frame = sys._current_frames().get(self._main_ident)
                if frame is None:
                    continue
                frames: list[tuple[str, int, str]] = []
                depth = 0
                while frame is not None and depth < 64:
                    code = frame.f_code
                    frames.append((code.co_filename, frame.f_lineno, code.co_name))
                    frame = frame.f_back
                    depth += 1
                frames.reverse()
                cheap = tuple(frames[-self._depth:])
                self._buf.append((time.time(), self._plugin_of(cheap), cheap))
                self.samples += 1
            except Exception:
                continue
            finally:
                try:
                    self.cpu_seconds = time.thread_time()
                except Exception:
                    pass

    @staticmethod
    def _plugin_from_filenames(filenames: Any) -> str:
        """从文件名序列里认出所属插件（AstrBot 插件位于 data/plugins/<name>/ 下）。"""
        try:
            key = "\\data\\plugins\\"
            for fn in filenames:
                low = str(fn).lower().replace("/", "\\")
                pos = low.find(key)
                if pos >= 0:
                    name = low[pos + len(key):].split("\\", 1)[0].strip()
                    if name:
                        return name
            for fn in filenames:
                if "base_events.py" in str(fn):
                    return "(asyncio 内部)"
            return "(未知)"
        except Exception:
            return "(未知)"

    @classmethod
    def _plugin_of(cls, frames: tuple[tuple[str, int, str], ...]) -> str:
        return cls._plugin_from_filenames(f[0] for f in frames)

    @classmethod
    def _guess_plugin(cls, stack: str) -> str:
        """从栈文本里猜出所属插件名（保留文本入口，便于复用与测试）。"""
        try:
            import re

            return cls._plugin_from_filenames(re.findall(r'File "([^"]+)"', stack))
        except Exception:
            return "(未知)"


class PerfGuard(Star):
    """AstrBot 性能守护插件：回复加速诊断 + 内存/日志/重启自动化。"""

    _EXIT_DELAY = 2.0
    """主动退出前的等待秒数，留给通知消息发送时间。"""

    _LAG_WARN_INTERVAL = 10.0
    """卡顿日志最小间隔（秒）：持续卡顿时不刷屏，避免日志本身拖慢系统"""

    _STATE_SAVE_MIN_INTERVAL = 20.0
    """状态落盘最小间隔（秒）：避免在卡顿时反复同步写盘"""

    def __init__(self, context: Context, config: AstrBotConfig | None = None) -> None:
        super().__init__(context, config)
        self.config = config if config is not None else {}

        self._data_dir: str = ""
        self._state_path: str = ""
        self._state: dict[str, Any] = {}
        self._task: asyncio.Task | None = None
        self._lag_task: asyncio.Task | None = None
        self._bg_tasks: set[asyncio.Task] = set()
        """临时后台任务的强引用集合。

        asyncio 只持有任务的弱引用：像“3 秒后重启进程”这种 create_task 后不保存引用的
        任务，可能在执行前就被垃圾回收，表现为用户被告知“正在重启”但什么都没发生。
        所有 fire-and-forget 任务都必须经 _spawn() 登记在这里。
        """
        self._sampler = _StackSampler()
        self._load_ts: float = time.time()
        self._over_count: int = 0
        self._last_release_ts: float = 0.0
        self._last_restart_ts: float = 0.0
        self._last_log_clean_ts: float = 0.0
        self._watchdog: bool = False

        # 回复速度诊断相关
        self._lag_records: deque[float] = deque(maxlen=1200)
        """最近的事件循环延迟样本（秒）"""
        self._lag_events: deque[dict[str, Any]] = deque(maxlen=50)
        """最近若干次卡顿事件（含嫌疑代码栈）"""
        self._slow_hits: dict[str, int] = {}
        """嫌疑插件 -> 命中卡顿样本次数"""
        self._msg_latency: deque[float] = deque(maxlen=500)
        """消息总响应耗时样本（秒）"""
        self._llm_latency: deque[float] = deque(maxlen=500)
        """LLM 调用耗时样本（秒）"""
        self._pending_llm: dict[str, float] = {}
        """event 标识 -> LLM 请求开始时间"""
        self._pending_msg: dict[str, float] = {}
        """event 标识 -> 消息到达时间"""
        self._last_ctx: tuple[int, int] | None = None
        """上次请求的上下文条数：(裁剪前, 裁剪后)"""
        self._cpu_over: int = 0
        self._last_lag_warn_ts: float = 0.0
        self._last_state_save: float = 0.0
        self._state_dirty: bool = False
        self._tool_count: int = 0
        self._dupe_tools: list[str] = []
        self._dupe_warned: bool = False

        # 插件报错监测（on_plugin_error）
        self._plugin_errors: deque[dict[str, Any]] = deque(maxlen=50)
        """最近若干次插件报错：{ts, plugin, handler, error, count}"""
        self._plugin_error_hits: dict[str, int] = {}
        """插件名 -> 本进程内累计报错次数"""
        self._err_notify_queue: dict[str, dict[str, Any]] = {}
        """待通知的报错聚合：插件名 -> {first_ts, last_ts, count, sample}"""
        self._last_err_notify_ts: float = 0.0
        self._last_disk_warn_ts: float = 0.0
        """磁盘告警冷却时间戳"""

        # 模型（LLM）健康监测
        self._model_calls: int = 0
        """本进程内观察到的模型响应次数"""
        self._model_errs: int = 0
        """其中 role=err（模型/中转失败）的次数"""
        self._model_err_events: deque[dict[str, Any]] = deque(maxlen=20)
        """最近的模型失败：{ts, text}"""
        self._model_ok_mark: int = 0
        """上次通知时的失败次数，用于去重"""
        self._last_model_notify_ts: float = 0.0

        # AI 自动化管理：提议 -> 管理员审核 -> 执行
        self._pending_ai: dict[str, dict[str, Any]] = {}
        """待审核提议：编号 -> 提议内容"""
        self._ai_seq: int = 0
        """提议编号自增序号"""
        self._ai_exec_ts: list[float] = []
        """已执行动作的时间戳（用于每小时频率限制）"""
        self._ai_last_run_ts: float = 0.0
        """上次 AI 评估时间（用于定时评估）"""
        self._ai_last_result: str = ""
        """最近一次 AI 评估/执行的结果摘要"""
        self._ai_lag_mark: int = 0
        """上次 AI 评估时的卡顿计数，用于判断是否新发生卡顿"""

        # 慢消息追踪：把“哪条消息慢、慢在哪一段”留下来
        self._slow_messages: deque[dict[str, Any]] = deque(maxlen=20)
        """超过阈值的慢消息记录"""
        self._pending_llm_cost: dict[str, tuple[float, float]] = {}
        """event 标识 -> (LLM 耗时, 结束时间)，用于算后处理耗时"""
        self._last_phases: tuple[float, float, float] | None = None
        """最近一条消息的三段耗时：(预处理, 模型, 后处理)"""
        self._msg_total: int = 0
        """累计统计到的消息条数"""

        # 历史趋势
        self._history: list[dict[str, Any]] = []
        """定时采样点（内存/耗时/卡顿），用于 /perf 报告"""
        self._history_path: str = ""
        self._last_history_ts: float = 0.0
        self._last_history_write: float = 0.0
        self._history_lag_mark: int = 0
        self._history_msg_mark: int = 0

        # 异常快照
        self._last_snapshot_ts: float = 0.0
        """上次写快照的时间（去抖，避免卡顿时刷文件）"""

        # 按需内存追踪（tracemalloc）
        self._tracer_on: bool = False
        self._tracer_start_ts: float = 0.0
        self._tracer_base: Any = None
        self._trace_report: str = ""
        """自动关闭时产出的报告，供 /perf 内存追踪 查看"""

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def _spawn(self, coro: Any, *, name: str = "") -> asyncio.Task | None:
        """启动一个 fire-and-forget 后台任务，并保留强引用。

        asyncio 内部只保存任务的弱引用，不持有引用的任务可能被 GC 掉（官方文档明确警告）。
        本插件的重启/退出任务一旦被 GC，用户就会看到“正在重启”但进程毫无反应，
        所以统一走这里登记，terminate() 时一并取消。
        """
        try:
            task = asyncio.create_task(coro, name=name or None) if name else asyncio.create_task(coro)
        except Exception as e:
            logger.warning(f"[perf_guard] 后台任务启动失败（{name or coro}）: {e}")
            return None
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

    async def initialize(self) -> None:
        try:
            self._data_dir = str(StarTools.get_data_dir(PLUGIN_NAME))
        except Exception as e:
            import tempfile

            self._data_dir = os.path.join(tempfile.gettempdir(), PLUGIN_NAME)
            logger.warning(f"[perf_guard] 无法获取插件数据目录({e})，改用 {self._data_dir}")
        os.makedirs(self._data_dir, exist_ok=True)
        self._state_path = os.path.join(self._data_dir, "perf_state.json")
        self._history_path = os.path.join(self._data_dir, "history.json")
        self._load_state()
        self._history_load()

        self._last_restart_ts = float(self._state.get("last_restart_ts") or 0.0)
        self._last_log_clean_ts = float(self._state.get("last_log_clean_ts") or 0.0)
        self._last_release_ts = float(self._state.get("last_release_ts") or 0.0)

        self._watchdog = self._detect_watchdog()
        mem = _read_memory_mb()
        mem_text = f"{mem[0]:.0f}MB" if mem else "未知"

        logger.info(
            "[perf_guard] 性能守护已启动 | 当前内存 %s | 看门狗 %s | 检查间隔 %ss | "
            "内存阈值 %sMB | 自动重启 %s | 定时重启 %s | 日志清理 %s | 日志目录 %s",
            mem_text,
            "已检测到" if self._watchdog else "未检测到",
            self._int_cfg("check_interval_seconds", 120),
            self._int_cfg("memory_threshold_mb", 800),
            "开" if self._cfg("auto_restart_enabled", False) else "关",
            str(self._cfg("scheduled_restart_time", "")) if self._cfg("scheduled_restart_enabled", False) else "关",
            "开" if self._cfg("log_cleanup_enabled", True) else "关",
            [str(p) for p in self._log_dirs()],
        )
        caps = self._restart_capabilities()
        if not any(caps.values()):
            logger.warning(
                "[perf_guard] 未发现任何可用的重启方式（自定义入口 / Dashboard 接口 / 进程重启 / 看门狗均不可用），"
                "自动重启将无法执行。建议用 watchdog\\run_with_watchdog.bat 启动 AstrBot。"
            )
        else:
            logger.info(
                "[perf_guard] 可用重启方式：%s",
                "、".join(k for k, v in caps.items() if v),
            )

        self._task = asyncio.create_task(self._monitor_loop())

        # 回复速度诊断：堆栈采样线程 + 事件循环卡顿监测
        # 采样是这个插件里唯一“常驻消耗 CPU”的东西，所以：间隔可调、可整体关闭、
        # 且会在 /perf 状态里向用户自报 CPU 开销，方便验证它没有拖慢机器人。
        try:
            if self._cfg("stack_sampling_enabled", True):
                self._sampler.set_interval(
                    self._int_cfg("stack_sample_interval_ms", 200) / 1000.0
                )
                self._sampler.start()
            self._lag_task = asyncio.create_task(self._lag_loop())
            logger.info(
                "[perf_guard] 回复速度诊断已开启 | 卡顿阈值 %sms | 循环采样 %sms | 堆栈采样 %s",
                self._int_cfg("lag_threshold_ms", 1000),
                self._int_cfg("lag_check_interval_ms", 1000),
                (
                    f"每 {self._int_cfg('stack_sample_interval_ms', 200)}ms"
                    if self._cfg("stack_sampling_enabled", True)
                    else "已关闭"
                ),
            )
        except Exception as e:
            logger.warning(f"[perf_guard] 启动速度诊断失败（不影响其它功能）: {e}")

    def _perm_text(self, what: str) -> str:
        """权限不足时的统一说明：必须讲清“怎么才能用”。

        QQ 私聊里没有“群管理员”这个概念，event.is_admin() 恒为 False，
        能否使用只取决于 AstrBot 主配置的 admins_id 名单——而它的默认值是
        ["astrbot"] 这个占位符。不提醒用户填自己的 QQ 号，用户就会以为插件坏了。
        """
        return (
            f"权限不足：当前设置下只有管理员才能{what}。请向用户说明："
            "群聊里群主/管理员可直接使用；"
            "私聊里没有群角色，需要把本人 QQ 号加入 AstrBot 主配置的 admins_id 名单；"
            "或在插件配置里关闭管理员限制（不推荐）。"
        )

    async def terminate(self) -> None:
        if self._lag_task is not None:
            self._lag_task.cancel()
            try:
                await self._lag_task
            except (asyncio.CancelledError, Exception):
                pass
            self._lag_task = None
        try:
            self._sampler.stop()
        except Exception:
            pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        # 仍在等待中的后台任务（快照落盘等）：取消掉，避免卸载后还在跑
        for task in list(self._bg_tasks):
            task.cancel()
        self._bg_tasks.clear()
        self._save_state(force=True)
        logger.info("[perf_guard] 性能守护已卸载。")

    # ------------------------------------------------------------------
    # 指令 /perf
    # ------------------------------------------------------------------
    @filter.command("perf", alias={"PERF", "性能"})
    async def perf(self, event: AstrMessageEvent, action: str = "", arg: str = ""):
        """/perf 性能守护：状态 / 释放内存 / 清理日志 / 重启（默认仅管理员可用）"""
        if self._cfg("command_admin_only", True) and not self._is_admin(event):
            yield event.plain_result(
                "⛔ 性能指令仅管理员可用。\n"
                "· 群聊里：群主 / 群管理员可直接使用\n"
                "· 私聊里：没有“群管理员”身份，请把你的 QQ 号填进 AstrBot 主配置的 admins_id 名单\n"
                "· 或把本插件的 command_admin_only 关掉（任何人都能用，慎用）"
            )
            return

        text = f"{action} {arg}".strip().lower()
        words = {w for w in text.split() if w}

        if words & HELP_WORDS:
            yield event.plain_result(HELP_TEXT)
            return
        # ---- 新增：一键体检 / 报错统计 / 数据库（都要读磁盘或 SQLite，丢线程）----
        if words & HEALTH_WORDS:
            yield event.plain_result(await asyncio.to_thread(self._health_text))
            return
        if words & ERROR_WORDS:
            yield event.plain_result(self._plugin_error_text())
            return
        if words & DB_WORDS:
            if words & DB_VACUUM_WORDS:
                yield event.plain_result(await self._db_vacuum_text())
            else:
                yield event.plain_result(await asyncio.to_thread(self._db_text))
            return
        if words & DIAG_WORDS:
            yield event.plain_result(self._diag_text())
            return

        # ---- 诊断增强类子指令（必须排在 内存/重启 之前：/perf 内存追踪 含“内存”字样）----
        if words & TRACE_WORDS:
            yield event.plain_result(
                await self._trace_text(stop=bool(words & STOP_WORDS))
            )
            return
        if words & REPORT_WORDS:
            days = self._int_arg(text, 1)
            # 趋势报告要读历史 + 统计，丢线程里做，别卡住其它消息
            yield event.plain_result(await asyncio.to_thread(self._report_text, days))
            return
        if words & SNAPSHOT_WORDS:
            which = self._int_arg(text, 1)
            yield event.plain_result(await asyncio.to_thread(self._snapshot_text, which))
            return
        if words & SLOWEST_WORDS:
            yield event.plain_result(self._slowest_text(self._int_arg(text, 5)))
            return
        if words & RESTART_WORDS:
            msg = await self._restart(f"管理员 {event.get_sender_id()} 手动触发")
            yield event.plain_result(msg)
            return
        if words & CLEAN_WORDS:
            result = await self._clean_logs(reason="手动清理")
            yield event.plain_result(self._clean_result_text(result))
            return
        if words & RELEASE_WORDS:
            result = await self._release_memory(reason="手动释放")
            yield event.plain_result(self._release_result_text(result))
            return

        # ---- AI 自动化管理 ----
        if words & AI_MODEL_WORDS:
            yield event.plain_result(self._ai_models_text())
            return
        if words & PENDING_WORDS:
            yield event.plain_result(self._ai_pending_text())
            return
        if words & APPROVE_WORDS:
            pid = self._pick_ai_id(text, APPROVE_WORDS)
            yield event.plain_result(await self._ai_decide(True, pid, event))
            return
        if words & REJECT_WORDS:
            pid = self._pick_ai_id(text, REJECT_WORDS)
            yield event.plain_result(await self._ai_decide(False, pid, event))
            return
        if words & AI_WORDS:
            umo = getattr(event, "unified_msg_origin", "") or ""
            yield event.plain_result(
                await self._ai_review(f"管理员 {event.get_sender_id()} 手动触发", umo=umo)
            )
            return

        if not words or words & STATUS_WORDS:
            # 状态查询要统计日志目录（磁盘 I/O），丢到线程里做，别卡住其它消息
            yield event.plain_result(await asyncio.to_thread(self._status_text))
            return

        yield event.plain_result("❓ 未识别的子指令。\n\n" + HELP_TEXT)

    # ------------------------------------------------------------------
    # LLM 工具
    # ------------------------------------------------------------------
    @filter.llm_tool(name="perf_guard")
    async def perf_guard_tool(self, event: AstrMessageEvent, action: str = "status"):
        """AstrBot 性能守护：查看性能状态、诊断回复慢的原因、释放内存、清理日志、重启机器人。

        仅在用户明确要求做这些事时才调用。

        Args:
            action (str): 要执行的操作，可选值：
                status —— 查看当前性能状态（内存、运行时长、日志占用、可用重启方式）
                health_check —— 做一次全面体检（插件加载失败、插件报错、模型可用性、磁盘、数据库）
                diagnose —— 诊断机器人回复慢/卡的原因并给出加速建议（用户抱怨慢时用这个）
                release_memory —— 释放运行内存（垃圾回收）
                clean_logs —— 清理日志文件
                restart —— 重启 AstrBot（仅在配置允许时才会真正执行）
                ai_review —— 让 AI 评估当前状态并生成一条待管理员审核的运维提议
        """
        name = str(action or "status").strip().lower()
        if name in ("status", "状态", "info"):
            return await self.get_bot_performance(event)
        if name in ("health_check", "health", "体检", "健康", "check"):
            if self._cfg("llm_admin_only", True) and not self._is_admin(event):
                return self._perm_text("做健康体检")
            return await asyncio.to_thread(self._health_text)
        if name in ("ai_review", "ai", "评审", "评估", "智能管理"):
            # 与其它动作一致的管理员门槛：非管理员不能借 LLM 触发评估（避免白烧 token）
            if self._cfg("llm_admin_only", True) and not self._is_admin(event):
                return self._perm_text("触发 AI 评估")
            return await self._ai_review(
                "LLM 工具调用", umo=getattr(event, "unified_msg_origin", "") or ""
            )
        if name in ("diagnose", "诊断", "slow", "慢", "latency"):
            return await self.diagnose_bot_speed(event)
        if name in ("release_memory", "释放内存", "release", "memory"):
            return await self.release_bot_memory(event)
        if name in ("clean_logs", "清理日志", "clean", "logs"):
            return await self.clean_bot_logs(event)
        if name in ("restart", "重启", "reboot"):
            return await self.restart_bot(event)
        return (
            f"未知操作：{action}。可选值：status / health_check / diagnose / "
            "release_memory / clean_logs / restart / ai_review。"
        )

    # 说明：下面 5 个方法都**故意不再单独注册成 LLM 工具**。
    # 一个插件注册 5 个工具会让每次请求的提示词变长、并和其它插件的工具抢空间
    # （工具太多时模型还会选错）。统一由上面的 perf_guard 一个工具分发。
    async def get_bot_performance(self, event: AstrMessageEvent):
        """查询 AstrBot 的运行性能状态：进程内存占用、运行时长、日志文件占用空间、磁盘剩余空间、累计重启次数、上次清理时间等。当用户询问机器人是否卡顿、内存占用多少、运行多久了、日志多大、磁盘够不够时调用此工具。"""
        if self._cfg("llm_admin_only", True) and not self._is_admin(event):
            return self._perm_text("查询性能状态")
        return self._status_text()

    async def diagnose_bot_speed(self, event: AstrMessageEvent):
        """诊断 AstrBot 回复慢的原因：统计事件循环卡顿次数与延迟、定位造成阻塞的插件代码、统计消息响应耗时（p50/p95/最慢），并给出加速建议。当用户抱怨机器人回复慢、卡、反应迟钝、要求优化速度时调用此工具。"""
        if self._cfg("llm_admin_only", True) and not self._is_admin(event):
            return self._perm_text("做性能诊断")
        return self._diag_text()

    async def release_bot_memory(self, event: AstrMessageEvent):
        """释放 AstrBot 的运行内存：执行垃圾回收并裁剪进程工作集，把空闲物理内存归还系统。当用户要求释放内存、清理内存、让机器人变流畅时调用此工具。"""
        if self._cfg("llm_admin_only", True) and not self._is_admin(event):
            return self._perm_text("释放运行内存")
        result = await self._release_memory(reason="LLM 调用")
        return self._release_result_text(result)

    async def clean_bot_logs(self, event: AstrMessageEvent):
        """清理 AstrBot 的日志文件：按保留天数与目录总大小上限删除最旧的日志。当用户要求清理日志、释放磁盘空间、日志文件太大时调用此工具。"""
        if self._cfg("llm_admin_only", True) and not self._is_admin(event):
            return self._perm_text("清理日志")
        result = await self._clean_logs(reason="LLM 调用")
        return self._clean_result_text(result)

    async def restart_bot(self, event: AstrMessageEvent):
        """重启 AstrBot 本体。仅在用户明确要求重启机器人、且插件配置允许 LLM 重启时才会真正执行。调用前请向用户确认。"""
        if not self._cfg("llm_allow_restart", False):
            return "当前配置不允许由我触发重启（llm_allow_restart=false），请让管理员使用 /perf 重启。"
        if self._cfg("llm_admin_only", True) and not self._is_admin(event):
            return self._perm_text("重启 AstrBot")
        return await self._restart("LLM 调用")

    # ------------------------------------------------------------------
    # 后台监控
    # ------------------------------------------------------------------
    async def _monitor_loop(self) -> None:
        interval = max(15, self._int_cfg("check_interval_seconds", 120))
        while True:
            try:
                await asyncio.sleep(interval)
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[perf_guard] 监控循环异常: {e}")
                await asyncio.sleep(10)

    async def _tick(self) -> None:
        mem = _read_memory_mb()
        threshold = self._int_cfg("memory_threshold_mb", 800)
        ai_alarm = ""

        # 1) 内存超阈值：先释放，连续超标按配置重启
        if mem is not None and threshold > 0:
            cur = mem[0]
            if cur >= threshold:
                self._over_count += 1
                logger.warning(
                    f"[perf_guard] 内存 {cur:.0f}MB 已超过阈值 {threshold}MB（连续第 {self._over_count} 次）"
                )
                await self._release_memory(reason="内存超阈值自动释放")
                ai_alarm = f"内存 {cur:.0f}MB 超过阈值 {threshold}MB（连续第 {self._over_count} 次）"
                await self._write_snapshot("内存告警", ai_alarm)
                need = max(1, self._int_cfg("memory_trigger_times", 3))
                if self._over_count >= need and self._cfg("auto_restart_enabled", False):
                    ok, why = self._can_restart()
                    if ok:
                        await self._restart(
                            f"内存连续 {self._over_count} 次超过 {threshold}MB（当前 {cur:.0f}MB）"
                        )
                    else:
                        logger.warning(f"[perf_guard] 内存超标但暂不重启：{why}")
            else:
                self._over_count = 0

        # 2) 每日定时重启
        if self._cfg("scheduled_restart_enabled", False):
            hhmm = str(self._cfg("scheduled_restart_time", "")).strip()
            today = datetime.now().strftime("%Y-%m-%d")
            if hhmm and datetime.now().strftime("%H:%M") == hhmm and self._state.get("last_scheduled_date") != today:
                self._state["last_scheduled_date"] = today
                self._save_state()
                ok, why = self._can_restart()
                if ok:
                    await self._restart(f"每日定时重启（{hhmm}）")
                else:
                    logger.warning(f"[perf_guard] 到点但暂不重启：{why}")

        # 3) 定期释放内存
        rel_interval = self._int_cfg("release_interval_seconds", 1800)
        if rel_interval > 0 and time.time() - self._last_release_ts >= rel_interval:
            rel_threshold = self._int_cfg("release_threshold_mb", 400)
            if mem is None or (rel_threshold <= 0 or mem[0] >= rel_threshold):
                await self._release_memory(reason="定期自动释放")

        # 4) 定期清理日志
        if self._cfg("log_cleanup_enabled", True):
            hours = max(1, self._int_cfg("log_cleanup_interval_hours", 6))
            if time.time() - self._last_log_clean_ts >= hours * 3600:
                await self._clean_logs(reason="定期自动清理")

        # 5) 历史趋势采样（供 /perf 报告 用；纯内存运算 + 线程落盘）
        await self._history_maybe_sample()

        # 5.5) 插件报错 / 模型失败主动通知（钩子只记录，通知放这里做，避免拖慢出错的那条消息）
        await self._notify_pending_issues()

        # 5.6) 磁盘余量保护
        await self._check_disk_space()

        # 6) 内存追踪到点自动关闭（避免 tracemalloc 一直开着有开销）
        if self._tracer_on:
            maxmin = max(1, self._int_cfg("memory_trace_max_minutes", 15))
            if time.time() - self._tracer_start_ts >= maxmin * 60:
                await self._trace_stop()
                logger.info("[perf_guard] 内存追踪已达时限，已自动关闭并生成报告")

        # 7) AI 自动化管理：只在“有异常”或“到点”时评估，AI 只提议，管理员批准才执行
        if self._cfg("ai_enabled", False):
            lag_total = int(self._state.get("lag_events") or 0)
            new_lag = lag_total > self._ai_lag_mark
            self._ai_lag_mark = lag_total
            interval_min = max(0, self._int_cfg("ai_interval_minutes", 0))
            due = interval_min > 0 and (time.time() - self._ai_last_run_ts) >= interval_min * 60
            alarm = ""
            if self._cfg("ai_trigger_on_alarm", True):
                if ai_alarm:
                    alarm = ai_alarm
                elif new_lag:
                    alarm = f"检测到事件循环卡顿（累计 {lag_total} 次）"
            if alarm or due:
                umo = str(self._cfg("ai_notify_umo", "") or "").strip()
                text = await self._ai_review(
                    alarm or f"定时评估（每 {interval_min} 分钟）", umo=umo, auto=True
                )
                logger.info(f"[perf_guard] AI 自动评估结果：{text.splitlines()[0] if text else ''}")

    # ------------------------------------------------------------------
    # 回复速度诊断
    # ------------------------------------------------------------------
    async def _lag_loop(self) -> None:
        """监测事件循环延迟：延迟 = 实际睡眠耗时 - 期望睡眠耗时。

        事件循环被同步阻塞时，本协程也无法按时醒来，因此测得的差值就是
        "这一瞬间所有消息被卡住的时间"——它直接决定用户体感的回复速度。
        """
        interval = max(0.05, self._int_cfg("lag_check_interval_ms", 1000) / 1000.0)
        threshold = max(0.05, self._int_cfg("lag_threshold_ms", 1000) / 1000.0)
        while True:
            try:
                t0 = time.perf_counter()
                await asyncio.sleep(interval)
                lag = max(0.0, (time.perf_counter() - t0) - interval)
                self._lag_records.append(lag)
                if lag >= threshold:
                    self._on_lag_detected(t0, lag)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug(f"[perf_guard] 卡顿监测异常: {e}")
                await asyncio.sleep(1)

    def _on_lag_detected(self, t_start: float, lag: float) -> None:
        """卡顿超阈值：回捞该时间窗内的主线程栈，锁定阻塞代码。

        注意：本函数运行在「事件循环已经被卡住」的时刻，所以这里每件事都必须极轻——
        写盘与打日志都做了去抖，否则优化插件自己会变成新的卡顿源。
        """
        samples = self._sampler.samples_since(t_start)
        plugin, stack_text = "(未知)", ""
        if samples:
            _, plugin, stack_text = samples[-1]
            self._slow_hits[plugin] = self._slow_hits.get(plugin, 0) + 1
        self._lag_events.append(
            {"ts": time.time(), "lag": lag, "plugin": plugin, "stack": stack_text}
        )
        self._state["lag_events"] = int(self._state.get("lag_events") or 0) + 1
        self._state["max_lag_ms"] = max(
            float(self._state.get("max_lag_ms") or 0.0), lag * 1000.0
        )
        self._save_state()  # 已去抖：默认 20 秒内最多落盘一次
        # 卡顿现场也留一份快照文件（fire-and-forget，不拖慢这里；必须留强引用）
        self._spawn(
            self._write_snapshot("卡顿", f"事件循环卡顿 {lag * 1000:.0f}ms，嫌疑：{plugin}"),
            name="perf_guard:snapshot",
        )
        now = time.time()
        if now - self._last_lag_warn_ts < self._LAG_WARN_INTERVAL:
            logger.debug(
                f"[perf_guard] 事件循环仍在卡顿（{lag * 1000:.0f}ms，{plugin}），已省略日志"
            )
            return
        self._last_lag_warn_ts = now
        tail = ""
        if stack_text:
            tail = "\n".join(
                line.strip() for line in stack_text.strip().splitlines()[-5:] if line.strip()
            )
        logger.warning(
            f"[perf_guard] ⚠️ 事件循环卡顿 {lag * 1000:.0f}ms，嫌疑插件：{plugin}\n{tail}"
        )

    def _cpu_percent(self) -> float | None:
        try:
            import psutil  # type: ignore

            return psutil.Process(os.getpid()).cpu_percent(interval=None)
        except Exception:
            return None

    @staticmethod
    def _percentile(values: list[float], pct: float) -> float:
        if not values:
            return 0.0
        data = sorted(values)
        idx = int(round((pct / 100.0) * (len(data) - 1)))
        return data[max(0, min(len(data) - 1, idx))]

    # --- 消息耗时记录（由 on_llm_request / on_decorating_result 钩子调用） ---
    def _event_key(self, event: AstrMessageEvent) -> str:
        try:
            return f"{id(event)}|{event.unified_msg_origin}|{event.get_sender_id()}"
        except Exception:
            return str(id(event))

    def _mark_llm_start(self, event: AstrMessageEvent) -> None:
        try:
            self._prune_pending()
            key = self._event_key(event)
            now = time.time()
            self._pending_llm[key] = now
            self._pending_msg.setdefault(key, now)
        except Exception:
            pass

    def _record_llm_done(self, event: AstrMessageEvent) -> None:
        try:
            key = self._event_key(event)
            t0 = self._pending_llm.pop(key, None)
            if t0:
                cost = max(0.0, time.time() - t0)
                self._llm_latency.append(cost)
                # 留给 _record_message_done 算“模型之后到发出去”的那一段
                self._pending_llm_cost[key] = (cost, time.time())
        except Exception:
            pass

    def _record_message_done(self, event: AstrMessageEvent) -> None:
        try:
            key = self._event_key(event)
            now = time.time()
            t0 = self._pending_msg.pop(key, None) or self._pending_llm.pop(key, None)
            llm_info = self._pending_llm_cost.pop(key, None)
            if not t0:
                return
            cost = max(0.0, now - t0)
            self._msg_latency.append(cost)
            self._msg_total += 1
            self._state["slowest_ms"] = max(
                float(self._state.get("slowest_ms") or 0.0), cost * 1000.0
            )

            # 三段拆分：预处理（收到→开始请求模型）/ 模型 / 后处理（模型结束→发出）
            llm_cost = llm_info[0] if llm_info else 0.0
            llm_end = llm_info[1] if llm_info else now
            llm_start = llm_end - llm_cost
            pre = max(0.0, llm_start - t0) if llm_info else 0.0
            post = max(0.0, now - llm_end) if llm_info else 0.0
            self._last_phases = (pre, llm_cost, post)

            limit = float(self._int_cfg("slow_message_seconds", 5) or 5)
            if cost >= limit:
                lag_hit, lag_plugin, lag_ms = 0.0, "", 0.0
                for ev in self._lag_events:
                    if t0 <= float(ev.get("ts") or 0) <= now:
                        lag_hit += 1
                        if float(ev.get("lag") or 0) * 1000.0 > lag_ms:
                            lag_ms = float(ev.get("lag") or 0) * 1000.0
                            lag_plugin = str(ev.get("plugin") or "")
                self._slow_messages.append(
                    {
                        "ts": now,
                        "umo": str(getattr(event, "unified_msg_origin", "") or ""),
                        "sender": self._safe_sender(event),
                        "total": cost,
                        "pre": pre,
                        "llm": llm_cost,
                        "post": post,
                        "lag_hit": lag_hit,
                        "lag_ms": lag_ms,
                        "lag_plugin": lag_plugin,
                    }
                )
        except Exception:
            pass

    @staticmethod
    def _safe_sender(event: AstrMessageEvent) -> str:
        try:
            return str(event.get_sender_id())
        except Exception:
            return "?"

    # --- 埋点钩子：AstrBot 版本缺少某个钩子时自动降级，不影响其它功能 ---
    def _prune_pending(self) -> None:
        """防止“没有回复”的消息把待统计表撑大。"""
        try:
            for table in (self._pending_msg, self._pending_llm, self._pending_llm_cost):
                if len(table) > 200:
                    old = sorted(table, key=lambda k: self._pending_ts(table, k))[:50]
                    for k in old:
                        table.pop(k, None)
        except Exception:
            pass

    @staticmethod
    def _pending_ts(table: dict[str, Any], key: str) -> float:
        val = table.get(key)
        if isinstance(val, tuple) and val:
            return float(val[-1])
        try:
            return float(val)
        except Exception:
            return 0.0
            pass

    def _mark_message_start(self, event: AstrMessageEvent) -> None:
        try:
            self._prune_pending()
            # created_at 是事件对象创建时间，比钩子触发更接近“用户发出消息”
            try:
                t0 = float(event.created_at)
            except Exception:
                t0 = time.time()
            self._pending_msg.setdefault(self._event_key(event), t0 or time.time())
            if bool(self._cfg("force_streaming", False)):
                event.set_extra("enable_streaming", True)
        except Exception:
            pass

    def _trim_context(self, req: Any) -> None:
        """按配置裁剪本次请求的上下文，缩短提示词以加快首字响应。

        只在 user 消息边界处裁剪，避免留下没有对应 tool_call 的孤儿 tool 结果。
        """
        limit = self._int_cfg("max_context_messages", 0)
        if limit <= 0 or req is None:
            return
        try:
            ctx = getattr(req, "contexts", None)
            if not isinstance(ctx, list) or len(ctx) <= limit:
                return
            before = len(ctx)
            start = before - limit
            while start < before:
                item = ctx[start]
                role = str((item or {}).get("role") or "") if isinstance(item, dict) else ""
                if role == "user":
                    break
                start += 1
            if start <= 0 or start >= before:
                return
            # 用「整体替换」而不是原地 del：其它插件可能也持有同一个列表引用，
            # 原地删会改变它们的视图（这是插件互相干扰的隐患）
            req.contexts = ctx[start:]
            self._last_ctx = (before, len(req.contexts))
            self._state["trimmed_messages"] = int(
                self._state.get("trimmed_messages") or 0
            ) + (before - len(req.contexts))
        except Exception as e:
            logger.debug(f"[perf_guard] 裁剪上下文失败: {e}")

    async def _hook_message_in(
        self, event: AstrMessageEvent, *args: Any, **kwargs: Any
    ) -> None:
        self._mark_message_start(event)

    def _scan_tool_names(self, req: Any) -> None:
        """尽力而为地统计本次请求携带的 LLM 工具，并检测重名。

        为什么值得做：工具重名会让模型服务商直接报
        `duplicate function name`，**整条请求都会失败**，表现却是“某个插件坏了”，
        极难排查。这里发现有重名就立刻在日志里点名，并写进 /perf 状态。
        """
        try:
            tool_set = getattr(req, "func_tool", None)
            if tool_set is None:
                return
            tools = getattr(tool_set, "tools", None)
            if tools is None:
                tools = tool_set if isinstance(tool_set, (list, tuple)) else None
            if not tools:
                return
            names: list[str] = []
            for t in list(tools)[:200]:  # 上限，避免异常结构拖慢
                name = getattr(t, "name", None)
                if name is None and isinstance(t, dict):
                    name = (t.get("function") or {}).get("name") or t.get("name")
                if name:
                    names.append(str(name))
            if not names:
                return
            self._tool_count = len(names)
            dupes = sorted({n for n in names if names.count(n) > 1})
            if dupes:
                self._dupe_tools = dupes
                if not self._dupe_warned:
                    self._dupe_warned = True
                    logger.warning(
                        "[perf_guard] ⚠️ 检测到 LLM 工具重名：%s\n"
                        "重名会让模型服务商报 duplicate function name，导致整条请求失败。"
                        "请停用注册了同名工具的插件（本插件只注册一个 perf_guard，通常不是它）。",
                        "、".join(dupes),
                    )
            else:
                self._dupe_tools = []  # 重名已解除时要清掉，避免一直误报
                self._dupe_warned = False
        except Exception:
            pass

    async def _hook_llm_request(
        self, event: AstrMessageEvent, req: Any = None, *args: Any, **kwargs: Any
    ) -> None:
        self._mark_llm_start(event)
        self._trim_context(req)
        self._scan_tool_names(req)

    async def _hook_llm_response(
        self, event: AstrMessageEvent, response: Any = None, *args: Any, **kwargs: Any
    ) -> None:
        self._record_llm_done(event)
        self._record_model_response(response)

    def _record_model_response(self, response: Any) -> None:
        """统计模型响应，识别失败（role='err'）。

        为什么值得做：中转/模型挂掉时，AstrBot 的表现是"机器人不回话"，
        用户完全看不出是模型的问题还是自己插件的问题。这里把失败次数与最后一次
        失败原因记下来，`/perf 状态`、`/perf 诊断`、`/perf 体检` 都会显示。
        """
        if response is None:
            return
        try:
            self._model_calls += 1
            role = str(getattr(response, "role", "") or "")
            if role != "err":
                return
            self._model_errs += 1
            text = ""
            for attr in ("completion_text", "result", "_completion_text"):
                val = getattr(response, attr, None)
                if val:
                    text = str(val)
                    break
            self._model_err_events.append(
                {"ts": time.time(), "text": " ".join(text.split())[:200]}
            )
            self._state["model_errs"] = int(self._state.get("model_errs") or 0) + 1
            self._save_state()
        except Exception:
            pass

    async def _hook_plugin_error(
        self,
        event: AstrMessageEvent,
        plugin_name: str = "",
        handler_name: str = "",
        error: Any = None,
        traceback_text: str = "",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """别的插件处理消息抛异常时被调用（AstrBot 4.27.4 的 on_plugin_error）。"""
        self._record_plugin_error(plugin_name, handler_name, error, traceback_text)

    def _record_plugin_error(
        self, plugin_name: str, handler_name: str, error: Any, traceback_text: str = ""
    ) -> None:
        """记录插件报错并聚合，等监控循环去通知管理员（钩子里绝不做慢操作）。"""
        try:
            name = str(plugin_name or "未知插件")
            handler = str(handler_name or "")
            err_text = f"{type(error).__name__}: {error}" if error is not None else "未知异常"
            now = time.time()
            self._plugin_error_hits[name] = self._plugin_error_hits.get(name, 0) + 1
            self._state["plugin_errors"] = int(self._state.get("plugin_errors") or 0) + 1
            self._state["last_plugin_error"] = f"{name}/{handler}: {err_text[:160]}"
            self._state["last_plugin_error_ts"] = now
            self._save_state()

            # 同一条错误（同插件+同异常）合并计数，避免刷屏
            key = f"{name}|{err_text[:80]}"
            found = None
            for item in self._plugin_errors:
                if item.get("key") == key:
                    found = item
                    break
            if found is not None:
                found["count"] = int(found.get("count") or 1) + 1
                found["ts"] = now
                found["last_trace"] = " ".join(str(traceback_text or "").splitlines()[-1:])[:200]
            else:
                self._plugin_errors.append(
                    {
                        "key": key,
                        "ts": now,
                        "plugin": name,
                        "handler": handler,
                        "error": err_text[:200],
                        "count": 1,
                        "last_trace": " ".join(str(traceback_text or "").splitlines()[-1:])[:200],
                    }
                )

            agg = self._err_notify_queue.setdefault(
                name, {"first_ts": now, "last_ts": now, "count": 0, "error": err_text[:160]}
            )
            agg["last_ts"] = now
            agg["count"] = int(agg.get("count") or 0) + 1
            agg["error"] = err_text[:160]

            logger.warning(
                f"[perf_guard] 检测到插件报错：{name} / {handler} → {err_text}"
            )
        except Exception:
            pass

    def _plugin_error_text(self, limit: int = 8) -> str:
        """插件报错排行与明细。"""
        if not self._plugin_errors:
            return (
                "✅ 还没有捕获到任何插件报错。\n"
                "（本插件会接住其它插件抛出的异常并在这里点名；"
                "若本插件在启动日志里提示钩子不可用，则说明当前 AstrBot 版本没有 on_plugin_error）"
            )
        total = sum(int(i.get("count") or 1) for i in self._plugin_errors)
        lines = [f"🧯 插件报错统计（本进程累计 {total} 次，最近 {len(self._plugin_errors)} 类）", ""]
        rank = sorted(self._plugin_error_hits.items(), key=lambda kv: kv[1], reverse=True)[:limit]
        if rank:
            lines.append("按插件排行：")
            for name, cnt in rank:
                lines.append(f"· {name} ×{cnt}")
            lines.append("")
        lines.append("最近几类错误（同类已合并计数）：")
        for item in list(self._plugin_errors)[-limit:][::-1]:
            lines.append(
                f"· [{_fmt_ts(item.get('ts'))}] {item.get('plugin')} / {item.get('handler')}"
                f"（×{item.get('count')}）\n    {item.get('error')}"
            )
        lines.append("")
        lines.append(
            "建议：报错最多的那个插件最值得先看。可以在 WebUI 里把它临时停用做个对照；"
            "报错本身通常不是性能问题，但异常处理不当中会连带拖慢整条消息。"
        )
        return "\n".join(lines)

    def _model_health_line(self) -> str:
        """模型调用健康度（一行）。"""
        if self._model_calls <= 0:
            return "· 模型调用：暂无样本（本插件启动后还没观察到模型响应）"
        rate = self._model_errs / self._model_calls * 100.0
        extra = ""
        if self._model_err_events:
            extra = f"，最近一次：{self._model_err_events[-1].get('text', '')[:80]}"
        return (
            f"· 模型调用：{self._model_calls} 次，失败 {self._model_errs} 次（{rate:.1f}%）{extra}"
        )

    async def _hook_after_sent(
        self, event: AstrMessageEvent, *args: Any, **kwargs: Any
    ) -> None:
        self._record_message_done(event)

    # priority 让埋点尽量跑在其它插件之前/之后，计时更接近真实耗时
    if _HOOK_EVENT_MESSAGE_TYPE is not None and _EVENT_MSG_TYPE_ALL is not None:
        _hook_message_in = _apply_optional_hook(
            lambda: _HOOK_EVENT_MESSAGE_TYPE(_EVENT_MSG_TYPE_ALL, priority=100),
            _hook_message_in,
        )
    _hook_llm_request = _apply_optional_hook(
        lambda: _HOOK_ON_LLM_REQUEST(priority=100), _hook_llm_request
    )
    _hook_llm_response = _apply_optional_hook(
        lambda: _HOOK_ON_LLM_RESPONSE(priority=-100), _hook_llm_response
    )
    _hook_after_sent = _apply_optional_hook(
        lambda: _HOOK_AFTER_MESSAGE_SENT(priority=-100), _hook_after_sent
    )
    _hook_plugin_error = _apply_optional_hook(
        lambda: _HOOK_ON_PLUGIN_ERROR(priority=-100), _hook_plugin_error
    )

    def _self_overhead_text(self) -> str:
        """自报常驻开销，让用户能直接验证「优化插件本身没有拖慢机器人」。"""
        try:
            if not self._cfg("stack_sampling_enabled", True):
                return (
                    "堆栈采样已关闭（卡顿仍可统计，但定位不到具体插件）；"
                    f"循环监测每 {self._int_cfg('lag_check_interval_ms', 1000)}ms 唤醒一次"
                )
            wall = max(0.0, time.time() - (self._sampler.started_ts or time.time()))
            cpu = float(self._sampler.cpu_seconds or 0.0)
            if wall <= 1.0:
                return "刚启动，暂无数据"
            pct = cpu / wall * 100.0
            return (
                f"采样线程累计 CPU {cpu * 1000:.1f}ms / 运行 {_fmt_duration(wall)}"
                f"（约 {pct:.3f}%，折合每小时 {pct / 100 * 3600:.1f} 秒 CPU），"
                f"每 {self._sampler.interval * 1000:.0f}ms 采一次，共 {self._sampler.samples} 次"
            )
        except Exception:
            return "无法读取"

    def _official_watchdog_log(self) -> str:
        """AstrBot 自带的事件循环看门狗日志（默认 15 秒阈值，命中时写文件）。"""
        try:
            for d in self._log_dirs():
                f = d / "event_loop_watchdog.log"
                if f.is_file():
                    ts = datetime.fromtimestamp(f.stat().st_mtime).strftime("%m-%d %H:%M")
                    return f"{f}（最后更新 {ts}）"
        except Exception:
            pass
        return ""

    def _diag_text(self) -> str:
        lag = list(self._lag_records)
        threshold = self._int_cfg("lag_threshold_ms", 1000)
        over = sum(1 for v in lag if v * 1000.0 >= threshold)
        msg = list(self._msg_latency)
        llm = list(self._llm_latency)
        mem = _read_memory_mb()
        cpu = self._cpu_percent()

        lines = ["🩺 AstrBot 回复速度诊断", ""]

        # 1) 消息响应耗时
        if msg:
            lines.append(f"📨 消息响应耗时（最近 {len(msg)} 条）")
            lines.append(f"· 中位数 p50：{self._percentile(msg, 50):.2f}s")
            lines.append(f"· 长尾 p95：{self._percentile(msg, 95):.2f}s")
            lines.append(f"· 最慢：{max(msg):.2f}s")
            if llm:
                p50_llm = self._percentile(llm, 50)
                lines.append(
                    f"· 其中 LLM 调用：p50 {p50_llm:.2f}s / p95 {self._percentile(llm, 95):.2f}s"
                )
                p50_msg = self._percentile(msg, 50)
                if p50_msg > 0:
                    lines.append(f"· LLM 占响应总耗时约 {p50_llm / p50_msg * 100:.0f}%")
            if self._last_phases:
                pre, llm_c, post = self._last_phases
                lines.append(f"· 最近一条消息拆分：{self._fmt_phases(pre, llm_c, post)}")
        else:
            lines.append("📨 消息响应耗时：暂无样本（先聊几句再看）")
        lines.append("")

        # 2) 事件循环卡顿
        lines.append(f"⚡ 事件循环卡顿（{len(lag)} 次采样，阈值 {threshold}ms）")
        if lag:
            lines.append(
                f"· 延迟 p50 {self._percentile(lag, 50) * 1000:.0f}ms / "
                f"p95 {self._percentile(lag, 95) * 1000:.0f}ms / 最大 {max(lag) * 1000:.0f}ms"
            )
            lines.append(f"· 超过阈值：{over} 次")
        else:
            lines.append("· 暂无采样")
        if self._slow_hits:
            top = sorted(self._slow_hits.items(), key=lambda kv: kv[1], reverse=True)[:5]
            lines.append("· 卡顿嫌疑：" + "，".join(f"{n} ×{c}" for n, c in top))
        lines.append("")

        # 3) 资源
        lines.append("🖥 资源占用")
        lines.append(
            f"· 内存：{f'{mem[0]:.0f} MB（峰值 {mem[1]:.0f} MB）' if mem else '未知'}"
        )
        lines.append(f"· CPU：{f'{cpu:.0f}%' if cpu is not None else '未知（未安装 psutil）'}")
        lines.append(f"· 本插件自身开销：{self._self_overhead_text()}")
        if self._last_ctx:
            b, a = self._last_ctx
            lines.append(
                f"· 上下文裁剪：上次请求 {b} 条 → {a} 条"
                f"（累计裁掉 {int(self._state.get('trimmed_messages') or 0)} 条）"
            )
        wd = self._official_watchdog_log()
        if wd:
            lines.append(f"· 官方卡顿日志：{wd}")

        # 4) 最近一次卡顿现场
        if self._lag_events:
            ev = self._lag_events[-1]
            tail = [
                line.strip()
                for line in str(ev.get("stack") or "").strip().splitlines()[-4:]
                if line.strip()
            ]
            if tail:
                lines.append("")
                lines.append(
                    f"🔍 最近一次卡顿现场（{ev['lag'] * 1000:.0f}ms，嫌疑：{ev['plugin']}）栈尾 4 行："
                )
                lines.extend(f"   {t}" for t in tail)

        # 5) 慢消息与内存走势（新）
        if self._slow_messages:
            lines.append("")
            lines.append(
                f"🐌 最近的慢消息（累计 {len(self._slow_messages)} 条，"
                f"超过 {self._int_cfg('slow_message_seconds', 5)} 秒才记录）"
            )
            for r in list(self._slow_messages)[-3:][::-1]:
                extra = (
                    f"｜期间卡顿 {int(r['lag_hit'])} 次"
                    + (f"（{r['lag_plugin']}）" if r.get("lag_plugin") else "")
                    if r.get("lag_hit")
                    else ""
                )
                lines.append(
                    f"· {_fmt_ts(r.get('ts'))} 总 {float(r.get('total') or 0):.1f}s"
                    f"（{self._fmt_phases(r.get('pre') or 0, r.get('llm') or 0, r.get('post') or 0)}）{extra}"
                )
            lines.append("  详细排行：/perf 最慢")
        if len(self._history) >= 6:
            mems = [float(s.get("mem") or 0) for s in self._history[-48:]]
            lines.append("")
            lines.append(f"📈 内存走势（最近 {len(mems)} 个采样点）：{self._sparkline(mems)}")
            lines.append(
                f"   区间 {mems[0]:.0f} → {mems[-1]:.0f} MB（峰值 {max(mems):.0f} MB）"
                "，详细趋势：/perf 报告 3"
            )
        if self._trace_report:
            lines.append("")
            lines.append("🔬 有一次内存追踪报告待查看：/perf 内存追踪")

        # 5.5) 插件报错与模型健康（“慢”之外的另一半问题：不回话 / 报错）
        lines.append("")
        lines.append("🧯 插件报错与模型")
        err_total = sum(int(i.get("count") or 1) for i in self._plugin_errors)
        if err_total:
            top = sorted(self._plugin_error_hits.items(), key=lambda kv: kv[1], reverse=True)[:5]
            lines.append(f"· 插件报错累计 {err_total} 次：" + "，".join(f"{n} ×{c}" for n, c in top))
            recent = list(self._plugin_errors)[-2:][::-1]
            for item in recent:
                lines.append(
                    f"  - {item.get('plugin')} / {item.get('handler')}：{item.get('error')}"
                )
            lines.append("  详情与排行：/perf 报错")
        else:
            lines.append("· 插件报错：未捕获到（正常）")
        lines.append(self._model_health_line())
        if self._model_err_events:
            for ev in list(self._model_err_events)[-2:]:
                lines.append(f"  - {_fmt_ts(ev.get('ts'))} {ev.get('text')}")

        # 6) 建议
        lines.append("")
        lines.append("💡 加速建议")
        tips = self._advice(lag, msg, llm, mem, cpu, threshold)
        tips = self._extra_tips(tips)
        lines.extend(tips)
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # /perf 体检：一键健康检查（把散落各处的判断汇成一份可执行的清单）
    # ------------------------------------------------------------------
    def _disk_free_gb(self, path: str | None = None) -> float | None:
        """探测磁盘剩余空间（GB）。路径不存在时逐级向上找，尽量给个结果。"""
        try:
            probe = Path(path or self._data_dir or ".")
            while not probe.exists() and probe != probe.parent:
                probe = probe.parent
            return shutil.disk_usage(str(probe)).free / 1073741824.0
        except Exception:
            return None

    def _dep_status(self) -> list[tuple[str, bool, str]]:
        """关键依赖是否可用（缺失只会少功能，不会崩，但用户应该知道）。"""
        out: list[tuple[str, bool, str]] = []
        try:
            import psutil  # noqa: F401

            out.append(("psutil（读内存/CPU）", True, "已安装"))
        except Exception:
            out.append(("psutil（读内存/CPU）", False, "未安装 → 内存阈值、自动重启都失效，建议 pip install psutil"))
        try:
            import jwt  # noqa: F401

            out.append(("PyJWT（Dashboard 优雅重启）", True, "已安装"))
        except Exception:
            out.append(("PyJWT（Dashboard 优雅重启）", False, "未安装 → 只能用进程重启/看门狗，或 pip install PyJWT"))
        return out

    def _failed_plugins(self) -> list[tuple[str, str]]:
        """加载失败的插件清单。

        AstrBot 把加载失败的插件记录在 StarManager.failed_plugin_dict 里，
        而 StarManager 挂在 context._star_manager 上（core/star/star_manager.py:198）。
        加载失败的插件是"静默故障"——它在插件列表里可能看着像存在，但功能全无。
        """
        out: list[tuple[str, str]] = []
        try:
            mgr = getattr(self.context, "_star_manager", None)
            failed = getattr(mgr, "failed_plugin_dict", None) or {}
            if isinstance(failed, dict):
                for dir_name, info in failed.items():
                    reason = ""
                    shown = str(dir_name)
                    if isinstance(info, dict):
                        # 记录形如 {"name","error","traceback","reserved", ...metadata}
                        shown = str(info.get("name") or dir_name)
                        reason = str(
                            info.get("error")
                            or info.get("message")
                            or info.get("reason")
                            or info.get("traceback")
                            or ""
                        )
                    elif info:
                        reason = str(info)
                    # 只保留异常首行，AstrBot 的完整栈留给日志
                    reason = " ".join(reason.split())
                    out.append((shown, reason[:160]))
        except Exception:
            pass
        return out

    def _astrbot_version(self) -> str:
        try:
            import astrbot

            return str(getattr(astrbot, "__version__", "") or "未知")
        except Exception:
            return "未知"

    def _db_files(self) -> list[Path]:
        """AstrBot 主数据库及其 WAL/SHM 附属文件。"""
        candidates: list[Path] = []
        try:
            from astrbot.core.config.default import DB_PATH  # type: ignore

            candidates.append(Path(DB_PATH))
        except Exception:
            pass
        try:
            # 兜底推导：<root>/data/plugin_data/<插件名> → <root>/data/data_v4.db
            data_root = Path(self._data_dir).parent.parent if self._data_dir else None
            if data_root:
                candidates.append(data_root / "data_v4.db")
        except Exception:
            pass
        seen: set[str] = set()
        out: list[Path] = []
        for base in candidates:
            if not base:
                continue
            for p in (base, Path(str(base) + "-wal"), Path(str(base) + "-shm")):
                key = str(p).lower()
                if key in seen:
                    continue
                seen.add(key)
                if p.exists():
                    out.append(p)
        return out

    def _db_stats(self) -> dict[str, Any]:
        """数据库体积与页统计（只读连接，不会写坏数据）。"""
        info: dict[str, Any] = {"files": [], "total_mb": 0.0, "page": None, "freelist": None}
        for p in self._db_files():
            try:
                size = p.stat().st_size
            except Exception:
                continue
            info["files"].append((p.name, size))
            info["total_mb"] += size / 1048576.0
        main_file = next((p for p in self._db_files() if p.suffix == ".db"), None)
        if main_file is not None:
            try:
                import sqlite3

                con = sqlite3.connect(f"file:{main_file}?mode=ro", uri=True, timeout=2.0)
                try:
                    cur = con.cursor()
                    cur.execute("PRAGMA page_count")
                    page_count = int(cur.fetchone()[0])
                    cur.execute("PRAGMA page_size")
                    page_size = int(cur.fetchone()[0])
                    cur.execute("PRAGMA freelist_count")
                    free = int(cur.fetchone()[0])
                    info["page"] = (page_count, page_size, free)
                    info["freelist"] = free * page_size / 1048576.0
                finally:
                    con.close()
            except Exception as e:
                info["page_error"] = str(e)[:120]
        return info

    def _health_text(self) -> str:
        """一键体检：把"要不要担心、该做什么"一次说清。"""
        items: list[tuple[str, str, str]] = []  # (级别, 项目, 说明)
        todo: list[str] = []

        # 1) 版本与插件
        stars = []
        try:
            stars = list(self.context.get_all_stars() or [])
        except Exception:
            pass
        ver = self._astrbot_version()
        items.append(("ok" if ver != "未知" else "warn", "AstrBot 版本", ver))
        failed = self._failed_plugins()
        if failed:
            items.append(
                (
                    "bad",
                    "插件加载失败",
                    "；".join(f"{n}（{r or '原因见日志'}）" for n, r in failed[:5]),
                )
            )
            todo.append("有插件加载失败 → WebUI 插件页看报错原因，改好或用插件页的“重载”")
        else:
            items.append(("ok", "已加载插件", f"{len(stars)} 个，无加载失败记录"))

        # 2) 插件报错
        err_total = sum(int(i.get("count") or 1) for i in self._plugin_errors)
        if err_total:
            top = sorted(self._plugin_error_hits.items(), key=lambda kv: kv[1], reverse=True)[:3]
            items.append(
                (
                    "warn" if err_total < 10 else "bad",
                    "插件运行报错",
                    f"累计 {err_total} 次；最多：" + "、".join(f"{n} ×{c}" for n, c in top),
                )
            )
            todo.append("看 `/perf 报错` 定位是哪个插件在报错（报错插件常连带拖慢整条消息）")
        else:
            items.append(("ok", "插件运行报错", "未捕获到任何插件异常"))

        # 3) 模型可用性
        if self._model_calls:
            rate = self._model_errs / self._model_calls * 100.0
            if self._model_errs == 0:
                items.append(("ok", "模型调用", f"{self._model_calls} 次全部成功"))
            else:
                items.append(
                    (
                        "warn" if rate < 20 else "bad",
                        "模型调用",
                        f"{self._model_calls} 次中失败 {self._model_errs} 次（{rate:.1f}%）"
                        + (
                            f"；最近：{self._model_err_events[-1].get('text', '')[:60]}"
                            if self._model_err_events
                            else ""
                        ),
                    )
                )
                if rate >= 20:
                    todo.append("模型失败率偏高 → 检查 API 中转/密钥/额度（`/perf 状态` 有最近失败原因）")
        else:
            items.append(("warn", "模型调用", "本次启动后还没有观察到模型响应（若机器人一直不回话，重点看这里）"))

        # 4) 内存与事件循环
        mem = _read_memory_mb()
        threshold = self._int_cfg("memory_threshold_mb", 800)
        if mem:
            cur = mem[0]
            if threshold > 0 and cur >= threshold:
                items.append(("bad", "进程内存", f"{cur:.0f} MB，已超过阈值 {threshold} MB"))
                todo.append("内存已超标 → `/perf 释放内存`，持续超标再考虑重启")
            else:
                items.append(
                    (
                        "ok",
                        "进程内存",
                        f"{cur:.0f} MB（峰值 {mem[1]:.0f} MB，阈值 {threshold} MB）",
                    )
                )
        else:
            items.append(("warn", "进程内存", "读不到（未安装 psutil）"))
            todo.append("装 psutil：pip install psutil（否则内存阈值与自动重启不生效）")

        lag = list(self._lag_records)
        if lag:
            p95 = self._percentile(lag, 95) * 1000.0
            worst = max(lag) * 1000.0
            limit = self._int_cfg("lag_threshold_ms", 1000)
            lvl = "ok" if worst < limit else ("warn" if p95 < limit else "bad")
            items.append(
                (lvl, "事件循环卡顿", f"p95 {p95:.0f}ms / 最大 {worst:.0f}ms（阈值 {limit}ms，采样 {len(lag)} 次）")
            )
            if lvl == "bad":
                todo.append("卡顿明显 → `/perf 诊断` 看是哪段慢（含嫌疑插件）")
        else:
            items.append(("warn", "事件循环卡顿", "暂无采样（插件刚启动或采样被关闭）"))

        msg = list(self._msg_latency)
        if msg:
            items.append(
                (
                    "ok",
                    "消息响应",
                    f"p50 {self._percentile(msg, 50):.2f}s / p95 {self._percentile(msg, 95):.2f}s（{len(msg)} 条样本）",
                )
            )
        else:
            items.append(("warn", "消息响应", "暂无样本"))

        # 5) 工具重名（会让整条请求失败）
        if self._dupe_tools:
            items.append(("bad", "LLM 工具重名", "、".join(self._dupe_tools)))
            todo.append("工具重名会让模型服务商直接报错 → 停用注册了同名工具的插件")
        else:
            items.append(
                ("ok", "LLM 工具", f"本次请求携带 {self._tool_count} 个，无重名" if self._tool_count else "暂无请求样本")
            )

        # 6) 磁盘与日志
        free_gb = self._disk_free_gb()
        warn_gb = float(self._int_cfg("disk_free_warn_gb", 3) or 0)
        if free_gb is None:
            items.append(("warn", "磁盘剩余", "无法读取"))
        elif warn_gb > 0 and free_gb < warn_gb:
            items.append(("bad", "磁盘剩余", f"{free_gb:.1f} GB，已低于警戒线 {warn_gb:.0f} GB"))
            todo.append("磁盘快满了 → `/perf 清理日志`，并检查其它程序占用")
        else:
            items.append(("ok", "磁盘剩余", f"{free_gb:.1f} GB"))

        try:
            dirs = self._log_dirs()
            files = self._collect_log_files(dirs)
            log_mb = sum(_safe_size(f) for f in files) / 1048576.0
            items.append(("ok", "日志占用", f"{len(files)} 个文件 / {log_mb:.1f} MB"))
        except Exception:
            pass

        # 7) 数据库
        db = self._db_stats()
        if db.get("files"):
            detail = f"共 {db['total_mb']:.1f} MB（" + "、".join(
                f"{n} {s / 1048576.0:.1f}MB" for n, s in db["files"]
            ) + "）"
            if db.get("freelist"):
                detail += f"，可回收约 {db['freelist']:.1f} MB"
            items.append(("ok" if db["total_mb"] < 512 else "warn", "数据库体积", detail))
            if db.get("freelist") and db["freelist"] >= 50:
                todo.append("数据库有较多空闲页 → `/perf 数据库 整理 yes` 可回收（会短暂锁库，建议闲时做）")
        else:
            items.append(("warn", "数据库体积", "未找到 data_v4.db（可能是全新安装）"))

        # 8) 重启能力与自动化
        caps = self._restart_capabilities()
        avail = [
            label
            for label, ok in (
                ("自定义入口", caps["hook"]),
                ("Dashboard 接口", caps["api"]),
                ("进程重启", caps["process"]),
                ("看门狗", caps["watchdog"]),
            )
            if ok
        ]
        items.append(
            (
                "ok" if avail else "bad",
                "可用重启方式",
                "、".join(avail) if avail else "一种都没有 → 重启功能不可用",
            )
        )
        if not avail:
            todo.append("重启不可用 → 用 watchdog/run_with_watchdog.bat 启动，或配好 Dashboard 账号")

        if not self._cfg("auto_restart_enabled", False):
            items.append(("ok", "自动重启", "已关闭（不会自己重启机器人）"))
        else:
            items.append(("warn", "自动重启", "已开启：内存连续超标会自动重启"))

        if self._cfg("ai_enabled", False):
            interval_min = max(0, self._int_cfg("ai_interval_minutes", 0))
            trig = bool(self._cfg("ai_trigger_on_alarm", True))
            if interval_min <= 0 and not trig:
                items.append(("warn", "AI 自动评估", "已开启但既没设间隔、也没开“异常时评估”→ 实际永远不会自动跑"))
                todo.append("AI 自动评估不会触发 → 设 ai_interval_minutes 或打开 ai_trigger_on_alarm")
            else:
                items.append(
                    (
                        "ok",
                        "AI 自动评估",
                        f"已开启（间隔 {interval_min} 分钟 / 异常时{'评估' if trig else '不评估'}）",
                    )
                )

        # 9) 依赖
        for name, ok, why in self._dep_status():
            items.append(("ok" if ok else "warn", name, why))
            if not ok:
                todo.append(f"{name}：{why}")

        # 10) 权限（群聊/私聊差异，最容易踩的坑）
        items.append(("ok", "谁能用这些指令", self._admin_status_line().removeprefix("· ")))

        # 输出
        icon = {"ok": "✅", "warn": "⚠️", "bad": "❌"}
        bad = sum(1 for lv, _, _ in items if lv == "bad")
        warn = sum(1 for lv, _, _ in items if lv == "warn")
        if bad:
            head = f"🩺 体检结果：发现 {bad} 个问题" + (f"，另有 {warn} 项需要留意" if warn else "")
        elif warn:
            head = f"🩺 体检结果：没有严重问题，{warn} 项需要留意"
        else:
            head = "🩺 体检结果：一切正常"

        lines = [head, ""]
        for lv, title, detail in items:
            lines.append(f"{icon.get(lv, '·')} {title}：{detail}")
        lines.append("")
        if todo:
            lines.append("💡 建议按这个顺序处理：")
            seen: set[str] = set()
            idx = 0
            for t in todo:
                if t in seen:
                    continue
                seen.add(t)
                idx += 1
                lines.append(f"{idx}. {t}")
        else:
            lines.append("💡 没发现需要处理的问题。想更细地看速度：`/perf 诊断`；看趋势：`/perf 报告 7`。")
        return "\n".join(lines)

    def _db_text(self) -> str:
        """数据库体积明细（只读）。"""
        db = self._db_stats()
        lines = ["🗄 AstrBot 数据库状态", ""]
        if not db.get("files"):
            lines.append("· 没找到 data_v4.db（全新安装或路径不标准）")
            lines.append("· 该文件通常在 <AstrBot>/data/data_v4.db")
            return "\n".join(lines)
        lines.append(f"· 总体积：{db['total_mb']:.1f} MB")
        for name, size in db["files"]:
            lines.append(f"  - {name}：{size / 1048576.0:.1f} MB")
        if db.get("page"):
            page_count, page_size, free = db["page"]
            lines.append(
                f"· 页统计：{page_count} 页 × {page_size} 字节，空闲 {free} 页"
                f"（约 {free * page_size / 1048576.0:.1f} MB 可回收）"
            )
        if db.get("page_error"):
            lines.append(f"· 页统计读取失败：{db['page_error']}")
        lines.append("")
        lines.append("说明：")
        lines.append("· 数据库存的是会话与消息历史，聊得越多越大，属正常现象。")
        lines.append("· WAL 文件（-wal）较大说明有未合并的写入；AstrBot 正常退出时会自动合并。")
        lines.append("· 想回收空闲空间：`/perf 数据库 整理 yes`（手动确认，仅管理员，建议闲时执行）。")
        return "\n".join(lines)

    async def _db_vacuum(self) -> str:
        """执行 VACUUM 回收数据库空闲页（需要显式确认，且不在 AI 白名单里）。"""
        main_file = next((p for p in self._db_files() if p.suffix == ".db"), None)
        if main_file is None:
            return "❌ 找不到 data_v4.db，无法整理。"
        before = self._db_stats()

        def _run() -> tuple[bool, str]:
            import sqlite3

            try:
                con = sqlite3.connect(str(main_file), timeout=15.0)
                try:
                    con.execute("PRAGMA busy_timeout = 15000")
                    con.execute("VACUUM")
                    con.commit()
                    return True, ""
                finally:
                    con.close()
            except Exception as e:
                return False, f"{type(e).__name__}: {e}"

        # VACUUM 会重写整个库并短暂独占，必须丢线程，别卡事件循环
        ok, err = await asyncio.to_thread(_run)
        after = self._db_stats()
        if not ok:
            return (
                "❌ 数据库整理失败（没有改动数据，可放心）。\n"
                f"· 原因：{err}\n"
                "· 常见原因：数据库正被占用（AstrBot 正在写消息）。\n"
                "· 建议：在人少的时段重试，或直接跳过（不整理也不影响使用）。"
            )
        saved = before["total_mb"] - after["total_mb"]
        return (
            "✅ 数据库整理完成（VACUUM）\n"
            f"· 整理前：{before['total_mb']:.1f} MB\n"
            f"· 整理后：{after['total_mb']:.1f} MB\n"
            f"· 回收：{saved:.1f} MB"
        )

    async def _db_vacuum_text(self) -> str:
        return await self._db_vacuum()

    def _extra_tips(self, tips: list[str]) -> list[str]:
        """结合“三段耗时拆分”和“内存趋势”补两条更具体的建议。"""
        out = list(tips)
        try:
            if self._last_phases:
                pre, llm_c, post = self._last_phases
                side = pre + post
                if llm_c > 0 and side > max(1.0, llm_c) * 0.5:
                    out.insert(
                        0,
                        f"• 最近一条消息：模型只占 {llm_c:.1f}s，预处理+后处理占了 {side:.1f}s —— "
                        "慢在插件/框架这一侧，优先用上面的「卡顿嫌疑」定位那个插件。",
                    )
            if len(self._history) >= 12:
                third = max(1, len(self._history) // 3)
                mems = [float(s.get("mem") or 0) for s in self._history]
                first = sum(mems[:third]) / third
                last = sum(mems[-third:]) / third
                if first > 0 and (last - first) / first > 0.15:
                    out.insert(
                        0,
                        f"• 内存从 {first:.0f}MB 涨到 {last:.0f}MB（+{(last - first) / first * 100:.0f}%），"
                        "呈上涨趋势 —— 建议发「/perf 内存追踪」定位泄漏点。",
                    )
        except Exception:
            pass
        return out

    def _advice(
        self,
        lag: list[float],
        msg: list[float],
        llm: list[float],
        mem: tuple[float, float] | None,
        cpu: float | None,
        threshold_ms: int,
    ) -> list[str]:
        tips: list[str] = []
        thr = max(0.05, threshold_ms / 1000.0)
        if any(v >= thr for v in lag):
            top = sorted(self._slow_hits.items(), key=lambda kv: kv[1], reverse=True)
            name = top[0][0] if top else "某插件"
            tips.append(
                f"• 存在明显卡顿，头号嫌疑是「{name}」：它可能在协程里做了同步阻塞操作"
                "（网络请求、大文件读写、密集计算），会让所有消息排队。建议停用它或让作者改成异步，"
                "这是最容易被忽略、也最伤回复速度的原因。"
            )
        if msg and llm:
            p50_msg = self._percentile(msg, 50)
            if p50_msg > 0 and self._percentile(llm, 50) / p50_msg >= 0.7:
                tips.append(
                    "• 大部分时间花在 LLM 本身：优先换更快的模型/服务商；"
                    "在 AstrBot 配置里打开流式输出（provider_settings.streaming_response，首字明显更快）；"
                    "缩短对话历史与系统提示词（本插件可设 max_context_messages 自动裁剪）、关闭知识库检索等重上下文功能。"
                )
                tips.append(
                    "• 想看 LLM 首字延迟（TTFT）与平均耗时，可直接看 AstrBot 面板的统计页（官方已记录 provider_stats）。"
                )
        if self._state.get("trimmed_messages"):
            tips.append(
                f"• 已按 max_context_messages 自动裁剪上下文，累计裁掉 {int(self._state['trimmed_messages'])} 条历史；"
                "若仍偏慢，可把这个值调得更小。"
            )
        if self._dupe_tools:
            tips.insert(
                0,
                f"• ⚠️ 检测到 LLM 工具重名：{'、'.join(self._dupe_tools)}。"
                "这会让模型服务商直接拒绝整条请求（duplicate function name），"
                "表现为某个插件“坏了”，请停用注册了同名工具的插件。",
            )
        if mem and mem[0] >= self._int_cfg("memory_threshold_mb", 800) * 0.8:
            tips.append(
                "• 内存偏高：长期运行会因 GC 变慢而拖慢响应，建议 /perf 释放内存，或开启内存超阈值自动重启。"
            )
        if cpu is not None and cpu >= 80:
            tips.append("• CPU 占用很高：检查服务器上是否有其它程序在抢资源。")
        tips.append(
            "• 通用项：关掉用不到的插件（每个插件都会参与消息处理）、TTS/图片渲染等重功能改为按需、"
            "服务器不要开省电模式。"
        )
        if not tips[:-1]:
            tips.insert(0, "• 暂未发现明显瓶颈：多半是 LLM 服务商的网络往返时间，可换服务商对比。")
        return tips

    # ------------------------------------------------------------------
    # 内存释放
    # ------------------------------------------------------------------
    async def _release_memory(self, reason: str = "手动", quiet: bool = False) -> dict[str, Any]:
        before = _read_memory_mb()
        try:
            collected = await asyncio.to_thread(gc.collect)
        except Exception:
            collected = 0
        trimmed = False
        if self._cfg("trim_working_set", False):
            trimmed = await asyncio.to_thread(_trim_working_set)
        await asyncio.sleep(0.4)
        after = _read_memory_mb()

        freed = 0.0
        if before and after:
            freed = max(0.0, before[0] - after[0])

        self._last_release_ts = time.time()
        self._state["releases"] = int(self._state.get("releases") or 0) + 1
        self._state["freed_total_mb"] = round(float(self._state.get("freed_total_mb") or 0.0) + freed, 2)
        self._state["last_release_ts"] = self._last_release_ts
        self._state["last_release_reason"] = reason
        self._save_state()

        if not quiet:
            logger.info(
                f"[perf_guard] 内存释放完成（{reason}）：{before[0] if before else -1:.0f}MB → "
                f"{after[0] if after else -1:.0f}MB，回收 {freed:.1f}MB，gc 回收对象 {collected}，"
                f"工作集裁剪 {'成功' if trimmed else '跳过/失败'}"
            )
        return {
            "reason": reason,
            "before_mb": before[0] if before else None,
            "after_mb": after[0] if after else None,
            "freed_mb": freed,
            "gc_collected": collected,
            "trimmed": trimmed,
        }

    def _release_result_text(self, r: dict[str, Any]) -> str:
        if r.get("before_mb") is None:
            return "⚠️ 无法读取进程内存（psutil 未安装且系统接口不可用），已执行垃圾回收。"
        return (
            "🧹 内存释放完成\n"
            f"· 释放前：{r['before_mb']:.1f} MB\n"
            f"· 释放后：{r['after_mb']:.1f} MB\n"
            f"· 归还系统：约 {r['freed_mb']:.1f} MB\n"
            f"· 垃圾回收对象：{r['gc_collected']} 个\n"
            f"· 工作集裁剪：{'成功' if r['trimmed'] else '跳过（非 Windows 或不可用）'}"
        )

    # ------------------------------------------------------------------
    # 日志清理
    # ------------------------------------------------------------------
    def _log_dirs(self) -> list[Path]:
        """探测日志目录：优先使用配置覆盖，否则自动从 AstrBot 目录结构推断。"""
        override = str(self._cfg("log_dir_override", "")).strip()
        candidates: list[Path] = []
        if override:
            candidates.append(Path(override))
        else:
            try:
                plugin_data = Path(StarTools.get_data_dir(PLUGIN_NAME))  # <root>/data/plugin_data/<name>
                data_dir = plugin_data.parents[1]                        # <root>/data
                root = data_dir.parent                                   # <root>
                candidates += [
                    data_dir / "logs",
                    data_dir / "log",
                    root / "logs",
                    root / "log",
                ]
            except Exception:
                pass
            if self._data_dir:
                candidates.append(Path(self._data_dir).parents[1] / "logs")

        seen: set[str] = set()
        out: list[Path] = []
        for d in candidates:
            try:
                key = str(d.resolve()).lower()
            except Exception:
                key = str(d).lower()
            if key in seen or not d.is_dir():
                continue
            seen.add(key)
            out.append(d)
        return out

    @staticmethod
    def _is_log_file(p: Path) -> bool:
        name = p.name.lower()
        return ".log" in name and not name.endswith(".tmp")

    _MAX_LOG_FILES = 500
    """单次最多处理的日志文件数，避免巨大目录拖慢事件循环"""

    def _log_globs(self) -> list[str]:
        """允许删除的文件名通配符（默认只动 AstrBot 自己的日志）。

        默认故意收得很窄：只删 `astrbot*` 开头的日志，避免误删别的插件放在
        日志目录里的数据文件——这是插件之间互相干扰最常见的原因。
        """
        raw = str(self._cfg("log_name_pattern", "astrbot*.log*")).strip()
        if not raw:
            return ["*.log", "*.log.*"]
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        return parts or ["astrbot*.log*"]

    def _collect_log_files(self, dirs: list[Path]) -> list[Path]:
        """收集可清理的日志文件。

        只扫目录本身（不递归子目录），并按通配符与数量上限约束，
        既避免误删其它插件的数据，也避免大目录把事件循环拖住。
        """
        files: list[Path] = []
        globs = self._log_globs()
        for d in dirs:
            for pattern in globs:
                try:
                    for f in d.glob(pattern):
                        if len(files) >= self._MAX_LOG_FILES:
                            return files
                        try:
                            if f.is_file() and self._is_log_file(f):
                                files.append(f)
                        except Exception:
                            continue
                except Exception:
                    continue
        return files

    async def _clean_logs(self, reason: str = "手动") -> dict[str, Any]:
        """清理日志。

        统计与删除都是磁盘 I/O，全部丢到线程里执行，避免卡住事件循环
        （日志清理是本插件少数会大量碰磁盘的操作，绝不能在主线程做）。
        """
        return await asyncio.to_thread(self._clean_logs_body, reason)

    def _clean_logs_body(self, reason: str = "手动") -> dict[str, Any]:
        keep_days = max(0, self._int_cfg("log_keep_days", 7))
        max_total_mb = max(0, self._int_cfg("log_max_total_mb", 200))
        now = time.time()
        protect_after = now - 300  # 最近 5 分钟仍在写入的文件不删
        cutoff = now - keep_days * 86400

        dirs = self._log_dirs()
        files = self._collect_log_files(dirs)
        total_before = 0.0
        for f in files:
            try:
                total_before += f.stat().st_size
            except Exception:
                pass
        total_before /= 1048576.0

        deleted, freed, failed = 0, 0.0, 0

        def _try_delete(f: Path) -> bool:
            nonlocal deleted, freed, failed
            try:
                size = f.stat().st_size
            except Exception:
                size = 0
            try:
                f.unlink()
                deleted += 1
                freed += size / 1048576.0
                return True
            except Exception as e:
                failed += 1
                logger.debug(f"[perf_guard] 日志删除失败 {f}: {e}")
                return False

        # 1) 按保留天数删除
        remaining: list[Path] = []
        for f in sorted(files, key=lambda p: self._safe_mtime(p)):
            mtime = self._safe_mtime(f)
            if mtime > protect_after:
                remaining.append(f)
                continue
            if keep_days and mtime < cutoff:
                if not _try_delete(f):
                    remaining.append(f)
            else:
                remaining.append(f)

        # 2) 按目录总大小上限删除最旧的
        if max_total_mb > 0:
            total = 0.0
            sizes: dict[Path, float] = {}
            for f in remaining:
                try:
                    sizes[f] = f.stat().st_size / 1048576.0
                except Exception:
                    sizes[f] = 0.0
                total += sizes[f]
            for f in sorted(remaining, key=lambda p: self._safe_mtime(p)):
                if total <= max_total_mb:
                    break
                if self._safe_mtime(f) > protect_after:
                    continue
                if _try_delete(f):
                    total -= sizes.get(f, 0.0)

        # 3) 顺带清理 __pycache__
        pycache_dirs = 0
        if self._cfg("clean_pycache_enabled", False):
            pycache_dirs = self._clean_pycache()

        files_after = self._collect_log_files(dirs)
        total_after = 0.0
        for f in files_after:
            try:
                total_after += f.stat().st_size
            except Exception:
                pass
        total_after /= 1048576.0

        self._last_log_clean_ts = now
        self._state["log_cleans"] = int(self._state.get("log_cleans") or 0) + 1
        self._state["log_deleted_files"] = int(self._state.get("log_deleted_files") or 0) + deleted
        self._state["log_freed_mb"] = round(float(self._state.get("log_freed_mb") or 0.0) + freed, 2)
        self._state["last_log_clean_ts"] = now
        self._save_state()

        result = {
            "reason": reason,
            "dirs": [str(d) for d in dirs],
            "deleted": deleted,
            "failed": failed,
            "freed_mb": freed,
            "total_before_mb": total_before,
            "total_after_mb": total_after,
            "pycache_dirs": pycache_dirs,
        }
        logger.info(
            f"[perf_guard] 日志清理完成（{reason}）：删除 {deleted} 个文件，释放 {freed:.1f}MB，"
            f"{total_before:.1f}MB → {total_after:.1f}MB，失败 {failed} 个"
        )
        return result

    @staticmethod
    def _safe_mtime(p: Path) -> float:
        try:
            return p.stat().st_mtime
        except Exception:
            return time.time()

    def _clean_pycache(self) -> int:
        """清理 __pycache__ 目录，返回删除数量。

        只扫 `data/plugins`（插件字节码的实际所在），不整棵树递归——
        整棵树会扫到虚拟环境里的成千上万个目录，既慢又可能影响正在运行的依赖。
        """
        count = 0
        try:
            roots: list[Path] = []
            if self._data_dir:
                plugin_data = Path(self._data_dir)          # <root>/data/plugin_data/<name>
                roots.append(plugin_data.parents[1] / "plugins")  # <root>/data/plugins
            seen = 0
            for root in roots:
                if not root.is_dir():
                    continue
                for cache in root.rglob("__pycache__"):
                    seen += 1
                    if seen > 500:  # 上限，避免异常目录结构把线程拖死
                        break
                    if not cache.is_dir():
                        continue
                    try:
                        shutil.rmtree(cache)
                        count += 1
                    except Exception as e:
                        logger.debug(f"[perf_guard] 清理 {cache} 失败: {e}")
        except Exception as e:
            logger.debug(f"[perf_guard] 清理 __pycache__ 异常: {e}")
        return count

    def _file_logging_enabled(self) -> bool | None:
        """读取 AstrBot「日志写文件」开关（默认关闭）；读不到返回 None。"""
        try:
            cfg = self.context.get_config() or {}
            if "log_file" in cfg:
                return bool((cfg.get("log_file") or {}).get("enable", False))
            return bool(cfg.get("log_file_enable", False))
        except Exception:
            return None

    def _no_log_hint(self) -> str:
        flag = self._file_logging_enabled()
        if flag is False:
            return "AstrBot 未开启文件日志（log_file_enable=false），可在 WebUI 中开启后再清理"
        return "未找到日志目录"

    def _clean_result_text(self, r: dict[str, Any]) -> str:
        if not r.get("dirs"):
            return (
                f"⚠️ {self._no_log_hint()}。\n"
                "常见位置：AstrBot 根目录下的 data/logs（可用配置项 log_dir_override 手动指定）。\n"
                "提示：AstrBot 面板「存储清理」也自带日志/缓存清理，本插件相当于它的自动化版本。"
            )
        lines = [
            "🧾 日志清理完成",
            f"· 清理范围：{len(r['dirs'])} 个目录",
            f"· 删除文件：{r['deleted']} 个" + (f"（{r['failed']} 个失败，可能正被占用）" if r.get("failed") else ""),
            f"· 释放空间：{r['freed_mb']:.2f} MB",
            f"· 目录占用：{r['total_before_mb']:.1f} MB → {r['total_after_mb']:.1f} MB",
        ]
        if r.get("pycache_dirs"):
            lines.append(f"· 附带清理 __pycache__：{r['pycache_dirs']} 个")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 重启
    # ------------------------------------------------------------------
    def _detect_watchdog(self) -> bool:
        """判断是否由看门狗脚本启动（环境变量或标记文件）。"""
        if os.environ.get("MF_PERF_WATCHDOG") == "1":
            return True
        try:
            plugin_data = Path(self._data_dir)
            data_dir = plugin_data.parents[1]
            for marker in (
                data_dir / ".perf_guard_watchdog",
                data_dir.parent / ".perf_guard_watchdog",
            ):
                if marker.is_file():
                    return True
        except Exception:
            pass
        return False

    def _resolve_dotted(self, dotted: str) -> Any:
        parts = [p for p in str(dotted).split(".") if p]
        cur: Any = self
        if parts and parts[0] in ("self", ""):
            parts = parts[1:]
        elif parts and parts[0] == "context":
            cur = self.context
            parts = parts[1:]
        for p in parts:
            cur = getattr(cur, p, None)
            if cur is None:
                return None
        return cur

    def _resolve_restart_hook(self) -> Callable | None:
        """仅处理配置项 restart_hook 指定的自定义重启入口（留空则返回 None）。

        说明：AstrBot 的 Context 上并不存在 core_lifecycle 属性，
        所以不能靠“探测属性链”拿到官方重启方法，真实可用的三种方式见
        _restart_via_dashboard / _restart_via_process / 看门狗退出。
        """
        dotted = str(self._cfg("restart_hook", "")).strip()
        if not dotted:
            return None
        fn = self._resolve_dotted(dotted)
        if callable(fn):
            return fn
        logger.warning(f"[perf_guard] 配置的 restart_hook 不可调用：{dotted}")
        return None

    def _dashboard_credentials(self) -> tuple[str, int, str, str] | None:
        """读取 Dashboard 地址与凭据，用于调用官方优雅重启接口。"""
        try:
            cfg = self.context.get_config() or {}
            dbc = cfg.get("dashboard") or {}
            host = str(dbc.get("host") or "127.0.0.1").strip()
            if host in ("0.0.0.0", "::", "[::]", ""):
                host = "127.0.0.1"
            port = int(os.environ.get("DASHBOARD_PORT") or dbc.get("port") or 6185)
            user = str(dbc.get("username") or "").strip()
            secret = str(dbc.get("jwt_secret") or "").strip()
            if not user or not secret:
                return None
            return host, port, user, secret
        except Exception:
            return None

    def _process_restart_available(self) -> bool:
        try:
            import astrbot.core.process_restart  # noqa: F401

            return True
        except Exception:
            return False

    def _restart_capabilities(self) -> dict[str, bool]:
        return {
            "hook": self._resolve_restart_hook() is not None,
            "api": self._dashboard_credentials() is not None,
            "process": self._process_restart_available(),
            "watchdog": self._watchdog,
        }

    async def _restart_via_dashboard(self) -> tuple[bool, str]:
        """自签 JWT 调 Dashboard 的 /api/stat/restart-core，走完整优雅重启流程。"""
        creds = self._dashboard_credentials()
        if creds is None:
            return False, "未读取到 Dashboard 账号/密钥"
        host, port, user, secret = creds
        try:
            import jwt as pyjwt
        except Exception as e:
            return False, f"缺少 PyJWT 依赖（{e}）"
        try:
            token = pyjwt.encode(
                {
                    "username": user,
                    "exp": datetime.now(timezone.utc) + timedelta(days=7),
                },
                secret,
                algorithm="HS256",
            )
        except Exception as e:
            return False, f"签发 JWT 失败（{e}）"
        url = f"http://{host}:{port}/api/stat/restart-core"

        def _post() -> int:
            # 用标准库发请求，避免任何额外依赖
            req = urllib.request.Request(
                url,
                data=b"{}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=8) as resp:
                resp.read()
                return int(resp.status)

        try:
            status = await asyncio.to_thread(_post)
            if status < 400:
                return True, f"已通过 Dashboard 接口触发优雅重启（HTTP {status}）"
            return False, f"Dashboard 返回 HTTP {status}"
        except urllib.error.HTTPError as e:
            return False, f"Dashboard 返回 HTTP {e.code}"
        except Exception as e:
            return False, f"调用 Dashboard 失败（{type(e).__name__}: {e}）"

    async def _restart_via_process(self) -> tuple[bool, str]:
        """调用 AstrBot 自带的进程重启原语：另起新进程，然后退出当前进程。"""
        try:
            from astrbot.core.process_restart import restart_process
        except Exception as e:
            return False, f"无法导入 restart_process（{e}）"
        self._spawn(self._call_restart_process(restart_process), name="perf_guard:restart")
        return True, "已调用 AstrBot 进程重启原语（约 3 秒后拉起新进程）"

    async def _call_restart_process(self, fn: Callable) -> None:
        await asyncio.sleep(self._EXIT_DELAY)
        try:
            await asyncio.to_thread(fn, 0)
        except Exception as e:
            logger.error(f"[perf_guard] 进程重启原语执行失败: {e}")

    def _can_restart(self) -> tuple[bool, str]:
        cooldown_min = max(0, self._int_cfg("restart_cooldown_minutes", 30))
        if cooldown_min <= 0 or not self._last_restart_ts:
            return True, ""
        elapsed = time.time() - self._last_restart_ts
        if elapsed < cooldown_min * 60:
            left = int((cooldown_min * 60 - elapsed) / 60) + 1
            return False, f"距上次重启不到 {cooldown_min} 分钟（约 {left} 分钟后可再重启）"
        return True, ""

    async def _restart(self, reason: str, *, manual: bool = False) -> str:
        """按可用性依次尝试：自定义入口 → Dashboard 优雅重启 → 进程重启 → 看门狗。"""
        hook = self._resolve_restart_hook()
        caps = self._restart_capabilities()
        method = str(self._cfg("restart_method", "auto")).strip().lower() or "auto"
        allow_without_wd = bool(self._cfg("allow_restart_without_watchdog", False))

        if not any((hook, caps["api"], caps["process"], caps["watchdog"])) and not allow_without_wd:
            return (
                "❌ 当前没有可用的重启方式：\n"
                "· Dashboard 接口不可用（需要 AstrBot 配置里的 dashboard 账号与密钥）\n"
                "· 进程重启原语不可用（AstrBot 版本过旧）\n"
                "· 未检测到看门狗\n"
                "建议用 watchdog\\run_with_watchdog.bat 启动 AstrBot（见 README），"
                "或把 allow_restart_without_watchdog 设为 true（重启后需要手动启动机器人）。"
            )

        if not manual:
            ok, why = self._can_restart()
            if not ok:
                return f"⏸ 暂不重启：{why}"

        # 重启过程中 AstrBot 不会调用插件 terminate()，所以统计必须先落盘
        self._state["restarts"] = int(self._state.get("restarts") or 0) + 1
        self._state["last_restart_ts"] = time.time()
        self._state["last_restart_reason"] = reason
        self._last_restart_ts = time.time()
        self._over_count = 0
        self._save_state(force=True)  # 重启后本进程就没了，必须立刻落盘
        logger.warning(f"[perf_guard] 触发 AstrBot 重启：{reason}（方式={method}）")

        await self._notify(f"♻️ AstrBot 即将重启\n原因：{reason}")

        # 1) 自定义入口
        if hook is not None and method in ("auto", "hook"):
            try:
                res = hook()
                if inspect.isawaitable(res):
                    await res
                return f"♻️ 已通过自定义入口触发重启\n原因：{reason}"
            except Exception as e:
                logger.error(f"[perf_guard] 自定义重启入口失败: {e}，尝试下一种方式")

        # 2) Dashboard 优雅重启（推荐）
        if caps["api"] and method in ("auto", "api"):
            ok, detail = await self._restart_via_dashboard()
            if ok:
                return f"♻️ {detail}\n原因：{reason}"
            logger.warning(f"[perf_guard] Dashboard 重启失败：{detail}，尝试下一种方式")

        # 3) AstrBot 进程重启原语
        if caps["process"] and method in ("auto", "process"):
            ok, detail = await self._restart_via_process()
            if ok:
                return f"♻️ {detail}\n原因：{reason}"
            logger.warning(f"[perf_guard] 进程重启失败：{detail}，尝试下一种方式")

        # 4) 看门狗兜底：主动退出，由 bat 拉起
        if caps["watchdog"] or allow_without_wd:
            self._spawn(self._exit_soon(reason), name="perf_guard:exit")
            who = "看门狗" if caps["watchdog"] else "你自己手动启动"
            return (
                f"♻️ 正在重启 AstrBot（原因：{reason}）\n"
                f"约 {int(self._EXIT_DELAY)} 秒后进程退出，之后由{who}拉起。"
            )

        return "❌ 所有重启方式都失败了，请手动重启 AstrBot。"

    async def _exit_soon(self, reason: str) -> None:
        await asyncio.sleep(self._EXIT_DELAY)
        logger.warning(f"[perf_guard] 主动退出进程以完成重启：{reason}")
        try:
            import logging

            logging.shutdown()
        except Exception:
            pass
        os._exit(0)

    # ------------------------------------------------------------------
    # 通知 / 状态
    # ------------------------------------------------------------------
    async def _notify_pending_issues(self) -> None:
        """把钩子里攒下的插件报错 / 模型失败，在超过阈值时通知管理员。

        设计取舍：钩子（on_plugin_error / on_llm_response）里只做内存记账，
        发消息一律推迟到监控循环。原因是报错那一刻机器人本身可能已经很糟，
        在钩子里 await 一次网络发送会拖慢（甚至卡住）出问题的那条消息。
        """
        now = time.time()

        # --- 插件报错 ---
        try:
            need = max(1, self._int_cfg("plugin_error_notify_times", 3))
            window = max(1, self._int_cfg("plugin_error_window_minutes", 10)) * 60.0
            cooldown = max(1, self._int_cfg("plugin_error_notify_cooldown_minutes", 30)) * 60.0
            for name in list(self._err_notify_queue.keys()):
                agg = self._err_notify_queue.get(name) or {}
                # 超过窗口没再犯的聚合项直接丢掉，避免旧账被翻出来通知
                if now - float(agg.get("last_ts") or 0) > window:
                    self._err_notify_queue.pop(name, None)
            if self._err_notify_queue:
                hot = [
                    (name, agg)
                    for name, agg in self._err_notify_queue.items()
                    if int(agg.get("count") or 0) >= need
                ]
                if hot and now - self._last_err_notify_ts >= cooldown:
                    self._last_err_notify_ts = now
                    self._err_notify_queue.clear()
                    body = "\n".join(
                        f"· {name}：{int(agg.get('count') or 0)} 次，例如 {agg.get('error', '')}"
                        for name, agg in hot[:5]
                    )
                    await self._notify(
                        "🧯 检测到插件连续报错\n"
                        f"{body}\n\n"
                        "这类报错通常意味着那个插件的某个功能已经坏了（有时只表现为"
                        "“机器人偶尔不回话”）。\n"
                        "查详情：/perf 报错　　（本条通知有冷却，不会刷屏）"
                    )
        except Exception as e:
            logger.debug(f"[perf_guard] 报错通知处理失败: {e}")

        # --- 模型（LLM）失败 ---
        try:
            need = max(1, self._int_cfg("model_error_notify_times", 3))
            cooldown = max(1, self._int_cfg("model_error_notify_cooldown_minutes", 30)) * 60.0
            new_errs = self._model_errs - self._model_ok_mark
            if new_errs >= need and now - self._last_model_notify_ts >= cooldown:
                self._last_model_notify_ts = now
                self._model_ok_mark = self._model_errs
                last = self._model_err_events[-1].get("text", "") if self._model_err_events else ""
                rate = self._model_errs / self._model_calls * 100.0 if self._model_calls else 0.0
                await self._notify(
                    "🛑 模型调用连续失败\n"
                    f"· 本次启动后：{self._model_calls} 次调用中失败 {self._model_errs} 次（{rate:.0f}%）\n"
                    f"· 最近一次原因：{last or '（未捕获到原因）'}\n\n"
                    "这通常不是 AstrBot 的问题，而是 API 中转/密钥/额度/网络的问题——"
                    "表现就是“机器人不回话了”。\n"
                    "查详情：/perf 状态"
                )
            elif self._model_errs == 0:
                self._model_ok_mark = 0
        except Exception as e:
            logger.debug(f"[perf_guard] 模型失败通知处理失败: {e}")

    async def _check_disk_space(self) -> None:
        """磁盘剩余低于警戒线时告警，并按配置顺带清一次日志。"""
        try:
            warn_gb = float(self._int_cfg("disk_free_warn_gb", 3) or 0)
            if warn_gb <= 0:
                return
            free = self._disk_free_gb()
            if free is None or free >= warn_gb:
                return
            cooldown = max(1, self._int_cfg("disk_warn_cooldown_minutes", 360)) * 60.0
            if time.time() - self._last_disk_warn_ts < cooldown:
                return
            self._last_disk_warn_ts = time.time()
            logger.warning(
                f"[perf_guard] ⚠️ 磁盘剩余 {free:.1f} GB 已低于警戒线 {warn_gb:.0f} GB"
            )
            cleaned = ""
            if self._cfg("log_cleanup_enabled", True) and self._cfg(
                "disk_warn_auto_clean", True
            ):
                result = await self._clean_logs(reason="磁盘空间不足自动清理")
                cleaned = (
                    f"\n· 已自动清理日志：删除 {int(result.get('deleted') or 0)} 个文件，"
                    f"释放 {float(result.get('freed_mb') or 0):.1f} MB"
                )
                free = self._disk_free_gb() or free
            await self._notify(
                f"💾 磁盘空间不足\n· 当前剩余：{free:.1f} GB（警戒线 {warn_gb:.0f} GB）{cleaned}\n\n"
                "磁盘写满会让 AstrBot 无法写日志、无法保存会话，甚至数据损坏，建议尽快处理。\n"
                "· 手动清理日志：/perf 清理日志\n"
                "· 看看数据库多大：/perf 数据库\n"
                "· 整机排查：/perf 体检"
            )
        except Exception as e:
            logger.debug(f"[perf_guard] 磁盘检查失败: {e}")

    async def _send_to(self, umo: str, text: str) -> bool:
        """向指定会话主动发消息（AI 提议、通知都用它）。"""
        if not umo:
            return False
        try:
            await self.context.send_message(umo, MessageChain().message(text))
            return True
        except Exception as e:
            logger.warning(f"[perf_guard] 消息发送失败({umo}): {e}")
            return False

    async def _notify(self, text: str) -> None:
        umo = str(self._cfg("notify_umo", "")).strip()
        if not umo:
            return
        await self._send_to(umo, text)

    def _status_text(self) -> str:
        mem = _read_memory_mb()
        uptime = _process_uptime_seconds()
        uptime_text = _fmt_duration(uptime) if uptime else _fmt_duration(time.time() - self._load_ts) + "（本插件加载后）"

        dirs = self._log_dirs()
        files = self._collect_log_files(dirs)
        log_mb = 0.0
        for f in files:
            try:
                log_mb += f.stat().st_size
            except Exception:
                pass
        log_mb /= 1048576.0

        disk_text = "未知"
        try:
            probe = dirs[0] if dirs else Path(self._data_dir or ".")
            usage = shutil.disk_usage(str(probe))
            disk_text = f"剩余 {usage.free / 1073741824:.1f} GB / 共 {usage.total / 1073741824:.1f} GB"
        except Exception:
            pass

        if mem:
            mem_line = f"· 进程内存：{mem[0]:.0f} MB（峰值 {mem[1]:.0f} MB，阈值 {self._int_cfg('memory_threshold_mb', 800)} MB）"
        else:
            mem_line = "· 进程内存：无法读取（建议安装 psutil）"

        caps = self._restart_capabilities()
        avail = [
            label
            for label, ok in (
                ("自定义入口", caps["hook"]),
                ("Dashboard 接口", caps["api"]),
                ("进程重启", caps["process"]),
                ("看门狗", caps["watchdog"]),
            )
            if ok
        ]

        lines = [
            "📊 AstrBot 性能守护",
            mem_line,
            f"· 运行时长：{uptime_text}",
            f"· 日志占用：{len(files)} 个文件 / {log_mb:.1f} MB"
            + (f"（目录：{dirs[0]}）" if dirs else "（" + self._no_log_hint() + "）"),
            f"· 磁盘空间：{disk_text}",
            f"· 上次释放：{_fmt_ts(self._last_release_ts)}"
            + (f"，累计归还 {float(self._state.get('freed_total_mb') or 0):.1f} MB" if self._state.get("freed_total_mb") else ""),
            f"· 上次清理：{_fmt_ts(self._last_log_clean_ts)}"
            + (f"，累计删除 {int(self._state.get('log_deleted_files') or 0)} 个文件" if self._state.get("log_deleted_files") else ""),
            f"· 累计重启：{int(self._state.get('restarts') or 0)} 次（上次：{_fmt_ts(self._state.get('last_restart_ts'))}）",
            "· 自动化："
            + "自动重启" + ("开" if self._cfg("auto_restart_enabled", False) else "关")
            + " / 定时重启" + (
                str(self._cfg("scheduled_restart_time", "")) if self._cfg("scheduled_restart_enabled", False) else "关"
            )
            + " / 日志清理" + ("开" if self._cfg("log_cleanup_enabled", True) else "关")
            + " / 自动释放" + (f"{self._int_cfg('release_interval_seconds', 1800)}s" if self._int_cfg("release_interval_seconds", 1800) > 0 else "关"),
            f"· 重启方式：{'、'.join(avail) if avail else '无可用方式（建议用看门狗启动）'}",
        ]
        if self._dupe_tools:
            lines.append(
                f"· ⚠️ LLM 工具重名：{'、'.join(self._dupe_tools)}"
                "（会让模型服务商报 duplicate function name、整条请求失败，请停用注册同名工具的插件）"
            )
        elif self._tool_count:
            lines.append(f"· LLM 工具数：{self._tool_count} 个（无重名）")
        err_total = sum(int(i.get("count") or 1) for i in self._plugin_errors)
        if err_total:
            top = sorted(self._plugin_error_hits.items(), key=lambda kv: kv[1], reverse=True)[:3]
            lines.append(
                f"· ⚠️ 插件报错：累计 {err_total} 次（最多："
                + "、".join(f"{n} ×{c}" for n, c in top)
                + "）→ 详情：/perf 报错"
            )
        else:
            lines.append("· 插件报错：未捕获到")
        lines.append(self._model_health_line())
        lines.append(self._ai_status_line())
        admin_line = self._admin_status_line()
        if admin_line:
            lines.append(admin_line)
        return "\n".join(lines)

    def _admin_status_line(self) -> str:
        """把“谁能用这个插件”讲清楚，避免私聊里被自己的插件锁在门外。

        QQ 私聊没有群角色，event.is_admin() 恒为 False，只认 AstrBot 主配置的
        admins_id 名单；而默认值是 ["astrbot"]（占位符），所以这里直接把名单列出来，
        用户一眼就能看出自己有没有被算进去。
        """
        try:
            admins = list(self.context.get_config().get("admins_id", []) or [])
        except Exception:
            admins = []
        text = "、".join(str(a) for a in admins) if admins else "（空）"
        scope = []
        if self._cfg("command_admin_only", True):
            scope.append("指令")
        if self._cfg("llm_admin_only", True):
            scope.append("LLM 工具")
        if not scope:
            return f"· 权限：未限制（admins_id：{text}）"
        return (
            f"· 权限：{'/'.join(scope)}仅管理员可用 | admins_id：{text}"
            + ("　⚠️ 私聊里请确保你的 QQ 号在上面这个名单里" if not any(str(a).isdigit() for a in admins) else "")
        )

    # ------------------------------------------------------------------
    # 诊断增强：慢消息追踪 / 历史趋势 / 异常快照 / 按需内存追踪
    # ------------------------------------------------------------------
    def _slowest_text(self, n: int = 5) -> str:
        """列出最近最慢的几条消息，并说明慢在哪一段。"""
        if not self._slow_messages:
            limit = self._int_cfg("slow_message_seconds", 5)
            return (
                f"📭 暂时没有超过 {limit} 秒的慢消息记录。\n"
                f"· 已统计 {self._msg_total} 条消息；超过阈值才会记录\n"
                f"· 阈值可用配置项 slow_message_seconds 调整（当前 {limit} 秒）"
            )
        rows = list(self._slow_messages)[-max(1, n):][::-1]
        lines = [
            f"🐌 慢消息排行（累计记录 {len(self._slow_messages)} 条，显示最新 {len(rows)} 条）",
            "",
        ]
        for r in rows:
            lines.append(
                f"· {_fmt_ts(r.get('ts'))}｜总 {float(r.get('total') or 0):.1f}s"
                f"（{self._fmt_phases(r.get('pre') or 0, r.get('llm') or 0, r.get('post') or 0)}）"
            )
            lines.append(
                f"  会话：{r.get('umo') or '?'}｜发送者：{r.get('sender') or '?'}"
            )
            if r.get("lag_hit"):
                extra = f"，嫌疑：{r['lag_plugin']}" if r.get("lag_plugin") else ""
                lines.append(
                    f"  ⚠️ 期间事件循环卡顿 {int(r['lag_hit'])} 次，最严重 "
                    f"{float(r.get('lag_ms') or 0):.0f}ms{extra}"
                )
        lines.append("")
        lines.append(
            "👉 判读：模型段最长 → 换更快模型 / 缩短上下文；"
            "预处理或后处理段最长 → 多半是别的插件在同步阻塞，可对照 /perf 诊断 的卡顿定位"
        )
        return "\n".join(lines)

    @staticmethod
    def _fmt_phases(pre: float, llm: float, post: float) -> str:
        return f"预处理 {pre:.1f}s / 模型 {llm:.1f}s / 后处理 {post:.1f}s"

    # --- 历史趋势 ---
    def _history_sample(self) -> dict[str, Any]:
        mem = _read_memory_mb()
        msg = list(self._msg_latency)
        lag_total = int(self._state.get("lag_events") or 0)
        sample = {
            "t": round(time.time(), 1),
            "mem": round(mem[0], 1) if mem else 0.0,
            "cpu": round(self._cpu_percent() or 0.0, 1),
            "p50": round(self._percentile(msg, 50), 3) if msg else 0.0,
            "p95": round(self._percentile(msg, 95), 3) if msg else 0.0,
            "lag": max(0, lag_total - self._history_lag_mark),
            "msgs": max(0, self._msg_total - self._history_msg_mark),
        }
        self._history_lag_mark = lag_total
        self._history_msg_mark = self._msg_total
        return sample

    async def _history_maybe_sample(self) -> None:
        if not self._cfg("history_enabled", True):
            return
        now = time.time()
        interval = max(1, self._int_cfg("history_interval_minutes", 10)) * 60
        if now - self._last_history_ts < interval:
            return
        self._last_history_ts = now
        self._history.append(self._history_sample())
        keep_days = max(1, self._int_cfg("history_keep_days", 7))
        cutoff = now - keep_days * 86400
        self._history = [s for s in self._history if float(s.get("t") or 0) >= cutoff][-2016:]
        # 落盘放线程里；且至少间隔 10 分钟才写一次，避免把磁盘写爆
        if now - self._last_history_write >= max(600, interval):
            await asyncio.to_thread(self._history_save)

    def _history_save(self) -> None:
        if not self._history_path:
            return
        try:
            tmp = f"{self._history_path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._history[-2016:], f, ensure_ascii=False)
            os.replace(tmp, self._history_path)
            self._last_history_write = time.time()
        except Exception as e:
            logger.debug(f"[perf_guard] 趋势数据保存失败: {e}")

    def _history_load(self) -> None:
        if not self._history_path or not os.path.exists(self._history_path):
            return
        try:
            with open(self._history_path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self._history = [d for d in data if isinstance(d, dict)][-2016:]
            if self._history:
                self._last_history_ts = float(self._history[-1].get("t") or 0.0)
            self._history_lag_mark = int(self._state.get("lag_events") or 0)
            self._history_msg_mark = self._msg_total
        except Exception as e:
            logger.debug(f"[perf_guard] 趋势数据读取失败: {e}")

    @staticmethod
    def _sparkline(values: list[float]) -> str:
        vals = [float(v or 0) for v in values]
        if len(vals) < 2:
            return ""
        lo, hi = min(vals), max(vals)
        blocks = "▁▂▃▄▅▆▇█"
        if hi - lo < 1e-9:
            return blocks[0] * len(vals)
        return "".join(blocks[min(7, int((v - lo) / (hi - lo) * 7.99))] for v in vals)

    def _report_text(self, days: int = 1) -> str:
        days = max(1, min(30, days))
        cutoff = time.time() - days * 86400
        rows = [s for s in self._history if float(s.get("t") or 0) >= cutoff]
        if not rows:
            iv = self._int_cfg("history_interval_minutes", 10)
            return (
                f"📭 还没有趋势数据（每 {iv} 分钟采样一次，运行一段时间后再看）。\n"
                f"· 已加载的历史采样点：{len(self._history)} 个\n"
                f"· 相关配置：history_enabled / history_interval_minutes / history_keep_days"
            )
        mems = [float(s.get("mem") or 0) for s in rows]
        cpus = [float(s.get("cpu") or 0) for s in rows]
        p50s = [float(s.get("p50") or 0) for s in rows if float(s.get("p50") or 0) > 0]
        p95s = [float(s.get("p95") or 0) for s in rows if float(s.get("p95") or 0) > 0]
        lag_sum = sum(int(s.get("lag") or 0) for s in rows)
        msg_sum = sum(int(s.get("msgs") or 0) for s in rows)

        lines = [
            f"📈 性能趋势报告（最近 {days} 天）",
            f"· 采样点：{len(rows)} 个（每 {self._int_cfg('history_interval_minutes', 10)} 分钟一次）",
            f"· 内存：{mems[0]:.0f} MB → {mems[-1]:.0f} MB（峰值 {max(mems):.0f} MB）",
            f"  {self._sparkline(mems)}",
            f"· CPU：平均 {sum(cpus) / max(1, len(cpus)):.0f}%（峰值 {max(cpus):.0f}%）",
        ]
        if p50s:
            lines.append(
                f"· 消息响应 p50：{p50s[0]:.2f}s → {p50s[-1]:.2f}s（中位 {self._percentile(p50s, 50):.2f}s）"
            )
        if p95s:
            lines.append(
                f"· 消息响应 p95：最高 {max(p95s):.2f}s（中位 {self._percentile(p95s, 50):.2f}s）"
            )
        lines.append(f"· 事件循环卡顿：{lag_sum} 次")
        lines.append(f"· 期间处理消息：{msg_sum} 条")

        if len(mems) >= 6:
            third = max(1, len(mems) // 3)
            first = sum(mems[:third]) / third
            last = sum(mems[-third:]) / third
            if first > 0:
                delta = (last - first) / first * 100
                if delta > 15:
                    trend = f"⚠️ 内存相对上涨 {delta:.0f}%，建议查内存泄漏（发「/perf 内存追踪」）"
                elif delta < -10:
                    trend = f"✅ 内存下降 {abs(delta):.0f}%（自动释放起作用了）"
                else:
                    trend = f"基本平稳（变化 {delta:+.0f}%）"
                lines.append(f"· 趋势判断：{trend}")

        if days > 1:
            lines.append("")
            lines.append("📅 按天汇总")
            by_day: dict[str, list[dict[str, Any]]] = {}
            for s in rows:
                day = datetime.fromtimestamp(float(s.get("t") or 0)).strftime("%m-%d")
                by_day.setdefault(day, []).append(s)
            for day in sorted(by_day):
                ds = by_day[day]
                m = [float(x.get("mem") or 0) for x in ds]
                pl = [float(x.get("p50") or 0) for x in ds if float(x.get("p50") or 0) > 0]
                lines.append(
                    f"· {day}：内存 {m[0]:.0f}→{m[-1]:.0f}（峰 {max(m):.0f}）MB｜"
                    f"p50 {(self._percentile(pl, 50) if pl else 0):.2f}s｜"
                    f"卡顿 {sum(int(x.get('lag') or 0) for x in ds)} 次｜"
                    f"消息 {sum(int(x.get('msgs') or 0) for x in ds)} 条"
                )
        return "\n".join(lines)

    # --- 异常快照 ---
    def _snapshot_dir(self) -> Path:
        return Path(self._data_dir or ".") / "snapshots"

    def _snapshot_body(self, kind: str, detail: str = "") -> str:
        mem = _read_memory_mb()
        uptime = _process_uptime_seconds()
        lag = list(self._lag_records)
        msg = list(self._msg_latency)
        threshold = self._int_cfg("lag_threshold_ms", 1000)
        lines = [
            "=" * 64,
            f"AstrBot 性能守护 · 诊断快照（{kind}）",
            f"时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ]
        if detail:
            lines.append(f"触发原因：{detail}")
        lines.append("=" * 64)
        lines.append(f"内存：{f'{mem[0]:.0f} MB（峰值 {mem[1]:.0f} MB）' if mem else '未知'}")
        lines.append(f"进程运行时长：{_fmt_duration(uptime) if uptime else '未知'}")
        lines.append(
            f"累计消息：{self._msg_total} 条｜累计卡顿：{int(self._state.get('lag_events') or 0)} 次"
            f"｜累计重启：{int(self._state.get('restarts') or 0)} 次"
        )
        if msg:
            lines.append(
                f"消息响应：p50 {self._percentile(msg, 50):.2f}s / "
                f"p95 {self._percentile(msg, 95):.2f}s / 最慢 {max(msg):.2f}s"
            )
        if lag:
            over = sum(1 for v in lag if v * 1000.0 >= threshold)
            lines.append(
                f"事件循环：p50 {self._percentile(lag, 50) * 1000:.0f}ms / "
                f"p95 {self._percentile(lag, 95) * 1000:.0f}ms（超阈值 {over} 次）"
            )
        if self._slow_hits:
            top = sorted(self._slow_hits.items(), key=lambda kv: kv[1], reverse=True)[:5]
            lines.append("卡顿嫌疑插件：" + "，".join(f"{n}×{c}" for n, c in top))
        lines.append(f"本插件自身开销：{self._self_overhead_text()}")
        if self._slow_messages:
            lines.append("")
            lines.append("最近的慢消息：")
            for r in list(self._slow_messages)[-5:]:
                lines.append(
                    f"  · {_fmt_ts(r.get('ts'))} 总 {float(r.get('total') or 0):.1f}s"
                    f"（{self._fmt_phases(r.get('pre') or 0, r.get('llm') or 0, r.get('post') or 0)}）"
                )
        if self._lag_events:
            ev = self._lag_events[-1]
            lines.append("")
            lines.append(
                f"最近一次卡顿现场（{float(ev.get('lag') or 0) * 1000:.0f}ms，"
                f"嫌疑：{ev.get('plugin')}）："
            )
            for row in str(ev.get("stack") or "").strip().splitlines()[-10:]:
                lines.append("  " + row.strip())
        if self._dupe_tools:
            lines.append("")
            lines.append("⚠️ LLM 工具重名：" + "、".join(self._dupe_tools))
        if self._ai_last_result:
            lines.append(f"最近一次 AI 动作：{self._ai_last_result}")
        return "\n".join(lines) + "\n"

    def _snapshot_write_sync(self, path: Path, text: str) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
            keep = max(1, self._int_cfg("snapshot_keep", 5))
            files = sorted(
                path.parent.glob("*.txt"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            for old in files[keep:]:
                try:
                    old.unlink()
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"[perf_guard] 写入快照失败: {e}")

    async def _write_snapshot(self, kind: str, detail: str = "") -> str | None:
        """异常时保存一份诊断快照（去抖 60 秒，磁盘 IO 在线程里做）。"""
        if not self._cfg("snapshot_on_alarm", True):
            return None
        now = time.time()
        if now - self._last_snapshot_ts < 60:
            return None
        self._last_snapshot_ts = now
        try:
            text = self._snapshot_body(kind, detail)
            path = self._snapshot_dir() / f"{kind}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            await asyncio.to_thread(self._snapshot_write_sync, path, text)
            logger.info(f"[perf_guard] 已保存诊断快照：{path}")
            return str(path)
        except Exception as e:
            logger.debug(f"[perf_guard] 保存快照异常: {e}")
            return None

    def _snapshot_text(self, which: int = 1) -> str:
        d = self._snapshot_dir()
        try:
            files = (
                sorted(d.glob("*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)
                if d.exists()
                else []
            )
        except Exception:
            files = []
        if not files:
            return (
                "📭 还没有诊断快照。\n"
                f"· 目录：{d}\n"
                "· 检测到卡顿或内存告警时会自动保存（配置项 snapshot_on_alarm）"
            )
        lines = [
            f"🗂 诊断快照（保留最近 {self._int_cfg('snapshot_keep', 5)} 个）",
            f"· 目录：{d}",
            "",
        ]
        for i, f in enumerate(files, 1):
            try:
                size = f.stat().st_size / 1024
                mtime = _fmt_ts(f.stat().st_mtime)
            except Exception:
                size, mtime = 0.0, "?"
            lines.append(f"{'▶ ' if i == which else '  '}{i}. {f.name}（{size:.1f} KB，{mtime}）")
        idx = min(max(1, which), len(files)) - 1
        lines.append("")
        lines.append(f"—— 第 {idx + 1} 个快照内容 ——")
        try:
            body = files[idx].read_text(encoding="utf-8", errors="replace")
            if len(body) > 1800:
                body = body[:1800] + "\n…（已截断，完整内容见上面的文件路径）"
            lines.append(body)
        except Exception as e:
            lines.append(f"（读取失败：{e}）")
        return "\n".join(lines)

    # --- 按需内存追踪（tracemalloc）---
    async def _trace_text(self, stop: bool = False) -> str:
        """内存追踪：默认开始，带 stop 时结束并出报告。"""
        try:
            import tracemalloc  # noqa: F401
        except Exception:
            return "⚠️ 当前 Python 环境不支持 tracemalloc。"
        if stop and self._tracer_on:
            return await self._trace_stop()
        if not self._tracer_on:
            if self._trace_report:
                return (
                    self._trace_report
                    + "\n\n（再次发送「/perf 内存追踪」可以重新开始一次追踪）"
                )
            return await self._trace_start()
        import tracemalloc as _tm

        try:
            cur, peak = _tm.get_traced_memory()
        except Exception:
            cur, peak = 0, 0
        maxmin = max(1, self._int_cfg("memory_trace_max_minutes", 15))
        elapsed = time.time() - self._tracer_start_ts
        left = max(0, int(maxmin * 60 - elapsed))
        return (
            "🔬 内存追踪进行中\n"
            f"· 已运行：{_fmt_duration(elapsed)}\n"
            f"· 追踪到的内存：{cur / 1048576:.1f} MB（峰值 {peak / 1048576:.1f} MB）\n"
            f"· 约 {left // 60} 分 {left % 60} 秒后自动关闭并生成报告\n"
            "👉 发「/perf 内存追踪 结束」可立即出报告\n"
            "⚠️ 追踪期间会略微增加内存与 CPU 开销，排查完请及时结束。"
        )

    async def _trace_start(self) -> str:
        try:
            import tracemalloc

            tracemalloc.start(10)
            self._tracer_base = await asyncio.to_thread(tracemalloc.take_snapshot)
            self._tracer_on = True
            self._tracer_start_ts = time.time()
            self._trace_report = ""
        except Exception as e:
            return f"⚠️ 开启内存追踪失败：{type(e).__name__}: {e}"
        maxmin = max(1, self._int_cfg("memory_trace_max_minutes", 15))
        return (
            "🔬 内存追踪已开启\n"
            f"· {maxmin} 分钟后自动关闭并生成报告（不会一直开着）\n"
            "· 现在去正常使用机器人（怀疑的对象多聊几轮）\n"
            "· 结束后发「/perf 内存追踪 结束」查看报告\n"
            "⚠️ 追踪会略微增加内存与 CPU 开销，用完记得结束。"
        )

    async def _trace_stop(self) -> str:
        try:
            import tracemalloc
        except Exception:
            self._tracer_on = False
            return "⚠️ 无法导入 tracemalloc。"
        base = self._tracer_base
        self._tracer_on = False
        elapsed = time.time() - self._tracer_start_ts
        try:
            snap = await asyncio.to_thread(tracemalloc.take_snapshot)
            stats = (
                snap.compare_to(base, "lineno")
                if base is not None
                else snap.statistics("lineno")
            )
            top = [s for s in stats if s.size_diff > 0][:10]
            mem = _read_memory_mb()
            lines = [
                "🔬 内存追踪报告",
                f"· 追踪时长：{_fmt_duration(elapsed)}",
                f"· 当前进程内存：{f'{mem[0]:.0f} MB' if mem else '未知'}",
                "· 期间新增最多的分配位置：",
            ]
            if not top:
                lines.append("  （没有明显增长，说明这段时间没有持续泄漏）")
            for i, s in enumerate(top, 1):
                frame = s.traceback[0] if s.traceback else None
                where = f"{frame.filename}:{frame.lineno}" if frame else "未知位置"
                lines.append(
                    f"  {i}. +{s.size_diff / 1024:.0f} KB（+{s.count_diff} 个对象）  {where}"
                )
            lines.append("")
            lines.append(
                "👉 判读：增长点集中在某个插件的文件里 → 基本就是它泄漏；"
                "集中在 AstrBot 自身或 asyncio 内部 → 多半是正常缓存/会话累积。"
            )
            self._trace_report = "\n".join(lines)
            logger.info(f"[perf_guard] 内存追踪结束（{_fmt_duration(elapsed)}），报告已生成")
        except Exception as e:
            self._trace_report = f"⚠️ 生成内存追踪报告失败：{type(e).__name__}: {e}"
        finally:
            try:
                tracemalloc.stop()
            except Exception:
                pass
            self._tracer_base = None
        return self._trace_report

    # ------------------------------------------------------------------
    # AI 自动化管理：AI 只负责“提议”，管理员审核后才执行
    # ------------------------------------------------------------------
    async def _resolve_ai_provider(self, umo: str | None = None) -> Any:
        """找出用来做评估的对话模型。

        优先级：配置指定的 ai_provider_id → 当前会话使用的模型 → 全局默认模型。
        全部拿到不到时返回 None（由调用方给出友好提示，绝不抛异常）。
        """
        pm = getattr(self.context, "provider_manager", None)
        if pm is None:
            return None

        pid = str(self._cfg("ai_provider_id", "") or "").strip()
        if pid:
            try:
                inst_map = getattr(pm, "inst_map", None) or {}
                prov = inst_map.get(pid)
                if prov is not None:
                    return prov
            except Exception:
                pass
            try:
                fn = getattr(pm, "get_provider_by_id", None)
                if fn is not None:
                    res = fn(pid)
                    if inspect.isawaitable(res):
                        res = await res
                    if res is not None:
                        return res
            except Exception as e:
                logger.debug(f"[perf_guard] 按 id 取模型失败: {e}")
            logger.warning(f"[perf_guard] 配置的 ai_provider_id={pid} 不存在，回退到默认模型")

        # 会话级 / 全局默认模型
        try:
            from astrbot.core.provider.entities import ProviderType  # type: ignore

            ptype = ProviderType.CHAT_COMPLETION
        except Exception:
            ptype = None
        for meth in ("get_using_provider_async", "get_using_provider"):
            fn = getattr(pm, meth, None)
            if fn is None or ptype is None:
                continue
            try:
                res = fn(ptype, umo or None)
                if inspect.isawaitable(res):
                    res = await res
                if res is not None:
                    return res
            except Exception as e:
                logger.debug(f"[perf_guard] {meth} 取模型失败: {e}")

        for attr in ("curr_provider_inst",):
            prov = getattr(pm, attr, None)
            if prov is not None:
                return prov
        insts = getattr(pm, "provider_insts", None) or []
        return insts[0] if insts else None

    @staticmethod
    def _provider_label(prov: Any) -> str:
        try:
            meta = prov.meta()
            return f"{meta.id}／{meta.model}"
        except Exception:
            name = getattr(prov, "model_name", "") or ""
            return str(name) or "未知模型"

    async def _ai_call_model(self, prov: Any, prompt: str) -> str | None:
        """调用模型拿回文本；失败返回 None（只记日志，不影响机器人）。"""
        timeout = max(10, self._int_cfg("ai_timeout_seconds", 60))
        try:
            resp = await asyncio.wait_for(
                prov.text_chat(prompt=prompt, system_prompt=AI_SYSTEM_PROMPT),
                timeout=float(timeout),
            )
        except asyncio.TimeoutError:
            logger.warning(f"[perf_guard] AI 评估超时（>{timeout}s）")
            return None
        except Exception as e:
            logger.warning(f"[perf_guard] AI 评估调用失败: {type(e).__name__}: {e}")
            return None
        text = getattr(resp, "completion_text", None)
        if str(getattr(resp, "role", "") or "") == "err":
            logger.warning("[perf_guard] AI 评估拿到的是错误响应，已忽略")
            return None
        if text is None:
            text = getattr(resp, "result", None) or getattr(resp, "_completion_text", "")
        return str(text or "").strip()

    def _ai_state_summary(self) -> str:
        """给模型看的紧凑状态摘要（尽量短，省 token 也更快）。"""
        mem = _read_memory_mb()
        uptime = _process_uptime_seconds()
        lag = list(self._lag_records)
        threshold = self._int_cfg("lag_threshold_ms", 1000)
        over = sum(1 for v in lag if v * 1000.0 >= threshold)
        msg = list(self._msg_latency)
        llm = list(self._llm_latency)

        lines: list[str] = []
        if mem:
            lines.append(
                f"内存：{mem[0]:.0f} MB（峰值 {mem[1]:.0f} MB），报警阈值 "
                f"{self._int_cfg('memory_threshold_mb', 800)} MB"
            )
        if uptime:
            lines.append(f"进程已运行：{_fmt_duration(uptime)}")
        if msg:
            lines.append(
                f"消息响应耗时：p50 {self._percentile(msg, 50):.2f}s / "
                f"p95 {self._percentile(msg, 95):.2f}s / 最慢 {max(msg):.2f}s（{len(msg)} 条样本）"
            )
        else:
            lines.append("消息响应耗时：暂无样本")
        if llm:
            p50_llm = self._percentile(llm, 50)
            ratio = ""
            if msg:
                p50_msg = self._percentile(msg, 50)
                if p50_msg > 0:
                    ratio = f"，占响应总耗时约 {p50_llm / p50_msg * 100:.0f}%"
            lines.append(f"LLM 调用耗时：p50 {p50_llm:.2f}s{ratio}")
        if lag:
            lines.append(
                f"事件循环：{len(lag)} 次采样，超过 {threshold}ms 的卡顿 {over} 次，"
                f"p95 延迟 {self._percentile(lag, 95) * 1000:.0f}ms"
            )
        else:
            lines.append("事件循环：暂无采样")
        if self._slow_hits:
            top = sorted(self._slow_hits.items(), key=lambda kv: kv[1], reverse=True)[:3]
            lines.append("卡顿嫌疑：" + "，".join(f"{n}×{c}" for n, c in top))
        try:
            dirs = self._log_dirs()
            files = self._collect_log_files(dirs)
            log_mb = sum(f.stat().st_size for f in files if f.exists()) / 1048576.0
            lines.append(
                f"日志占用：{log_mb:.1f} MB（保留 {self._int_cfg('log_keep_days', 7)} 天，"
                f"上限 {self._int_cfg('log_max_total_mb', 200)} MB，共 {len(files)} 个文件）"
            )
        except Exception:
            pass
        lines.append(f"上下文条数上限：{self._int_cfg('max_context_messages', 0)}（0 表示不裁剪）")
        # 让 AI 也能看到"报错"和"模型失败"这两类非性能问题，判断才不会只盯着内存
        err_total = sum(int(i.get("count") or 1) for i in self._plugin_errors)
        if err_total:
            top = sorted(self._plugin_error_hits.items(), key=lambda kv: kv[1], reverse=True)[:3]
            last = self._plugin_errors[-1] if self._plugin_errors else {}
            lines.append(
                f"插件报错：累计 {err_total} 次（最多："
                + "，".join(f"{n}×{c}" for n, c in top)
                + f"），最近：{last.get('plugin')}/{last.get('handler')} {last.get('error')}"
            )
        else:
            lines.append("插件报错：无")
        lines.append(self._model_health_line().lstrip("· "))
        try:
            failed = self._failed_plugins()
            lines.append(
                "插件加载失败：" + ("；".join(n for n, _ in failed) if failed else "无")
            )
        except Exception:
            pass
        lines.append(
            "自动功能：内存自动释放 "
            f"{self._int_cfg('release_interval_seconds', 1800)}s / 日志清理 "
            f"{'开' if self._cfg('log_cleanup_enabled', True) else '关'} / 自动重启 "
            f"{'开' if self._cfg('auto_restart_enabled', False) else '关'} / 定时重启 "
            f"{'开' if self._cfg('scheduled_restart_enabled', False) else '关'}"
        )
        caps = self._restart_capabilities()
        lines.append("可用重启方式：" + ("、".join(k for k, v in caps.items() if v) or "无"))
        if self._slow_messages:
            recent = list(self._slow_messages)[-3:]
            lines.append(
                "最近慢消息："
                + "；".join(
                    f"{float(r.get('total') or 0):.1f}s"
                    f"（预处理{float(r.get('pre') or 0):.1f}/模型{float(r.get('llm') or 0):.1f}/"
                    f"后处理{float(r.get('post') or 0):.1f}）"
                    for r in recent
                )
            )
        if len(self._history) >= 6:
            mems = [float(s.get("mem") or 0) for s in self._history[-24:]]
            lines.append(
                f"内存走势（最近 {len(mems)} 个采样）：{mems[0]:.0f} → {mems[-1]:.0f} MB，"
                f"峰值 {max(mems):.0f} MB"
            )
        if self._trace_report:
            lines.append("内存追踪：有一份报告待查看（action=status 或 /perf 内存追踪 可看）")
        lines.append(f"最近一次 AI 动作：{self._ai_last_result or '无'}")
        return "\n".join(lines)

    @staticmethod
    def _extract_json(text: str) -> dict[str, Any] | None:
        """从模型输出里抠出 JSON 对象（容忍代码块标记与多余文字）。"""
        if not text:
            return None
        s = text.strip()
        s = re.sub(r"^```[a-zA-Z0-9_]*\s*", "", s)
        s = re.sub(r"```\s*$", "", s).strip()
        start = s.find("{")
        end = s.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            obj = json.loads(s[start : end + 1])
        except Exception:
            return None
        return obj if isinstance(obj, dict) else None

    def _validate_ai_proposal(self, obj: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """把模型输出校验成可执行的提议；不合法时返回 (None, 原因)。"""
        action = str(obj.get("action") or "").strip().lower()
        reason = str(obj.get("reason") or "").strip()[:200]
        try:
            conf = float(obj.get("confidence") or 0.0)
        except Exception:
            conf = 0.0
        conf = min(1.0, max(0.0, conf))
        raw_args = obj.get("args") if isinstance(obj.get("args"), dict) else {}

        if action not in AI_ACTIONS:
            return None, f"动作不在允许列表内：{action or '(空)'}"

        args: dict[str, Any] = {}
        if action == "restart":
            if not self._cfg("ai_allow_restart", False):
                return None, "配置未允许由 AI 提议重启（ai_allow_restart=false）"
            ok, why = self._can_restart()
            if not ok:
                return None, f"当前不具备重启条件：{why}"
        elif action == "set_context_limit":
            try:
                value = int(raw_args.get("value"))
            except Exception:
                return None, "set_context_limit 缺少合法的 value 参数"
            if value < 0 or value > 200:
                return None, f"上下文条数超出允许范围 0~200：{value}"
            args = {"value": value}

        return {
            "action": action,
            "args": args,
            "reason": reason or "（模型未给出理由）",
            "confidence": conf,
        }, ""

    def _ai_rate_ok(self) -> tuple[bool, str]:
        limit = max(0, self._int_cfg("ai_max_actions_per_hour", 3))
        if limit <= 0:
            return False, "已配置为不允许 AI 执行动作（ai_max_actions_per_hour=0）"
        now = time.time()
        self._ai_exec_ts = [t for t in self._ai_exec_ts if now - t < 3600]
        if len(self._ai_exec_ts) >= limit:
            return False, f"最近一小时已执行 {len(self._ai_exec_ts)} 次动作（上限 {limit} 次），本次不提议"
        return True, ""

    def _new_ai_id(self) -> str:
        self._ai_seq += 1
        return f"A{self._ai_seq}"

    def _prune_pending_ai(self) -> None:
        now = time.time()
        for pid in [k for k, v in self._pending_ai.items() if v.get("expire_ts", 0) <= now]:
            item = self._pending_ai.pop(pid, None)
            logger.info(f"[perf_guard] AI 提议 {pid} 已超时作废：{item.get('action') if item else ''}")

    async def _ai_review(self, reason: str, umo: str = "", auto: bool = False) -> str:
        """让模型评估当前状态并生成待审核提议。返回给用户/日志看的文本。"""
        if not self._cfg("ai_enabled", False):
            return "AI 自动化管理未启用（请在插件配置里打开 ai_enabled）。"
        self._prune_pending_ai()
        if len(self._pending_ai) >= 3:
            return "⚠️ 已有待审核的 AI 提议，请先「/perf 批准」或「/perf 拒绝」处理完再来。"
        ok, why = self._ai_rate_ok()
        if not ok:
            return f"⚠️ {why}"

        prov = await self._resolve_ai_provider(umo or None)
        if prov is None:
            return "⚠️ 找不到可用的对话模型提供商，无法评估（可在配置里指定 ai_provider_id）。"

        summary = self._ai_state_summary()
        logger.info(f"[perf_guard] 开始 AI 评估（触发原因：{reason}，模型：{self._provider_label(prov)}）")
        text = await self._ai_call_model(prov, f"当前状态：\n{summary}\n{AI_REVIEW_FOOTER}")
        self._ai_last_run_ts = time.time()
        if text is None:
            return "⚠️ AI 评估失败（模型调用出错），详情见日志。"

        obj = self._extract_json(text)
        if obj is None:
            logger.warning(f"[perf_guard] AI 返回无法解析：{text[:200]}")
            return f"⚠️ AI 返回的内容不是合法 JSON，已忽略。\n原始返回片段：{text[:150]}"

        prop, err = self._validate_ai_proposal(obj)
        if prop is None:
            self._ai_last_result = f"提议被安全规则拒绝（{err}）"
            logger.info(f"[perf_guard] AI 提议被拒绝：{err}｜模型原始输出：{text[:200]}")
            return (
                f"🛡 AI 提议被安全规则拒绝：{err}\n"
                f"· 模型原意：{obj.get('action')}（{str(obj.get('reason') or '')[:80]}）\n"
                f"· 模型：{self._provider_label(prov)}"
            )

        if prop["action"] == "noop":
            self._ai_last_result = f"评估为无需处理（{prop['reason'][:40]}）"
            return (
                "✅ AI 评估完成：当前无需处理。\n"
                f"· 理由：{prop['reason']}\n"
                f"· 模型：{self._provider_label(prov)}（置信度 {prop['confidence']:.2f}）"
            )

        timeout = max(30, self._int_cfg("ai_approval_timeout_seconds", 300))
        pid = self._new_ai_id()
        self._pending_ai[pid] = {
            "id": pid,
            "prop": prop,
            "reason": reason,
            "umo": umo,
            "created_ts": time.time(),
            "expire_ts": time.time() + timeout,
            "model": self._provider_label(prov),
            "auto": auto,
        }
        preview = self._ai_proposal_text(self._pending_ai[pid])
        target = (
            str(self._cfg("ai_notify_umo", "") or "").strip()
            or umo
            or str(self._cfg("notify_umo", "") or "").strip()
        )
        if target:
            sent = await self._send_to(target, preview)
            if not sent:
                logger.warning("[perf_guard] AI 提议已生成，但推送失败（请检查 ai_notify_umo）")
        logger.info(
            f"[perf_guard] AI 提议 {pid}：{prop['action']}｜{prop['reason']}｜等待管理员审核"
        )
        return preview

    def _ai_proposal_text(self, item: dict[str, Any]) -> str:
        prop = item.get("prop") or {}
        action = prop.get("action", "?")
        args = prop.get("args") or {}
        left = int(max(0, item.get("expire_ts", 0) - time.time()) // 60)
        lines = [
            "🤖 AI 运维提议（等待管理员审核）",
            f"· 编号：{item.get('id')}",
            f"· 动作：{action} —— {AI_ACTIONS.get(action, '')}",
        ]
        if args:
            lines.append(f"· 参数：{args}")
        lines.extend(
            [
                f"· AI 理由：{prop.get('reason', '')}",
                f"· 置信度：{float(prop.get('confidence') or 0):.2f}",
                f"· 触发原因：{item.get('reason', '')}",
                f"· 评估模型：{item.get('model', '未知')}",
                f"· 有效期：约 {left} 分钟后自动作废",
                f"👉 回复「/perf 批准 {item.get('id')}」执行，或「/perf 拒绝 {item.get('id')}」取消",
            ]
        )
        return "\n".join(lines)

    def _ai_pending_text(self) -> str:
        self._prune_pending_ai()
        if not self._pending_ai:
            return "📭 当前没有等待审核的 AI 提议。"
        lines = [f"📋 待审核的 AI 提议（{len(self._pending_ai)} 条）", ""]
        for item in self._pending_ai.values():
            lines.append(self._ai_proposal_text(item))
            lines.append("")
        return "\n".join(lines).strip()

    @staticmethod
    def _int_arg(text: str, default: int) -> int:
        """从指令里取第一个整数参数（/perf 报告 3 → 3）。"""
        for tok in text.split():
            if re.fullmatch(r"\d+", tok):
                return int(tok)
        return default

    @staticmethod
    def _pick_ai_id(text: str, keywords: set[str]) -> str:
        """从「/perf 批准 A1」里取出编号；只写「批准」时返回空串（单条待审时自动选）。"""
        toks = [t for t in text.split() if t and t not in keywords]
        for t in toks:
            if re.fullmatch(r"a?\d+", t, re.IGNORECASE):
                return t.upper() if t.lower().startswith("a") else f"A{t}"
        return toks[0].upper() if toks else ""

    async def _ai_decide(self, approve: bool, pid: str, event: AstrMessageEvent) -> str:
        """管理员批准/拒绝一条提议。这里是唯一真正执行 AI 动作的入口。"""
        self._prune_pending_ai()
        if not self._is_admin(event):
            return "⛔ 只有管理员可以审核 AI 提议。"
        if not self._pending_ai:
            return "📭 当前没有等待审核的 AI 提议。"

        pid = (pid or "").strip().upper()
        if not pid:
            if len(self._pending_ai) == 1:
                pid = next(iter(self._pending_ai))
            else:
                ids = "、".join(self._pending_ai.keys())
                return f"⚠️ 有多条待审核提议，请指定编号：{ids}\n用法：/perf 批准 {ids.split('、')[0]}"
        item = self._pending_ai.get(pid)
        if item is None:
            return f"⚠️ 找不到编号 {pid} 的提议（可能已超时或已处理）。{self._ai_pending_text()}"

        prop = item.get("prop") or {}
        action = prop.get("action", "?")
        approver = event.get_sender_id()

        if not approve:
            self._pending_ai.pop(pid, None)
            self._ai_last_result = f"提议 {pid}（{action}）被管理员拒绝"
            logger.info(f"[perf_guard] AI 提议 {pid}（{action}）被管理员 {approver} 拒绝")
            return f"🚫 已取消提议 {pid}（{action}），未执行任何操作。"

        # 批准：再检查一次频率限制与前置条件（配置可能在等待期间被改动）
        ok, why = self._ai_rate_ok()
        if not ok:
            self._pending_ai.pop(pid, None)
            return f"⚠️ 无法执行：{why}"
        prop2, err = self._validate_ai_proposal(dict(prop))
        if prop2 is None:
            self._pending_ai.pop(pid, None)
            return f"⚠️ 无法执行：{err}"

        self._pending_ai.pop(pid, None)
        self._ai_exec_ts.append(time.time())
        try:
            result = await self._ai_execute(prop2)
        except Exception as e:
            logger.error(f"[perf_guard] AI 动作执行失败（{action}）: {e}")
            self._ai_last_result = f"执行 {action} 失败（{type(e).__name__}）"
            return f"❌ 执行 {action} 时出错：{type(e).__name__}: {e}"
        self._ai_last_result = f"已执行 {action}（管理员 {approver} 批准）"
        self._state["ai_actions"] = int(self._state.get("ai_actions") or 0) + 1
        self._save_state()
        logger.info(f"[perf_guard] AI 动作 {action} 已由管理员 {approver} 批准并执行")
        return (
            f"✅ 已执行 AI 提议 {pid}：{action}\n"
            f"· 批准人：{approver}\n"
            f"· 执行结果：\n{result}"
        )

    async def _ai_execute(self, prop: dict[str, Any]) -> str:
        """执行已批准的提议。动作清单与 _validate_ai_proposal 严格对应。"""
        action = prop.get("action")
        if action == "release_memory":
            r = await self._release_memory(reason="AI 提议（管理员已批准）")
            return self._release_result_text(r)
        if action == "clean_logs":
            r = await self._clean_logs(reason="AI 提议（管理员已批准）")
            return self._clean_result_text(r)
        if action == "restart":
            return await self._restart("AI 提议（管理员已批准）")
        if action == "set_context_limit":
            value = int((prop.get("args") or {}).get("value", 0))
            self.config["max_context_messages"] = value
            await self._save_plugin_config()
            tail = "（0 表示不再裁剪上下文）" if value == 0 else ""
            return f"✅ 已把上下文条数上限设为 {value}{tail}，后续请求生效。"
        return "（未知动作，未执行任何操作）"

    async def _save_plugin_config(self) -> None:
        """保存插件配置，兼容不同 AstrBot 版本的方法名。"""
        for name in ("save_config", "save_config_async"):
            fn = getattr(self.config, name, None)
            if fn is None:
                continue
            try:
                res = fn()
                if inspect.isawaitable(res):
                    await res
                return
            except Exception as e:
                logger.warning(f"[perf_guard] 保存插件配置失败（{name}）: {e}")

    def _ai_models_text(self) -> str:
        """列出可选模型，方便填 ai_provider_id。"""
        pm = getattr(self.context, "provider_manager", None)
        if pm is None:
            return "⚠️ 无法访问模型管理器（context.provider_manager 不存在）。"
        rows: list[str] = []
        try:
            for prov in getattr(pm, "provider_insts", None) or []:
                try:
                    meta = prov.meta()
                    rows.append(f"· {meta.id} —— {meta.model}（{meta.type}）")
                except Exception:
                    rows.append(f"· {self._provider_label(prov)}")
        except Exception as e:
            return f"⚠️ 读取模型列表失败：{e}"
        if not rows:
            return "⚠️ 没有已加载的对话模型提供商。"
        cur = str(self._cfg("ai_provider_id", "") or "").strip() or "(未指定，用当前会话默认模型)"
        return (
            "🧠 可用的对话模型（把 id 填到配置项 ai_provider_id）\n"
            + "\n".join(rows)
            + f"\n\n当前配置：{cur}"
        )

    def _ai_status_line(self) -> str:
        if not self._cfg("ai_enabled", False):
            return "· AI 管理：关（ai_enabled=false）"
        self._prune_pending_ai()
        pid = str(self._cfg("ai_provider_id", "") or "").strip() or "当前会话默认模型"
        return (
            f"· AI 管理：开（模型 {pid}｜审核制，待审 {len(self._pending_ai)} 条｜"
            f"近 1 小时已执行 {len(self._ai_exec_ts)} 次｜"
            f"允许提议重启 {'是' if self._cfg('ai_allow_restart', False) else '否'}）"
        )

    # ------------------------------------------------------------------
    # 配置 / 状态持久化 / 权限
    # ------------------------------------------------------------------
    def _cfg(self, key: str, default: Any = None) -> Any:
        try:
            val = self.config.get(key, default)
        except Exception:
            val = default
        return default if val is None else val

    def _int_cfg(self, key: str, default: int) -> int:
        try:
            return int(float(self._cfg(key, default)))
        except Exception:
            return default

    def _load_state(self) -> None:
        defaults: dict[str, Any] = {
            "restarts": 0,
            "last_restart_ts": 0.0,
            "last_restart_reason": "",
            "last_scheduled_date": "",
            "releases": 0,
            "freed_total_mb": 0.0,
            "last_release_ts": 0.0,
            "last_release_reason": "",
            "log_cleans": 0,
            "log_deleted_files": 0,
            "log_freed_mb": 0.0,
            "last_log_clean_ts": 0.0,
        }
        try:
            with open(self._state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                for k, v in data.items():
                    if k in defaults:
                        defaults[k] = v
        except Exception:
            pass
        self._state = defaults

    def _save_state(self, *, force: bool = False) -> None:
        """落盘统计数据。

        默认做去抖（默认 20 秒内最多写一次盘）：状态变化发生在“卡顿刚被检测到”
        这类最糟糕的时刻，如果每次都同步写盘 + os.replace，插件自己就会加重卡顿。
        """
        if not self._state_path:
            return
        self._state_dirty = True
        if not force and (time.time() - self._last_state_save) < self._STATE_SAVE_MIN_INTERVAL:
            return
        self._flush_state()

    def _flush_state(self) -> None:
        try:
            tmp = f"{self._state_path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._state, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self._state_path)
            self._last_state_save = time.time()
            self._state_dirty = False
        except Exception as e:
            logger.debug(f"[perf_guard] 状态保存失败: {e}")

    def _is_admin(self, event: AstrMessageEvent) -> bool:
        try:
            if event.is_admin():
                return True
        except Exception:
            pass
        try:
            sid = str(event.get_sender_id())
            admins = self.context.get_config().get("admins_id", []) or []
            return any(str(a) == sid for a in admins)
        except Exception:
            return False
