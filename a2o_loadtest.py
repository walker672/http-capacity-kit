#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
a2o_loadtest.py — 站点负载/容量测试工具（HTTP/1.1，threads / asyncio 双引擎）

能力
----
  · 目标由 --url 指定，无内置锁定；多个 URL 用逗号分隔或走 --url-file 轮换
  · 并发：threads 引擎 1..2000（1 VU = 1 线程），asyncio 引擎 1..20000（1 VU = 1 协程）
  · 阶梯爬坡（--ramp-up）：错峰启动，能看出容量拐点落在哪个并发区间
  · 自动熔断可选，默认关闭；加 --fuse 或传阈值参数才启用
  · 探测体积可调（--probe-bytes）：默认每次只读 100B，传 full 则整档下载
  · --duration 0 无限运行（Ctrl+C 停止并输出汇总）；--max-egress-mb 限制累计出流量
  · 逐秒时序采集 + 分位数统计 + 吞吐(MB/s、Mbit/s) + JSON 输出

使用边界
--------
仅用于你拥有、或已获书面授权的站点。--authorized 是显式的责任声明；
对第三方站点施压属于对计算机信息系统的破坏行为。

用法
----
  python a2o_loadtest.py --url https://example.com/ --concurrency 5 --duration 5

  # 分级压测找拐点（默认不熔断，跑满时长）
  python a2o_loadtest.py --url https://example.com/assets/app.js --concurrency 200 --duration 60 --ramp-up 20 --authorized

  # 需要自动熔断时
  python a2o_loadtest.py --url https://example.com/ --concurrency 1000 --duration 60 --authorized --fuse

  # 机器可读
  python a2o_loadtest.py --url https://example.com/ --concurrency 100 --duration 60 --authorized --json

  # 整档下载（真实带宽/传输容量测试，注意出流量）
  python a2o_loadtest.py --url https://example.com/video.mp4 --probe-bytes full --concurrency 20 --duration 30 --authorized

  # 无限运行：--duration 0 表示一直跑到 Ctrl+C，停之前照常输出汇总与时序表
  python a2o_loadtest.py --url https://example.com/assets/app.js --concurrency 200 --duration 0 --ramp-up 20 --authorized
  python a2o_loadtest.py --url https://example.com/assets/app.js --concurrency 2000 --duration 0 --ramp-up 60 --authorized

  # 搬运大体积数据：多个大文件轮换 + 整档下载，单轮就是几 MB
  python a2o_loadtest.py --url https://example.com/a.jpg,https://example.com/b.mp4 \
      --probe-bytes full --concurrency 200 --duration 60 --authorized

  # 大清单 + 出流量封顶（跑到 5 GB 自动停，防账单失控）
  python a2o_loadtest.py --url-file urls.txt --probe-bytes full --concurrency 200 \
      --duration 0 --authorized --max-egress-mb 5000

  # 自动扫站点上的大文件（图片/视频/音频，跟到 app.js 里找），只留 >= 1MB 的
  # 扫描只发几十次普通 GET，不算压测，不需要 --authorized
  python a2o_loadtest.py --scan --url https://example.com/ --min-size 1m --out urls.txt

  # 单轮多段：1 个虚拟用户一次循环连续拉 8 段，单轮搬运量 x8
  python a2o_loadtest.py --url https://example.com/a.jpg --probe-bytes 4m \
      --segments 8 --concurrency 200 --duration 60 --authorized
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import http.client
import json
import re
import ssl
import statistics
import sys
import threading
import time
from array import array
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit

# ---------------------------------------------------------------- 常量

DEFAULT_PROBE_BYTES = 100     # 默认 Range 探测字节数（--probe-bytes 可改）
FULL_BODY = 0                 # probe=0 / full：不发 Range，整档下载
DEFAULT_TIMEOUT = 20.0
SAFE_CONCURRENCY = 5          # 不声明授权时的默认上限
THREADS_MAX = 2_000           # 线程引擎实测上限（1 VU = 1 线程）
ASYNC_MAX = 20_000            # asyncio 引擎上限（1 VU = 1 协程）

CHUNK = 1 << 16               # 单次读 body 的块大小
TICK = 0.5                    # 熔断检查 / 进度刷新间隔（秒）
REPORT_EVERY = 60.0           # 长跑时每 N 秒打一行累计概况（无限运行才用得上）
MAX_WINDOW = 900              # 逐秒时序最多保留多少秒，防止无限运行时内存无限涨
MAX_LAT = 2_000_000           # 延迟样本上限（array('f')，200 万条约 8MB）
TIMELINE_ROWS = 300           # 无限运行时时序表只打印最后多少秒

UA = "loadtest/2.3 (authorized; range-probe)"

# 扫描：从 HTML/CSS/JS 里认这些后缀，且默认只留"大对象"
ASSET_EXT = (".jpg", ".jpeg", ".png", ".webp", ".avif", ".gif", ".bmp", ".svg",
             ".mp4", ".webm", ".mov", ".m4v", ".mkv",
             ".mp3", ".m4a", ".aac", ".wav", ".flac", ".ogg",
             ".m3u8", ".ts", ".mpd",
             ".pdf", ".zip", ".woff2", ".woff", ".ttf", ".otf",
             ".js", ".css", ".wasm")
SCAN_HTML_MAX = 4 << 20       # 单个 HTML/CSS/JS 最多读 4MB，够抽 URL 了
SCAN_PROBE_MAX = 200          # 最多给多少个候选做体积探测（每个一次 HEAD）
SCAN_FOLLOW_MAX = 120         # 每层最多跟读多少个 JS/CSS（Vite/webpack 拆包常有 20~60 个 chunk）
SCAN_WORKERS = 8              # 并行称重的线程数：串行逐个 HEAD 在 200 个候选上要一分钟
HLS_MAX = 2 << 20             # 单个 m3u8 最多读 2MB（正常只有几 KB）
HLS_SAMPLES = 8               # 每个视频抽几个切片称重，用于外推总大小
HLS_DEPTH = 3                 # master → 档位 → 切片，最多递归几层


# ---------------------------------------------------------------- 目标解析

class Target:
    __slots__ = ("scheme", "host", "port", "path", "secure")

    def __init__(self, url: str) -> None:
        sp = urlsplit(url)
        if sp.scheme not in ("http", "https"):
            raise ValueError(f"只支持 http/https，收到：{sp.scheme or '(空)'}")
        if not sp.hostname:
            raise ValueError(f"URL 缺少主机名：{url}")
        self.scheme = sp.scheme
        self.secure = sp.scheme == "https"
        self.host = sp.hostname
        self.port = sp.port or (443 if self.secure else 80)
        self.path = (sp.path or "/") + (f"?{sp.query}" if sp.query else "")

    @property
    def label(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}{self.path}"

    @property
    def host_header(self) -> str:
        default = 443 if self.secure else 80
        return self.host if self.port == default else f"{self.host}:{self.port}"

    @property
    def key(self) -> tuple[bool, str, int]:
        """同一 key 的连接可以复用；换了主机/端口就必须重连。"""
        return (self.secure, self.host, self.port)


# ---------------------------------------------------------------- 统计容器

class Stats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.status: dict[int, int] = {}
        self.errors: dict[str, int] = {}
        self.lat = array("f")                  # 秒，float32 足够且省内存
        self.total = 0
        self.bytes = 0
        self.abort = threading.Event()
        self.abort_reason = ""
        self.started = 0.0
        self.window: dict[int, list] = {}      # 秒 -> [ok, err, lat_sum, lat_max]
        self.next_report = REPORT_EVERY        # 下一次打累计概况的时刻
        self.user_stop = False                 # 是否被 Ctrl+C 主动中断
        self.inflight = 0                      # 在途请求已预留的字节额度
        self.port_exhausted = False            # 是否撞上本机临时端口耗尽
        self._prune_at = 0

    def reserve(self, cap: int, n: int) -> bool:
        """发请求前先占额度：并发下不预留的话，在途请求会一起把总量冲过上限。"""
        with self._lock:
            if cap and self.bytes + self.inflight + n > cap:
                return False
            self.inflight += n
            return True

    def unreserve(self, n: int) -> None:
        with self._lock:
            self.inflight = max(0, self.inflight - n)

    def _sec_window(self) -> list:
        """取当前秒的桶；顺带按 MAX_WINDOW 丢掉过老的秒，避免无限运行吃内存。"""
        sec = int(time.monotonic() - self.started)
        if sec >= self._prune_at:
            self._prune_at = sec + 60
            cutoff = sec - MAX_WINDOW
            if cutoff > 0:
                for k in [k for k in self.window if k < cutoff]:
                    del self.window[k]
        return self.window.setdefault(sec, [0, 0, 0.0, 0.0])

    def record(self, status: int, dt: float, nbytes: int) -> None:
        ok = status in (200, 206)
        with self._lock:
            self.total += 1
            self.status[status] = self.status.get(status, 0) + 1
            self.bytes += nbytes            # 非 2xx 的回包同样消耗流量，也要计入
            w = self._sec_window()
            if ok:
                if len(self.lat) < MAX_LAT:
                    self.lat.append(dt)
                w[0] += 1
                w[2] += dt
                w[3] = max(w[3], dt)
            else:
                w[1] += 1

    def record_error(self, reason: str) -> None:
        key = reason.split(":")[0][:40] or "unknown"
        # 10048 = 地址已被占用（端口用尽），10055 = 缓冲区不足；都是客户端侧瓶颈
        if "10048" in reason or "10055" in reason:
            self.port_exhausted = True
        with self._lock:
            self.total += 1
            self.errors[key] = self.errors.get(key, 0) + 1
            self.status[-1] = self.status.get(-1, 0) + 1
            self._sec_window()[1] += 1

    def snapshot(self) -> tuple[int, int, dict[int, int]]:
        with self._lock:
            return self.total, sum(self.errors.values()), dict(self.status)

    def stop(self, reason: str) -> None:
        if not self.abort.is_set():
            self.abort_reason = reason
        self.abort.set()

    def interrupt(self) -> None:
        """Ctrl+C：标记为用户中断，让所有虚拟用户尽快退出循环。"""
        self.user_stop = True
        if not self.abort.is_set():
            self.abort_reason = "用户中断 Ctrl+C"
        self.abort.set()


@dataclass(frozen=True)
class Run:
    """一次加压的完整参数，避免每个函数都摊开十几个入参。"""
    targets: tuple[Target, ...]     # 多个目标时轮流请求，用于搬运更大体积的数据
    n: int
    ramp_up: float
    deadline: float
    stats: Stats
    timeout: float
    limit: int | None               # None = 整档下载
    fuse_on: bool
    max_err: float
    abort_codes: set[int]
    live: bool
    max_egress_mb: float | None = None   # 累计出流量上限，超过即停
    egress_cap: int = 0                  # 同上，换算成字节，0 表示不限
    segments: int = 1                    # 一轮循环内连续请求多少段（单轮搬运量 xN）
    seg_bytes: int = 0                   # 单请求预计字节，用于出流量额度预留


# ---------------------------------------------------------------- HTTP 工具

def _safe_close(obj) -> None:
    try:
        obj.close()
    except Exception:                                  # noqa: BLE001
        pass


_PROXY: tuple[str, int] | None = None      # (host, port)，由 --proxy 设置


def _new_conn(t: Target, timeout: float):
    if _PROXY:
        # 走 HTTP 代理：先连代理、发 CONNECT 建隧道，再在隧道上做 TLS。
        # http.client 的 set_tunnel 正好是这个语义，host/port 传代理地址即可。
        ph, pp = _PROXY
        conn = http.client.HTTPSConnection(ph, pp, timeout=timeout,
                                           context=ssl.create_default_context())
        conn.set_tunnel(t.host, t.port)
        return conn
    if t.secure:
        ctx = ssl.create_default_context()
        return http.client.HTTPSConnection(t.host, t.port, timeout=timeout, context=ctx)
    return http.client.HTTPConnection(t.host, t.port, timeout=timeout)


def _drain(resp, limit: int | None) -> int:
    """分块读取响应体并计数：limit 为 None 时读到 EOF，避免整档进内存。"""
    n = 0
    while limit is None or n < limit:
        chunk = resp.read(CHUNK if limit is None else min(CHUNK, limit - n))
        if not chunk:
            break
        n += len(chunk)
    return n


def _one_request(conn, t: Target, limit: int | None,
                 offset: int = 0) -> tuple[int, int, bool]:
    """返回 (status, bytes, reuse_ok)。offset > 0 时取该偏移之后的 limit 字节。"""
    headers = {"Accept-Encoding": "identity", "User-Agent": UA,
               "Connection": "keep-alive"}
    if limit is not None:
        headers["Range"] = f"bytes={offset}-{offset + limit - 1}"
    conn.request("GET", t.path, headers=headers)
    resp = conn.getresponse()
    nbytes = _drain(resp, limit)
    reuse = not resp.will_close
    if limit is not None and resp.length:   # 服务端无视 Range 返回整档，只能弃连接
        reuse = False
    return resp.status, nbytes, reuse


def _build_request(t: Target, limit: int | None, offset: int = 0) -> bytes:
    head = (f"GET {t.path} HTTP/1.1\r\n"
            f"Host: {t.host_header}\r\n"
            f"Accept-Encoding: identity\r\n"
            f"User-Agent: {UA}\r\n"
            f"Connection: keep-alive\r\n")
    if limit is not None:
        head += f"Range: bytes={offset}-{offset + limit - 1}\r\n"
    return (head + "\r\n").encode("latin-1")


# ---------------------------------------------------------------- threads 引擎

def worker(idx: int, r: Run) -> None:
    """单个虚拟用户：按 ramp-up 错峰启动，然后轮流请求 targets 里的大文件。"""
    if r.ramp_up > 0 and r.n > 1:
        delay = r.ramp_up * idx / (r.n - 1)
        if delay > 0 and time.monotonic() + delay < r.deadline:
            time.sleep(delay)

    targets = r.targets
    n_t = len(targets)
    k = idx
    conn, conn_key = None, None
    while not r.stats.abort.is_set() and time.monotonic() < r.deadline:
        if r.egress_cap and r.stats.bytes >= r.egress_cap:
            r.stats.stop(f"出流量达到上限 {r.max_egress_mb:,.0f} MB")
            break
        t = targets[k % n_t]
        k += 1
        seg, off = 0, 0                       # segments：一轮连续拉多段，搬运量 xN
        while seg < r.segments:
            seg += 1
            if r.stats.abort.is_set() or time.monotonic() >= r.deadline:
                break
            if r.egress_cap and not r.stats.reserve(r.egress_cap, r.seg_bytes):
                r.stats.stop(f"出流量达到上限 {r.max_egress_mb:,.0f} MB")
                break
            try:
                if conn is None or conn_key != t.key:
                    if conn is not None:
                        _safe_close(conn)
                    conn = _new_conn(t, r.timeout)
                    conn.connect()
                    conn_key = t.key
                t0 = time.perf_counter()
                status, nbytes, reuse = _one_request(conn, t, r.limit, off)
                r.stats.record(status, time.perf_counter() - t0, nbytes)
                off += r.limit or 0
                if not reuse:
                    _safe_close(conn)
                    conn, conn_key = None, None
            except Exception as exc:                   # noqa: BLE001
                r.stats.record_error(f"{type(exc).__name__}: {exc}")
                if conn is not None:
                    _safe_close(conn)
                    conn, conn_key = None, None
                break
            finally:
                if r.egress_cap:
                    r.stats.unreserve(r.seg_bytes)
    if conn is not None:
        _safe_close(conn)


# ---------------------------------------------------------------- asyncio 引擎

async def _read_n(reader, n: int) -> int:
    """从流里恰读 n 字节并计数（分块，不整档驻留内存）。"""
    got = 0
    while got < n:
        chunk = await reader.read(min(CHUNK, n - got))
        if not chunk:
            break
        got += len(chunk)
    return got


async def _a_request(reader, writer, req: bytes,
                     limit: int | None) -> tuple[int, int, bool]:
    writer.write(req)
    await writer.drain()

    head = await reader.readuntil(b"\r\n\r\n")
    lines = head.split(b"\r\n")
    try:
        status = int(lines[0].split(b" ")[1])
    except Exception:                                  # noqa: BLE001
        status = -1

    hdrs: dict[str, str] = {}
    for ln in lines[1:]:
        if b":" in ln:
            k, v = ln.split(b":", 1)
            hdrs[k.strip().lower().decode("latin-1")] = v.strip().decode("latin-1")

    nbytes = 0
    reuse = hdrs.get("connection", "").lower() != "close"

    if status in (204, 304):
        pass
    elif "content-length" in hdrs:
        try:
            want = int(hdrs["content-length"])
        except ValueError:
            want = 0
        take = want if limit is None else min(want, limit)
        nbytes = await _read_n(reader, take)
        if limit is not None and take < want:          # 服务端无视 Range，包没读完
            reuse = False
    elif "chunked" in hdrs.get("transfer-encoding", "").lower():
        while True:
            size_line = await reader.readuntil(b"\r\n")
            try:
                size = int(size_line.strip().split(b";")[0], 16)
            except ValueError:
                break
            if size == 0:
                await reader.readuntil(b"\r\n")
                break
            if limit is not None and nbytes + size > limit:
                nbytes += await _read_n(reader, max(0, limit - nbytes))
                reuse = False
                break
            nbytes += await _read_n(reader, size)
            await reader.readexactly(2)
    else:
        # 无长度信息：body 以连接关闭为界，天然不可复用
        if limit is None:
            while True:
                chunk = await reader.read(CHUNK)
                if not chunk:
                    break
                nbytes += len(chunk)
        else:
            nbytes = await _read_n(reader, limit)
        reuse = False

    return status, nbytes, reuse


async def _a_vu(idx: int, r: Run, reqs: list[tuple[bytes, ...]], ctxs: list) -> None:
    """一个虚拟用户 = 一个协程，轮流请求 targets；上万协程也不会爆线程栈。"""
    if r.ramp_up > 0 and r.n > 1:
        d = r.ramp_up * idx / (r.n - 1)
        if d and time.monotonic() + d < r.deadline:
            await asyncio.sleep(d)

    targets = r.targets
    n_t = len(targets)
    k = idx
    reader = writer = None
    conn_key = None
    while not r.stats.abort.is_set() and time.monotonic() < r.deadline:
        if r.egress_cap and r.stats.bytes >= r.egress_cap:
            r.stats.stop(f"出流量达到上限 {r.max_egress_mb:,.0f} MB")
            break
        i = k % n_t
        t, req_set, ctx = targets[i], reqs[i], ctxs[i]
        k += 1
        for req in req_set:                    # segments：一组里连续发多段
            if r.stats.abort.is_set() or time.monotonic() >= r.deadline:
                break
            if r.egress_cap and not r.stats.reserve(r.egress_cap, r.seg_bytes):
                r.stats.stop(f"出流量达到上限 {r.max_egress_mb:,.0f} MB")
                break
            try:
                if writer is None or conn_key != t.key:
                    if writer is not None:
                        _safe_close(writer)
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(t.host, t.port, ssl=ctx,
                                                server_hostname=t.host if ctx else None,
                                                limit=CHUNK), r.timeout)
                    conn_key = t.key
                t0 = time.perf_counter()
                status, nbytes, reuse = await asyncio.wait_for(
                    _a_request(reader, writer, req, r.limit), r.timeout)
                r.stats.record(status, time.perf_counter() - t0, nbytes)
                if not reuse:
                    _safe_close(writer)
                    reader = writer = None
                    conn_key = None
            except Exception as exc:                       # noqa: BLE001
                r.stats.record_error(f"{type(exc).__name__}: {exc}")
                if writer is not None:
                    _safe_close(writer)
                    reader = writer = None
                    conn_key = None
                break
            finally:
                if r.egress_cap:
                    r.stats.unreserve(r.seg_bytes)


# ---------------------------------------------------------------- 引擎调度

def _fuse_trip(r: Run) -> str | None:
    if not r.fuse_on:
        return None
    total, errs, statuses = r.stats.snapshot()
    if total and errs / total > r.max_err and total >= 50:
        return f"错误率 {errs / total:.1%} 超过阈值 {r.max_err:.0%}"
    hit = [c for c in r.abort_codes if statuses.get(c)]
    return f"命中限流状态码 {hit}" if hit else None


def _tick(r: Run, last: tuple[int, float]) -> tuple[bool, tuple[int, float]]:
    """每 TICK 秒一次：查出流量上限、查熔断、刷进度。返回 (是否继续, 新基准)。"""
    if r.max_egress_mb and r.stats.bytes >= r.max_egress_mb * 1e6:
        r.stats.stop(f"出流量达到上限 {r.max_egress_mb:,.0f} MB")
        return False, last
    reason = _fuse_trip(r)
    if reason:
        r.stats.stop(reason)
        return False, last
    if not r.live:
        return True, last
    total, errs, _ = r.stats.snapshot()
    now = time.monotonic()
    el = now - r.stats.started
    qps = (total - last[0]) / max(1e-6, now - last[1])
    print(f"      t={el:5.1f}s  已完成 {total:>7}  "
          f"瞬时 {qps:>7.0f} req/s", end="\r", flush=True)
    if el >= r.stats.next_report:          # 长跑时定期留一行可回溯的累计概况
        r.stats.next_report = el + REPORT_EVERY
        rate = errs / total if total else 0.0
        print(f"\n  —— {el / 60:,.1f} 分钟 · 累计 {total:,} 请求 · "
              f"平均 {total / max(el, 1e-6):,.0f} req/s · "
              f"错误 {rate:.1%} · 出流量 {r.stats.bytes / 1e6:,.0f} MB ——")
    return True, (total, now)


def run_threads(r: Run) -> float:
    threads = [threading.Thread(target=worker, args=(i, r), daemon=True)
               for i in range(r.n)]
    for t in threads:
        t.start()

    last = (0, time.monotonic())
    interrupted = False
    try:
        while time.monotonic() < r.deadline and not r.stats.abort.is_set():
            time.sleep(TICK)
            go, last = _tick(r, last)
            if not go:
                break
    except KeyboardInterrupt:
        interrupted = True
        r.stats.interrupt()

    # 停止加压的时刻，而不是收尾完成的时刻：QPS 的分母只能算到这里，
    # 否则等尾部请求的几秒钟会把吞吐算低。
    stopped = min(time.monotonic(), r.deadline)
    # 正常结束时必须等在途请求跑完，否则尾部请求被漏计、汇总数偏小；
    # 主动中断时只宽限几秒，daemon 线程随进程退出。
    grace = 3.0 if interrupted else min(60.0, r.timeout + 5.0)
    stop_at = time.monotonic() + grace
    for t in threads:
        left = stop_at - time.monotonic()
        if left <= 0:
            break
        t.join(timeout=left)
    return stopped - r.stats.started


def run_asyncio(r: Run) -> float:
    ctxs = [ssl.create_default_context() if t.secure else None for t in r.targets]
    # 每个目标预构建 segments 条请求（不同 Range 偏移），虚拟用户直接取用
    reqs = [tuple(_build_request(t, r.limit, s * r.limit if r.limit else 0)
                  for s in range(r.segments)) for t in r.targets]

    async def _driver() -> None:
        tasks = [asyncio.create_task(_a_vu(i, r, reqs, ctxs)) for i in range(r.n)]
        last = (0, time.monotonic())
        while time.monotonic() < r.deadline and not r.stats.abort.is_set():
            await asyncio.sleep(TICK)
            go, last = _tick(r, last)
            if not go:
                break
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    try:
        asyncio.run(_driver())
    except KeyboardInterrupt:
        r.stats.interrupt()
    return time.monotonic() - r.stats.started


# ---------------------------------------------------------------- 输出

BAR = "=" * 74


def percentiles(vals, qs: tuple[float, ...]) -> tuple[list[float], float]:
    """一次排序同时求多个分位数与最大值（毫秒）。

    以前每个分位数各自 sorted 一次，200 万样本时等于 3 次全排序 + 3 份拷贝，
    光收尾就要 1 秒多。现在只排一次。
    """
    if not vals:
        return [0.0] * len(qs), 0.0
    s = sorted(vals)
    n = len(s)
    out = [s[min(n - 1, max(0, int(round(n * q)) - 1))] * 1000 for q in qs]
    return out, s[-1] * 1000


def steady_qps(stats: Stats, ramp_up: float, elapsed: float) -> float:
    """爬坡结束后的稳态吞吐。爬坡期并发还在陆续启动，混进去会把 QPS 系统性算低。

    稳态区间不足 3 秒时直接返回 0：样本太少的"稳态"只是噪声，报出来反而误导。
    """
    if elapsed - ramp_up < 3.0 or not stats.window:
        return 0.0
    secs = [s for s in stats.window if s >= ramp_up]
    if not secs:
        return 0.0
    cnt = sum(stats.window[s][0] + stats.window[s][1] for s in secs)
    span = min(elapsed - ramp_up, float(len(secs)))
    return cnt / span if span > 0 else 0.0


def parse_probe(raw: str) -> int:
    """'100' → 100B；'64k' / '1m' → 带单位；'full' / '0' → 整档下载。"""
    s = raw.strip().lower().replace("_", "")
    if s in ("full", "all", "0", "-1", "none"):
        return FULL_BODY
    mult, num = 1, s
    for suf, m in (("kb", 1024), ("k", 1024), ("mb", 1 << 20), ("m", 1 << 20),
                   ("gb", 1 << 30), ("g", 1 << 30)):
        if s.endswith(suf):
            num, mult = s[: -len(suf)], m
            break
    try:
        val = int(float(num) * mult)
    except ValueError:
        raise ValueError(f"需为字节数（可带 k/m/g，如 64k）或 full，收到：{raw}") from None
    if val < 1:
        raise ValueError(f"需 >= 1，或传 full 表示整档下载，收到：{raw}")
    return val


def probe_text(probe: int) -> str:
    if probe == FULL_BODY:
        return "整档下载（不发 Range，回包 = 文件实际大小）"
    return f"Range 0-{probe - 1}，单请求回包 {probe}B"


def print_timeline(stats: Stats, max_rows: int = 0) -> None:
    keys = sorted(stats.window)
    print(f"\n{BAR}")
    print(" 逐秒时序（看劣化拐点，而非只看平均值）")
    if max_rows and len(keys) > max_rows:
        print(f" 无限运行：仅显示最后 {max_rows} 秒（共 {len(keys)} 秒，全部数据以 --json 为准）")
    print(BAR)
    print(f"  {'秒':>4}  {'成功':>6}  {'失败':>6}  {'成功率':>7}  {'均值ms':>8}  {'峰值ms':>8}")
    for sec in (keys[-max_rows:] if max_rows else keys):
        ok, err, lsum, lmax = stats.window[sec]
        n = ok + err
        rate = (ok / n * 100) if n else 0.0
        mean = (lsum / ok * 1000) if ok else 0.0
        print(f"  {sec:>4}  {ok:>6}  {err:>6}  {rate:>6.1f}%  {mean:>8.0f}  {lmax * 1000:>8.0f}")


def baseline_latency(targets: tuple[Target, ...], timeout: float,
                     limit: int | None) -> tuple[float, int]:
    """各目标轮流采样，取中位延迟与中位回包大小（用于出流量预估）。"""
    vals: list[float] = []
    sizes: list[int] = []
    rounds = 3 if len(targets) == 1 else len(targets)
    for i in range(rounds):
        t = targets[i % len(targets)]
        c = _new_conn(t, timeout)
        try:
            c.connect()
            t0 = time.perf_counter()
            _, nbytes, _ = _one_request(c, t, limit)
            vals.append(time.perf_counter() - t0)
            sizes.append(nbytes)
        finally:
            _safe_close(c)
        time.sleep(0.2)
    lat = statistics.median(vals) if vals else 0.0
    size = int(statistics.mean(sizes)) if sizes else 0   # 多目标时按均值算单请求体积
    return lat, size


# ---------------------------------------------------------------- 资源扫描
# 目的：站点上的 HTML 只是壳，真正的大体积数据（图片/视频/音频）散在
# app.js、CSS、<img>/<video> 里。先把它们捞出来称重，再喂给 --url-file。

_RE_ABS = re.compile(r"https?://[^\s\"'<>()\\,\`;|]+")
_RE_ATTR = re.compile(r'(?:src|href|poster|data-src|data-original)\s*=\s*["\']([^"\']+)["\']',
                      re.I)
_RE_SRCSET = re.compile(r'srcset\s*=\s*["\']([^"\']+)["\']', re.I)
_RE_CSSURL = re.compile(r'url\(\s*["\']?([^"\')]+)["\']?\s*\)', re.I)
_RE_QUOTED = re.compile(r'["\']([^"\'\s]{2,300}?)["\']')
_RE_CSSCOMMENT = re.compile(r'/\*.*?\*/', re.S)


REDIRECT_CODES = (301, 302, 303, 307, 308)


def _http_fetch(url: str, timeout: float, max_bytes: int = 0,
                method: str = "GET", hops: int = 0) -> tuple[int, dict[str, str], bytes]:
    """同步取一次（扫描阶段用，串行、可控）。max_bytes 只对 GET 生效。

    自动跟随最多 5 次重定向：站点常把 http:// 跳到 https://、把 / 跳到 /index.html，
    不跟随的话扫描会拿到一个空 body，然后误报「没找到资源」。
    """
    t = Target(url)
    conn = _new_conn(t, timeout)
    try:
        conn.connect()
        headers = {"User-Agent": UA, "Accept": "*/*",
                   "Accept-Encoding": "identity", "Connection": "close"}
        if method == "GET" and max_bytes:
            headers["Range"] = f"bytes=0-{max_bytes - 1}"
        conn.request(method, t.path, headers=headers)
        resp = conn.getresponse()
        hdrs = {k.lower(): v for k, v in resp.getheaders()}
        st = resp.status
        body = resp.read(max_bytes) if method == "GET" else b""
        if method != "GET":
            resp.read()
    finally:
        _safe_close(conn)

    loc = hdrs.get("location")
    if st in REDIRECT_CODES and loc and hops < 5:
        nxt = urljoin(url, loc)
        if urlsplit(nxt).scheme in ("http", "https"):
            return _http_fetch(nxt, timeout, max_bytes, method, hops + 1)
    return st, hdrs, body


def _object_size(url: str, timeout: float) -> int:
    """先 HEAD 拿 Content-Length；不支持 HEAD 就 Range 0-0 读 Content-Range 的总长。"""
    try:
        st, hd, _ = _http_fetch(url, timeout, 0, "HEAD")
        n = int(hd.get("content-length", "0") or 0)
        if 200 <= st < 300 and n > 0:
            return n
    except Exception:                                  # noqa: BLE001
        pass
    try:
        st, hd, body = _http_fetch(url, timeout, 1)
        if not 200 <= st < 300:
            return 0            # 4xx/5xx 是错误页不是资源，别把它的长度当成文件体积
        cr = hd.get("content-range", "")
        if "/" in cr:
            tot = cr.rsplit("/", 1)[1].strip()
            if tot.isdigit():
                return int(tot)
        if st == 200:
            return int(hd.get("content-length", "0") or 0) or len(body)
        return 0            # 206 却没给总长：体积未知，宁可不收也不虚报
    except Exception:                                  # noqa: BLE001
        return 0


def _sample(seq: list, k: int) -> list:
    """均匀抽 k 个元素（首尾都取到），用于切片抽样。"""
    n = len(seq)
    if n <= k:
        return list(seq)
    step = (n - 1) / (k - 1) if k > 1 else 0
    return [seq[int(round(i * step))] for i in range(k)]


def _hls_num(line: str, key: str) -> int:
    m = re.search(rf"{key}=(\d+)", line)
    return int(m.group(1)) if m else 0


def _hls_expand(url: str, timeout: float, depth: int = 0) -> tuple[list[str], int]:
    """把 m3u8 展开成切片 URL 列表。多档播放列表取平均码率最高的一档。
    返回 (切片列表, 该档平均码率 bps)；码率未知时为 0。"""
    if depth > HLS_DEPTH:
        return [], 0
    st, _, body = _http_fetch(url, timeout, HLS_MAX)
    if not 200 <= st < 300:
        raise RuntimeError(f"HTTP {st}")
    lines = [ln.strip() for ln in body.decode("utf-8", "replace").splitlines() if ln.strip()]

    best_uri, best_bw = None, 0
    for i, ln in enumerate(lines):
        if ln.startswith("#EXT-X-STREAM-INF"):
            bw = _hls_num(ln, "AVERAGE-BANDWIDTH") or _hls_num(ln, "BANDWIDTH")
            uri = next((x for x in lines[i + 1:] if not x.startswith("#")), None)
            if uri and bw > best_bw:            # 取码率最高的一档
                best_uri, best_bw = urljoin(url, uri), bw
    if best_uri:
        segs, _ = _hls_expand(best_uri, timeout, depth + 1)
        return segs, best_bw

    segs = [urljoin(url, ln) for ln in lines if not ln.startswith("#")]
    return segs, 0


def _is_asset_url(url: str) -> bool:
    path = urlsplit(url).path.lower()
    return path.endswith(ASSET_EXT)


def _collect_urls(text: str, base: str, host: str,
                  same_host: bool) -> set[str]:
    """从 HTML/CSS/JS 文本里捞出所有资源 URL，转成绝对地址并按主机过滤。"""
    found: set[str] = set()
    sp_base = urlsplit(base)
    origin = f"{sp_base.scheme}://{sp_base.netloc}/"

    def push(raw: str) -> None:
        raw = raw.strip().strip("\"'`")
        if not raw or raw.startswith(("data:", "blob:", "mailto:", "javascript:", "#")):
            return
        try:
            absu = urljoin(base, raw)
        except Exception:                              # noqa: BLE001
            return
        sp = urlsplit(absu)
        if sp.scheme not in ("http", "https"):
            return
        if same_host and (sp.hostname or "").lower() != host.lower():
            return
        found.add(absu.split("#", 1)[0])
        # 打包器（Vite/webpack）会把路径写成相对站点根的 "assets/x.js"。
        # 相对当前 chunk 目录拼接会得到 /assets/assets/x.js，必然 404。
        # 两个候选都收下，称重时拿到 0 的自然被丢弃。
        if not raw.startswith(("/", "./", "../", "http://", "https://")):
            alt = urljoin(origin, raw)
            if alt != absu:
                found.add(alt.split("#", 1)[0])

    for m in _RE_ABS.finditer(text):
        push(m.group(0).rstrip("'\"),;`"))
    for m in _RE_ATTR.finditer(text):
        push(m.group(1))
    for m in _RE_SRCSET.finditer(text):
        for part in m.group(1).split(","):
            tok = part.strip().split(" ")[0]
            if tok:
                push(tok)
    for m in _RE_CSSURL.finditer(text):
        push(m.group(1))
    for m in _RE_QUOTED.finditer(text):          # app.js 里的 "/assets/x.mp4" 之类
        cand = m.group(1)
        if _is_asset_url(cand) or (cand.startswith("/") and "." in cand.rsplit("/", 1)[-1]):
            push(cand)
    return {u for u in found if _is_asset_url(u)}


def scan_assets(seed: str, timeout: float, min_size: int, max_items: int,
                follow: int, same_host: bool = True,
                hls: bool = False) -> tuple[list[tuple[str, int]], list[str], list[dict]]:
    """抓 seed 页面 → 顺带读 JS/CSS → 汇总资源 → 逐个称重 → 只留大对象。"""
    host = urlsplit(seed).hostname or ""
    st, hd, body = _http_fetch(seed, timeout, SCAN_HTML_MAX)
    if st >= 400:
        raise RuntimeError(f"入口页返回 HTTP {st}")
    ctype = hd.get("content-type", "").lower()
    charset = "utf-8"
    if "charset=" in ctype:
        charset = ctype.split("charset=", 1)[1].split(";")[0].strip() or "utf-8"
    try:
        html = body.decode(charset, "replace")
    except LookupError:
        html = body.decode("utf-8", "replace")

    urls = _collect_urls(html, seed, host, same_host)

    # 跟随 JS/CSS：SPA 的资源清单基本都在打包后的入口包里，媒体路径常在进一步
    # 懒加载的 chunk 里，所以要按层递归，且每层不能只取固定的几个（Vite 拆包常 20+）。
    if follow > 0:
        def _is_code(u: str) -> bool:
            return urlsplit(u).path.lower().endswith((".js", ".css", ".mjs"))

        loaded: set[str] = set()
        failed: list[str] = []
        frontier = sorted(u for u in urls if _is_code(u))[:SCAN_FOLLOW_MAX]
        for _ in range(follow):
            nxt: list[str] = []
            for u in frontier:
                if u in loaded:
                    continue
                loaded.add(u)
                dec = ""
                hard = False                   # 连接层失败才算真失败；干净 404 是预期的
                for _try in range(2):           # 抖一次就丢掉整段媒体清单，值得重试
                    try:
                        st, _, txt = _http_fetch(u, timeout, SCAN_HTML_MAX)
                    except Exception:              # noqa: BLE001
                        hard = True
                        continue
                    hard = False
                    if st < 400:
                        dec = txt.decode("utf-8", "replace")
                    break
                if not dec:
                    if hard:
                        failed.append(u)
                    continue
                more = _collect_urls(_RE_CSSCOMMENT.sub(" ", dec), u, host, same_host)
                fresh = more - urls
                urls |= more
                # 必须排序：nxt 来自集合差，遍历顺序随 hash 随机，再截断就会每次
                # 漏掉不同的 chunk —— 表现为同一站点两次扫描结果不一样。
                nxt.extend(sorted(x for x in fresh if _is_code(x)))
            if len(nxt) > SCAN_FOLLOW_MAX:
                print(f"  注意   发现 {len(nxt)} 个 JS/CSS，只跟读前 {SCAN_FOLLOW_MAX} 个"
                      f"（--scan-follow 分层控制，或直接调大上限）", file=sys.stderr)
            frontier = nxt[:SCAN_FOLLOW_MAX]
            if not frontier:
                break
        if failed:
            print(f"  注意   {len(failed)} 个 JS/CSS 读取失败（重试后仍失败），"
                  f"其中的资源可能漏掉：{failed[0][-46:]}", file=sys.stderr)

    cands = sorted(urls)[:SCAN_PROBE_MAX]
    sized: list[tuple[str, int]] = []
    dead: list[str] = []                   # 抽到了地址但称不出体积（404 / 连不上）
    hls_rows: list[dict] = []              # --hls 展开后的视频清单
    tty = sys.stderr.isatty()
    probe_timeout = min(timeout, 8.0)      # 外链不可达时别把扫描拖成几分钟

    # HLS：m3u8 本身只有 1KB 左右，真正的字节在切片里。展开取最高码率档，
    # 均匀抽几个切片称重后按数量外推，比逐个 HEAD 上百个切片快得多。
    if hls:
        m3u8 = [u for u in cands if urlsplit(u).path.lower().endswith(".m3u8")]
        if m3u8 and tty:
            print(f"\r  展开 {len(m3u8)} 个 HLS 播放列表…", end="", file=sys.stderr,
                  flush=True)
        for u in m3u8:
            row = {"url": u, "segs": 0, "est": 0, "avg": 0, "bw": 0, "err": ""}
            try:
                segs, bw = _hls_expand(u, timeout)
                row["segs"], row["bw"] = len(segs), bw
                if segs:
                    pick = _sample(segs, HLS_SAMPLES)
                    got = [(s, _object_size(s, probe_timeout)) for s in pick]
                    good = [n for _, n in got if n > 0]
                    if good:
                        row["avg"] = int(statistics.mean(good))
                        row["est"] = row["avg"] * len(segs)
                        # 抽到的切片按真实体积入清单，剩下的用估算值不进清单
                        sized.extend((s, n) for s, n in got if n > 0)
                    else:
                        row["err"] = "切片称不出体积"
                else:
                    row["err"] = "播放列表里没有切片"
            except Exception as exc:                   # noqa: BLE001
                row["err"] = f"{type(exc).__name__}: {str(exc)[:40]}"
            hls_rows.append(row)
        if m3u8 and tty:
            print("\r" + " " * 78 + "\r", end="", file=sys.stderr, flush=True)
        # m3u8 本体不进体积表（1KB 的量级，只会占位置）
        cands = [u for u in cands if not urlsplit(u).path.lower().endswith(".m3u8")]

    # 并行称重：纯粹是 IO 等待，8 路并发能把 200 个候选从一分钟压到十几秒
    workers = max(1, min(SCAN_WORKERS, len(cands)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        sizes = pool.map(lambda u: (u, _object_size(u, probe_timeout)), cands)
        for i, (u, size) in enumerate(sizes, 1):
            if size > 0:
                if size >= min_size:
                    sized.append((u, size))
            else:
                dead.append(u)
            if tty:                                    # 管道/重定向下不打动态进度行
                print(f"\r  称重 {i}/{len(cands)} … {size / 1024:,.0f} KB  "
                      f"{u[-52:]:<52}", end="", file=sys.stderr, flush=True)
    if cands and tty:
        print("\r" + " " * 78 + "\r", end="", file=sys.stderr, flush=True)

    sized.sort(key=lambda x: -x[1])
    return sized[:max_items], dead, hls_rows


def run_scan(args, seed: str) -> int:
    """--scan 模式：只产出清单，不发压测流量。"""
    try:
        min_size = parse_probe(args.min_size)
    except ValueError as exc:
        print(f"--min-size 无效：{exc}", file=sys.stderr)
        return 2
    max_items = max(1, args.max_items)

    print(BAR)
    print(f" 扫描   {seed}")
    print(f" 门槛   {'不限体积' if not min_size else f'>= {min_size / 1024:,.0f} KB'}"
          f" · 最多 {max_items} 个"
          f" · 跟读 JS/CSS {'开' if args.scan_follow > 0 else '关'}"
          f" · 范围 {'任意主机' if args.any_host else '同域'}")
    print(BAR)
    try:
        found, dead, hls_rows = scan_assets(seed, args.timeout, min_size, max_items,
                                            args.scan_follow, not args.any_host,
                                            args.hls)
        if not found and not args.any_host:
            print("  同域没捞到资源，放宽到任意主机（CDN / 对象存储）再扫一次…",
                  file=sys.stderr)
            found, dead, hls_rows = scan_assets(seed, args.timeout, min_size, max_items,
                                                args.scan_follow, False, args.hls)
    except Exception as exc:                           # noqa: BLE001
        print(f"扫描失败：{type(exc).__name__}: {exc}", file=sys.stderr)
        return 2

    if dead:
        hosts = sorted({urlsplit(u).hostname or "?" for u in dead})
        print(f" 注意   {len(dead)} 个地址抽到了但称不出体积（404 或被网络拦截），"
              f"已排除：{', '.join(hosts[:3])}{' …' if len(hosts) > 3 else ''}",
              file=sys.stderr)

    if hls_rows:
        print(BAR)
        print(" 视频（HLS 最高码率档，总体积按切片抽样外推）")
        print(BAR)
        for r in hls_rows:
            if r["err"]:
                print(f"  !  {r['url'][-58:]}  {r['err']}")
                continue
            print(f"  {r['est'] / 1e6:>8.1f} MB  {r['segs']:>3} 片 · "
                  f"单片约 {r['avg'] / 1e6:.2f} MB · "
                  f"{r['bw'] / 1e6:.1f} Mbps · {r['url'][-46:]}")
        ok = [r for r in hls_rows if not r["err"]]
        if ok:
            print(f"  小计 {sum(r['est'] for r in ok) / 1e6:,.1f} MB，"
                  f"共 {len(ok)} 个视频 {sum(r['segs'] for r in ok)} 个切片")

    if not found:
        print("没找到符合条件的资源。放宽一点：--min-size 0 --scan-follow 1 --any-host")
        return 1

    try:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(f"# a2o_loadtest.py --scan 生成，来源 {seed}\n")
            fh.write(f"# {len(found)} 条，合计 {sum(s for _, s in found) / 1e6:.1f} MB，按体积降序\n")
            for u, _ in found:
                fh.write(u + "\n")
    except OSError as exc:
        print(f"写不进 {args.out}：{exc}", file=sys.stderr)
        return 2

    print(f"  {'#':>3}  {'体积':>9}  URL")
    for i, (u, s) in enumerate(found, 1):
        print(f"  {i:>3}  {s / 1e6:>7.2f}MB  {u}")

    total = sum(s for _, s in found)
    print(f"\n{BAR}")
    print(f" 已写入 {args.out}：{len(found)} 个资源，合计 {total / 1e6:.1f} MB"
          f"（整档跑完一遍 = 一个虚拟用户搬 {total / 1e6:.1f} MB）")
    print(BAR)
    print(" 下一步 · 整档搬运（回包 = 文件实际大小，最接近真实用户行为）")
    print(f"   python a2o_loadtest.py --url-file {args.out} --probe-bytes full "
          f"--concurrency 100 --duration 60 --authorized")
    print(" 下一步 · 分段搬运（每请求固定字节 + 单轮多段，控制单请求成本）")
    print(f"   python a2o_loadtest.py --url-file {args.out} --probe-bytes 1m "
          f"--segments 4 --concurrency 200 --duration 0 --authorized --max-egress-mb 2000")
    print(BAR)
    return 0



# ---------------------------------------------------------------- 主流程

def main() -> int:
    ap = argparse.ArgumentParser(
        description="站点负载测试（阶梯加压 + 熔断 + 逐秒时序）")
    ap.add_argument("--url", default=None,
                    help="目标 URL（含协议）。可逗号分隔多个，轮换请求以搬运更大体积数据")
    ap.add_argument("--url-file", default=None,
                    help="URL 清单文件，每行一个（# 开头为注释）；与 --url 可叠加")
    ap.add_argument("--concurrency", type=int, default=SAFE_CONCURRENCY,
                    help=f"并发虚拟用户数（线程引擎 1..{THREADS_MAX}，"
                         f"asyncio 引擎 1..{ASYNC_MAX}，默认 {SAFE_CONCURRENCY}）")
    ap.add_argument("--engine", choices=("auto", "threads", "asyncio"), default="auto",
                    help="执行引擎：auto 时 >500 并发自动切 asyncio（默认 auto）")
    ap.add_argument("--duration", type=float, default=30.0,
                    help="稳态持续秒数（默认 30）；传 0 表示无限运行，直到按 Ctrl+C")
    ap.add_argument("--ramp-up", type=float, default=10.0, help="爬坡秒数（默认 10）")
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="单请求超时秒")
    ap.add_argument("--probe-bytes", default=str(DEFAULT_PROBE_BYTES),
                    metavar="N|64k|1m|full",
                    help=f"每个请求读取的响应体字节数（默认 {DEFAULT_PROBE_BYTES}）；"
                         f"可带 k/m/g 单位，传 full 或 0 表示整档下载")
    ap.add_argument("--segments", type=int, default=1, metavar="N",
                    help="一轮循环内连续请求 N 段（不同 Range 偏移），单轮搬运量 xN；"
                         "仅对固定字节探测有效，整档下载时强制为 1")
    ap.add_argument("--scan", action="store_true",
                    help="只扫描站点上的大文件（图片/视频/音频/JS），生成清单后退出，不压测")
    ap.add_argument("--min-size", default="0", metavar="N|1m",
                    help="--scan 时只保留体积 >= 该值的资源（默认 0，全部保留）")
    ap.add_argument("--max-items", type=int, default=30,
                    help="--scan 清单最多保留多少个资源（默认 30，按体积从大到小）")
    ap.add_argument("--scan-follow", type=int, default=2, metavar="N",
                    help="--scan 时跟读几层 JS/CSS 找资源（默认 2；SPA 拆包需要 2 层才够，"
                         "0 只看入口页 HTML）")
    ap.add_argument("--any-host", action="store_true",
                    help="--scan 时不限制同域，跨域 CDN 资源也收进清单")
    ap.add_argument("--hls", action="store_true",
                    help="--scan 时展开 m3u8（取最高码率档），按切片抽样估算视频总体积")
    ap.add_argument("--out", default="urls.txt",
                    help="--scan 输出的清单文件路径（默认 urls.txt）")
    ap.add_argument("--fuse", action="store_true",
                    help="开启自动熔断（默认关闭，跑满时长）")
    ap.add_argument("--max-error-rate", type=float, default=None,
                    help="错误率熔断阈值（传入即自动开启熔断，默认 0.05 当 --fuse 时）")
    ap.add_argument("--abort-on", default=None,
                    help="命中即熔断的状态码，逗号分隔（传入即自动开启熔断，"
                         "默认 429,503 当 --fuse 时）")
    ap.add_argument("--authorized", action="store_true",
                    help="声明已获站方授权；并发 > 5 时必填")
    ap.add_argument("--max-egress-mb", type=float, default=None, metavar="MB",
                    help="累计出流量超过该 MB 数即停止（长跑防意外账单）")
    ap.add_argument("--proxy", default=None, metavar="http://host:port",
                    help="经 HTTP 代理访问目标（CONNECT 隧道）。目标在墙内不可达、"
                         "或必须走本机代理时用；注意此时瓶颈常常是代理而不是站点")
    ap.add_argument("--json", action="store_true", help="输出 JSON")
    ap.add_argument("--dry-run", action="store_true", help="只打印计划，不发包")
    args = ap.parse_args()

    global _PROXY
    if args.proxy:
        ps = urlsplit(args.proxy if "//" in args.proxy else f"http://{args.proxy}")
        if not ps.hostname:
            print(f"--proxy 无效：{args.proxy}", file=sys.stderr)
            return 2
        _PROXY = (ps.hostname, ps.port or 8080)

    raw_urls: list[str] = []
    if args.url:
        raw_urls += [u.strip() for u in args.url.split(",") if u.strip()]
    if args.url_file:
        try:
            with open(args.url_file, encoding="utf-8") as fh:
                raw_urls += [ln.strip() for ln in fh
                             if ln.strip() and not ln.lstrip().startswith("#")]
        except OSError as exc:
            print(f"读不到 --url-file：{exc}", file=sys.stderr)
            return 2
    if not raw_urls:
        print("需要 --url 或 --url-file 指定至少一个目标", file=sys.stderr)
        return 2

    if args.scan:                       # 扫描模式：只出清单，不压测
        return run_scan(args, raw_urls[0])

    seen: dict[str, Target] = {}
    try:
        for u in raw_urls:
            t = Target(u)
            seen.setdefault(t.label, t)      # 同一个 URL 重复出现只算一次
    except ValueError as exc:
        print(f"URL 无效：{exc}", file=sys.stderr)
        return 2
    targets = tuple(seen.values())
    try:
        probe = parse_probe(args.probe_bytes)
    except ValueError as exc:
        print(f"--probe-bytes 无效：{exc}", file=sys.stderr)
        return 2

    # segments：一轮循环连续拉多段，放大单轮搬运量；整档下载本身就是最大体积
    segments = max(1, min(args.segments, 64))
    if args.segments > 64:
        print(f"--segments 收敛到 64（单轮再多也没意义，起新连接更划算）。",
              file=sys.stderr)
    if probe == FULL_BODY and segments > 1:
        print("整档下载时 --segments 不生效（已按 1 处理）：单请求已是文件全量。",
              file=sys.stderr)
        segments = 1

    # 段数不能超过文件长度，否则后几段会返回 416；先探体积再收敛
    if segments > 1:
        room: list[int] = []
        for t in targets:
            sz = _object_size(t.label, args.timeout)
            if sz > 0:
                room.append(max(1, (sz + probe - 1) // probe))
        if room and min(room) < segments:
            capped = min(room)
            print(f"目标最小只有约 {min(room) * probe / 1e6:.1f} MB，可切 {capped} 段，"
                  f"--segments 由 {segments} 收敛到 {capped}（再多会 416）。", file=sys.stderr)
            segments = capped

    # 引擎选择：并发 >500 时线程模型自身会成为瓶颈，自动切 asyncio
    engine = args.engine
    if engine == "auto":
        engine = "asyncio" if args.concurrency > 500 else "threads"
    if _PROXY and engine == "asyncio":
        print("代理隧道目前只在 threads 引擎实现，已切到 threads。"
              "（走代理时瓶颈通常是代理本身，高并发也没有意义）", file=sys.stderr)
        engine = "threads"
    cap = ASYNC_MAX if engine == "asyncio" else THREADS_MAX
    n = max(1, min(args.concurrency, cap))
    if args.concurrency > cap:
        print(f"{engine} 引擎上限 {cap}，已收敛到 {cap}。", file=sys.stderr)
    if engine == "threads" and n > 800:
        print(f"提示：{n} 个线程时客户端自身（GIL/线程调度）可能先成为瓶颈，"
              f"建议加 --engine asyncio。", file=sys.stderr)
    if n > SAFE_CONCURRENCY and not args.authorized:
        print(f"并发 > {SAFE_CONCURRENCY} 需要显式声明授权：加 --authorized\n"
              f"（仅在你拥有该站点、或持有站方书面授权时使用）", file=sys.stderr)
        return 3

    # duration <= 0 表示无限运行：deadline 设为无穷，只有 Ctrl+C / 熔断能结束
    infinite = args.duration <= 0
    total_seconds = float("inf") if infinite else args.ramp_up + args.duration
    limit = None if probe == FULL_BODY else probe      # None = 整档下载

    # 熔断默认关闭：只有显式 --fuse 或传了阈值参数才启用
    fuse_on = bool(args.fuse or args.max_error_rate is not None
                   or args.abort_on is not None)
    max_err = args.max_error_rate if args.max_error_rate is not None else 0.05
    abort_raw = args.abort_on if args.abort_on is not None else "429,503"
    abort_codes = {int(x) for x in abort_raw.split(",") if x.strip().isdigit()}

    plan = {
        "targets": [t.label for t in targets],
        "engine": engine,
        "concurrency": n,
        "ramp_up_s": args.ramp_up,
        "duration_s": "unlimited" if infinite else args.duration,
        "total_s": None if infinite else round(total_seconds, 1),
        "fuse": "on" if fuse_on else "off",
        "max_error_rate": max_err if fuse_on else None,
        "abort_on": sorted(abort_codes) if fuse_on else [],
        "probe_bytes": "full" if probe == FULL_BODY else probe,
        "segments": segments,
        "proxy": f"{_PROXY[0]}:{_PROXY[1]}" if _PROXY else None,
    }

    if args.dry_run:
        print(json.dumps(plan, ensure_ascii=False, indent=2))
        return 0

    if not args.json:
        print(BAR)
        if len(targets) == 1:
            print(f" 目标   {targets[0].label}")
        else:
            print(f" 目标   {targets[0].label}")
            print(f"        等 {len(targets)} 个目标轮换（分散单对象限流，搬更多数据）")
        print(f" 并发   {n} 个虚拟用户 · 引擎 {engine}（{args.ramp_up:.0f}s 内阶梯爬升）")
        if _PROXY:
            print(f" 代理   {_PROXY[0]}:{_PROXY[1]}（CONNECT 隧道；瓶颈通常是代理而非站点）")
        if infinite:
            print(f" 时长   无限运行（爬坡 {args.ramp_up:.0f}s 后一直跑，Ctrl+C 停止并输出汇总）")
        else:
            print(f" 时长   稳态 {args.duration:.0f}s，总计约 {total_seconds:.0f}s")
        if fuse_on:
            print(f" 熔断   开 · 错误率 > {max_err:.0%} 或命中 {sorted(abort_codes)} 即停")
        else:
            print(" 熔断   关 · 跑满时长，不因报错/限流提前停止（结果需自行判读）")
        if args.max_egress_mb:
            print(f" 流量   累计超过 {args.max_egress_mb:,.0f} MB 自动停止")
        print(f" 探测   {probe_text(probe)}")
        if segments > 1:
            print(f" 搬运   每轮连续 {segments} 段（单轮 {probe * segments / 1e6:.1f} MB，"
                  f"单轮请求数 {segments}）")
        print(" 责任   已声明授权（--authorized）；仅限自有或书面授权站点")
        print(BAR)
        print("\n[0/3] 采集延迟基线…", end="", flush=True)

    try:
        base, body_bytes = baseline_latency(targets, args.timeout, limit)
    except Exception as exc:                           # noqa: BLE001
        print(f"\n基线探测失败：{exc}", file=sys.stderr)
        return 2

    if not args.json:
        print(f" {base * 1000:.0f} ms（单请求回包 {body_bytes}B，"
              f"约 {body_bytes / 1024:.0f} KB）")
        if probe == FULL_BODY:
            # 粗估：QPS ≈ 并发 / 单请求延迟
            per_s = body_bytes * segments * (n / base if base else 0.0)
            if infinite:
                print(f"       注意：整档下载 + 本档并发，预估出流量约 "
                      f"{per_s * 60 / 1e6:,.0f} MB/分钟；无限运行请留意流量费，Ctrl+C 随时可停。")
            else:
                est_mb = per_s * total_seconds / 1e6
                if est_mb >= 1024:
                    print(f"       注意：整档下载 + 本档并发，预估出流量约 {est_mb:,.0f} MB"
                          f"（{est_mb / 1024:,.1f} GB），注意对象存储流量费。")

    stats = Stats()
    stats.started = time.monotonic()
    run = Run(targets=targets, n=n, ramp_up=args.ramp_up,
              deadline=stats.started + total_seconds, stats=stats,
              timeout=args.timeout, limit=limit, fuse_on=fuse_on,
              max_err=max_err, abort_codes=abort_codes,
              live=sys.stdout.isatty() and not args.json,
              max_egress_mb=args.max_egress_mb,
              egress_cap=int(args.max_egress_mb * 1e6) if args.max_egress_mb else 0,
              segments=segments,
              seg_bytes=probe or body_bytes)

    if not args.json:
        tail = "无限时长，Ctrl+C 停止" if infinite else f"稳态 {args.duration:.0f}s"
        print(f"[1/3] 加压中（{n} 并发，爬坡 {args.ramp_up:.0f}s + {tail}）…")

    runner = run_asyncio if engine == "asyncio" else run_threads
    elapsed = runner(run)

    if run.live:
        print(" " * 70, end="\r")

    total, errs, statuses = stats.snapshot()
    qps = total / elapsed if elapsed else 0.0
    sqps = steady_qps(stats, args.ramp_up, elapsed)      # 去掉爬坡期的稳态吞吐
    (p50, p95, p99), max_ms = percentiles(stats.lat, (0.50, 0.95, 0.99))
    p50, p95, p99, max_ms = (round(x, 1) for x in (p50, p95, p99, max_ms))
    base_ms = round(base * 1000, 1)
    ratio = p95 / (base_ms or 1.0)
    hold = total > 0 and errs == 0 and all(s in (200, 206) for s in statuses)
    bad_codes = {s: c for s, c in statuses.items() if s not in (200, 206, -1)}
    err_rate = round(errs / total, 4) if total else 0.0

    if stats.user_stop:
        verdict = (f"运行 {elapsed:.1f}s 后被手动中断（Ctrl+C）：累计 {total:,} 请求，"
                   f"平均 {qps:,.0f} req/s，错误率 {err_rate:.1%}")
    elif stats.abort.is_set():
        if stats.abort_reason.startswith("出流量"):
            verdict = (f"{stats.abort_reason} → 已主动停止；累计 {total:,} 请求，"
                       f"平均 {qps:,.0f} req/s")
        else:
            verdict = f"已熔断（{stats.abort_reason}）→ 容量边界在 {n} 并发以下"
    elif hold and ratio <= 1.5:
        verdict = f"{n} 并发稳定通过，p95 为基线 {ratio:.1f}x → 余量充足"
    elif hold and ratio <= 3:
        verdict = f"{n} 并发稳定通过，但 p95 已放大到基线 {ratio:.1f}x → 开始劣化"
    elif hold:
        verdict = f"{n} 并发未报错，但 p95 放大 {ratio:.1f}x → 接近饱和"
    elif errs == 0 and bad_codes:
        verdict = (f"{n} 并发收到非 2xx 响应 {bad_codes}"
                   f" → 目标拒绝服务（限流/路径不存在），非延迟问题")
    else:
        verdict = (f"{n} 并发出现连接级失败 {errs} 次"
                   f"（错误率 {err_rate:.1%}）→ 已超容量")

    mbps = (stats.bytes * 8 / elapsed / 1e6) if elapsed else 0.0
    mbs = (stats.bytes / elapsed / 1e6) if elapsed else 0.0

    result = {
        "plan": plan,
        "elapsed_s": round(elapsed, 1),
        "requests": total,
        "achieved_qps": round(qps, 1),
        "steady_qps": round(sqps, 1),
        "status": {str(k): v for k, v in sorted(statuses.items())},
        "errors": stats.errors,
        "error_rate": err_rate,
        "latency_ms": {"baseline": base_ms, "p50": p50, "p95": p95,
                       "p99": p99, "max": max_ms},
        "egress_mb": round(stats.bytes / 1e6, 2),
        "throughput_mbps": round(mbps, 1),
        "held": hold,
        "aborted": stats.abort.is_set(),
        "abort_reason": stats.abort_reason,
        "verdict": verdict,
    }

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    print_timeline(stats, TIMELINE_ROWS if infinite else 0)
    print(f"\n{BAR}")
    print(" 汇总")
    print(BAR)
    head = targets[0].label if len(targets) == 1 else f"{targets[0].label} 等 {len(targets)} 个"
    print(f"  目标               {head}")
    print(f"  并发 / 实际时长      {n} / {elapsed:.1f}s")
    print(f"  完成请求           {total:,}")
    steady_txt = f"{sqps:,.0f}" if sqps else "—（稳态区间不足 3 秒）"
    print(f"  QPS 整体 / 稳态      {qps:,.0f} / {steady_txt}")
    print(f"  状态码分布           {result['status']}")
    print(f"  错误               {err_rate:.2%}  {stats.errors or ''}")
    print(f"  延迟 基线/p50/p95/p99  {base_ms:.0f} / {p50:.0f} / {p95:.0f} / {p99:.0f} ms"
          f"（最大 {max_ms:.0f}）")
    print(f"  本次测试出流量       {result['egress_mb']} MB"
          f"（{mbs:.2f} MB/s · {mbps:.1f} Mbit/s）")
    print(f"\n  判定：{verdict}")
    if stats.port_exhausted:
        print(BAR)
        print(" 诊断：出现了 WinError 10048/10055（本机临时端口/缓冲区耗尽）。")
        print("       这是客户端瓶颈，不是站点限流——再加大并发只会得到更多超时。")
        print("       缓解：降低并发、用 --engine asyncio 复用连接、或从带宽更大的一侧发起。")
    print(BAR)
    print(" 提示：容量边界应通过「分级加压」逐步逼近，单次跳到高并发只会得到崩溃结论。")
    print(BAR)
    return 0


if __name__ == "__main__":
    try:
        # 行缓冲：无限运行时被重定向/管道也要能实时看到输出，Ctrl+C 时不丢汇总
        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)  # type: ignore[attr-defined]
    except Exception:                                  # noqa: BLE001
        pass
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:                          # Ctrl+C 落在其它阶段时也别抛栈
        print("\n已中断。")
        raise SystemExit(130) from None
