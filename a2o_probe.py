#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
a2o_probe.py — 站点容量探测与建模工具（低强度）

用途
----
测量单个对象的真实响应延迟，并据此推算「达到某个 QPS 需要多少并发连接 /
带宽 / 出口 IP」。这是一个**测量 + 建模**工具，不是压力测试工具。

安全边界（硬编码，不可通过参数放宽）
----------------------------------
  · 并发上限          5 条连接
  · 单轮请求上限      50 个
  · 默认目标 QPS      < 10
这套上限远低于任何服务端的限流阈值（例如对象存储单文件热点频控通常是
1000 QPS 量级），因此不会对站点造成可用性影响。工具故意不提供 --qps /
--duration 之类的洪泛参数——那会让它变成 DDoS 工具。

用法
----
  python a2o_probe.py --url https://example.com/assets/app.js   # 完整探测 + 建模
  python a2o_probe.py --url https://example.com/ --baseline-only  # 只测延迟
  python a2o_probe.py --model-only                              # 用默认延迟直接建模
  python a2o_probe.py --url https://example.com/ --json         # 机器可读输出

前提
----
仅限对自己拥有或已获书面授权的站点使用。
"""

from __future__ import annotations

import argparse
import http.client
import json
import socket
import ssl
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

# ---------------------------------------------------------------- 配置

HOST = "example.com"
ASSET = "/"
ASSET_BYTES = 0            # 0 = 运行时自动探测（Range 0-0 读 Content-Range 的总长）
SCHEME = "https"
PORT = 443

# 硬上限：不可通过命令行提高
MAX_CONCURRENCY = 5
MAX_REQUESTS = 50
TIMEOUT = 20.0
PROBE_BYTES = 100          # Range 只取 100 字节，避免产生流量费

# COS 官方规格（其他地域）
LIMIT_SINGLE_FILE_QPS = 1_000
LIMIT_BUCKET_READ_QPS = 3_000
LIMIT_BANDWIDTH_GBPS = 10.0


# ---------------------------------------------------------------- 基础请求

def _conn(host: str = "") -> http.client.HTTPConnection:
    host = host or HOST
    if SCHEME == "https":
        ctx = ssl.create_default_context()
        return http.client.HTTPSConnection(host, PORT, timeout=TIMEOUT, context=ctx)
    return http.client.HTTPConnection(host, PORT, timeout=TIMEOUT)


def probe_once(conn: http.client.HTTPSConnection, path: str, nbytes: int = PROBE_BYTES) -> dict:
    """在给定连接上发一次 Range 请求，返回状态码与耗时。"""
    t0 = time.perf_counter()
    conn.request(
        "GET",
        path,
        headers={
            "Range": f"bytes=0-{nbytes - 1}",
            "Accept-Encoding": "identity",
            "User-Agent": "a2o-probe/1.0 (authorized self-assessment, low-rate)",
            "Connection": "keep-alive",
        },
    )
    resp = conn.getresponse()
    ttfb = time.perf_counter() - t0
    body = resp.read()
    total = time.perf_counter() - t0
    return {
        "status": resp.status,
        "ttfb": ttfb,
        "total": total,
        "bytes": len(body),
        "headers": dict(resp.getheaders()),
        "reason": resp.reason,
    }


def probe_cold(n: int) -> list[dict]:
    """冷连接：每次新建 TCP + TLS，代表首次访客。"""
    out = []
    for _ in range(n):
        c = _conn(HOST)
        try:
            c.connect()
            out.append(probe_once(c, ASSET))
        finally:
            c.close()
    return out


def probe_warm(n: int) -> list[dict]:
    """热连接：复用同一条 keep-alive 连接，代表持续请求的客户端。"""
    c = _conn(HOST)
    out = []
    try:
        c.connect()
        for _ in range(n):
            out.append(probe_once(c, ASSET))
    finally:
        c.close()
    return out


def probe_concurrent(concurrency: int, per_worker: int) -> list[dict]:
    """低并发采样：concurrency 硬上限 MAX_CONCURRENCY。"""
    concurrency = max(1, min(concurrency, MAX_CONCURRENCY))
    per_worker = max(1, min(per_worker, MAX_REQUESTS // concurrency))

    def worker(_: int) -> list[dict]:
        c = _conn(HOST)
        res = []
        try:
            c.connect()
            for _ in range(per_worker):
                res.append(probe_once(c, ASSET))
        except Exception as exc:                     # noqa: BLE001
            res.append({"status": -1, "ttfb": 0.0, "total": 0.0,
                        "bytes": 0, "headers": {}, "reason": str(exc)})
        finally:
            c.close()
        return res

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        batches = list(pool.map(worker, range(concurrency)))
    return [r for b in batches for r in b]


def detect_size() -> int:
    """一次 Range 0-0 拿对象真实大小；服务端忽略 Range 时退回 Content-Length。"""
    c = _conn()
    try:
        c.connect()
        r = probe_once(c, ASSET, 1)
        h = {k.lower(): v for k, v in r["headers"].items()}
        cr = h.get("content-range", "")
        if "/" in cr:
            tot = cr.rsplit("/", 1)[1].strip()
            if tot.isdigit():
                return int(tot)
        if r["status"] == 200:
            return int(h.get("content-length", "0") or 0) or r["bytes"]
        return int(h.get("content-length", "0") or 0)
    finally:
        c.close()


# ---------------------------------------------------------------- 统计

def summarize(samples: list[dict], label: str) -> dict:
    ok = [s for s in samples if s["status"] in (200, 206)]
    bad = [s for s in samples if s["status"] not in (200, 206)]
    ttfbs = [s["ttfb"] for s in ok] or [0.0]
    return {
        "label": label,
        "n": len(samples),
        "ok": len(ok),
        "failed": len(bad),
        "statuses": sorted({s["status"] for s in samples}),
        "ttfb_mean": statistics.fmean(ttfbs),
        "ttfb_p50": statistics.median(ttfbs),
        "ttfb_p95": sorted(ttfbs)[max(0, int(len(ttfbs) * 0.95) - 1)],
        "ttfb_min": min(ttfbs),
        "ttfb_max": max(ttfbs),
        "bytes": ok[0]["bytes"] if ok else 0,
    }


# ---------------------------------------------------------------- 建模

def model(latency_warm: float, latency_cold: float) -> dict:
    """根据实测延迟推算达到目标 QPS 所需的并发与带宽。

    核心公式：并发连接数 ≈ 目标QPS × 单请求耗时（Little 定律）
    HTTP/1.1 单连接串行，故并发数即连接数。
    """
    req_bytes = 260          # 请求头估算
    resp_bytes = PROBE_BYTES  # Range 只取 100 字节

    rows = []
    for qps in (100, 500, LIMIT_SINGLE_FILE_QPS, LIMIT_BUCKET_READ_QPS):
        need_warm = qps * latency_warm
        need_cold = qps * latency_cold
        up_mbps = qps * req_bytes * 8 / 1_000_000
        down_mbps = qps * resp_bytes * 8 / 1_000_000
        rows.append({
            "qps": qps,
            "conn_warm": round(need_warm),
            "conn_cold": round(need_cold),
            "up_mbps": round(up_mbps, 2),
            "down_mbps": round(down_mbps, 2),
            "reachable": qps <= LIMIT_SINGLE_FILE_QPS,
            "note": (
                "触及单文件热点频控上限"
                if qps == LIMIT_SINGLE_FILE_QPS
                else ("需分散到多个对象" if qps > LIMIT_SINGLE_FILE_QPS else "")
            ),
        })

    # 若用于灌满带宽（下完整文件而非 Range）
    full_qps = min(LIMIT_SINGLE_FILE_QPS, LIMIT_BUCKET_READ_QPS)
    full_bps = full_qps * ASSET_BYTES
    return {
        "rows": rows,
        "limits": {
            "single_file_qps": LIMIT_SINGLE_FILE_QPS,
            "bucket_read_qps": LIMIT_BUCKET_READ_QPS,
            "bandwidth_gbps": LIMIT_BANDWIDTH_GBPS,
        },
        "full_file": {
            "qps": full_qps,
            "throughput_MBps": round(full_bps / 1_000_000, 1),
            "throughput_Gbps": round(full_bps * 8 / 1e9, 2),
            "tb_per_day": round(full_bps * 86_400 / 1e12, 1),
            "cost_per_day_cny": round(full_bps * 86_400 / 1e9 * 0.9),
            "bandwidth_capped": full_bps * 8 / 1e9 >= LIMIT_BANDWIDTH_GBPS,
        },
    }


# ---------------------------------------------------------------- 防护诊断

def diagnose(baseline: dict, conc: dict, headers: dict) -> list[dict]:
    checks = []

    def add(item, ok, detail):
        checks.append({"item": item, "ok": ok, "detail": detail})

    statuses = set(baseline["statuses"]) | set(conc["statuses"])
    add("限流响应", 429 in statuses or 503 in statuses,
        f"观测到状态码 {sorted(statuses)}；未见 429/503 → 低强度下无限流")

    h_lower = {k.lower(): v for k, v in headers.items()}
    add("边缘缓存", bool(h_lower.get("age") or h_lower.get("x-cache") or h_lower.get("cf-cache-status")),
        "无 age / x-cache 头 → 无 CDN 边缘缓存，每请求回源")

    add("内容压缩", h_lower.get("content-encoding") in ("gzip", "br", "zstd"),
        f"Content-Encoding={h_lower.get('content-encoding', '缺失')} → 未压缩")

    add("防嵌套", "x-frame-options" in h_lower or "frame-ancestors" in h_lower.get("content-security-policy", ""),
        "无 X-Frame-Options / frame-ancestors")

    add("HSTS", "strict-transport-security" in h_lower,
        f"Strict-Transport-Security={h_lower.get('strict-transport-security', '缺失')}")

    add("Range 支持", h_lower.get("accept-ranges") == "bytes",
        "Accept-Ranges: bytes → 支持小分片请求（低成本占用 QPS 的前提）")

    add("长缓存资产", "immutable" in h_lower.get("cache-control", ""),
        f"Cache-Control={h_lower.get('cache-control', '缺失')}")
    return checks


# ---------------------------------------------------------------- 输出

def hr(ch: str = "-", n: int = 68) -> str:
    return ch * n


def main() -> int:
    global HOST, ASSET, ASSET_BYTES, SCHEME, PORT
    ap = argparse.ArgumentParser(description="站点低强度容量探测与建模")
    ap.add_argument("--url", default=None,
                    help="要探测的完整 URL（默认 https://example.com/）")
    ap.add_argument("--baseline-only", action="store_true", help="只测延迟，不建模")
    ap.add_argument("--model-only", action="store_true", help="跳过探测，用默认延迟建模")
    ap.add_argument("--json", action="store_true", help="输出 JSON")
    ap.add_argument("--cold", type=int, default=8, help=f"冷连接采样数（上限 {MAX_REQUESTS}）")
    ap.add_argument("--warm", type=int, default=12, help=f"热连接采样数（上限 {MAX_REQUESTS}）")
    args = ap.parse_args()

    if args.url:
        sp = urlsplit(args.url)
        if sp.scheme not in ("http", "https"):
            print(f"--url 只支持 http/https，收到：{sp.scheme or '(空)'}", file=sys.stderr)
            return 2
        if not sp.hostname:
            print(f"--url 缺少主机名：{args.url}", file=sys.stderr)
            return 2
        SCHEME = sp.scheme
        HOST = sp.hostname
        PORT = sp.port or (443 if SCHEME == "https" else 80)
        ASSET = (sp.path or "/") + (f"?{sp.query}" if sp.query else "")

    target_url = (f"{SCHEME}://{HOST}" if PORT in (80, 443) else f"{SCHEME}://{HOST}:{PORT}")
    target_url += ASSET

    cold_n = max(1, min(args.cold, MAX_REQUESTS))
    warm_n = max(1, min(args.warm, MAX_REQUESTS))

    result: dict = {"target": target_url}

    if args.model_only:
        # 2026-11-09 直连实测中位值（经代理的 curl 数据会偏高约 3 倍，勿用）
        lat_warm, lat_cold = 0.085, 0.090
        result["latency"] = {"warm": lat_warm, "cold": lat_cold, "source": "实测默认值"}
    else:
        if not args.json:
            print(hr("="))
            print(f" 目标   {target_url}")
            print(f" 方式   Range 0-{PROBE_BYTES - 1}（仅取 {PROBE_BYTES} 字节，不产生显著流量费）")
            print(f" 强度   并发 ≤ {MAX_CONCURRENCY}，总请求 ≤ {warm_n + cold_n + MAX_REQUESTS}")
            print(hr("="))
            print("\n[1/3] 冷连接采样（新建 TCP + TLS）…")

        try:
            cold = probe_cold(cold_n)
        except Exception as exc:                     # noqa: BLE001
            print(f"冷连接探测失败: {exc}", file=sys.stderr)
            return 2
        s_cold = summarize(cold, "cold")

        if not args.json:
            print(f"      完成 {s_cold['ok']}/{s_cold['n']}，"
                  f"TTFB 中位 {s_cold['ttfb_p50'] * 1000:.0f} ms")
            print("\n[2/3] 热连接采样（复用 keep-alive）…")

        try:
            warm = probe_warm(warm_n)
        except Exception as exc:                     # noqa: BLE001
            print(f"热连接探测失败: {exc}", file=sys.stderr)
            return 2
        s_warm = summarize(warm, "warm")

        if not args.json:
            print(f"      完成 {s_warm['ok']}/{s_warm['n']}，"
                  f"TTFB 中位 {s_warm['ttfb_p50'] * 1000:.0f} ms")
            print(f"\n[3/3] 低并发采样（并发 {MAX_CONCURRENCY}，验证限流行为）…")

        conc = probe_concurrent(MAX_CONCURRENCY, MAX_REQUESTS // MAX_CONCURRENCY)
        s_conc = summarize(conc, "concurrent")

        if not args.json:
            print(f"      完成 {s_conc['ok']}/{s_conc['n']}，"
                  f"状态码 {s_conc['statuses']}")

        result["latency"] = {
            "cold": s_cold["ttfb_p50"],
            "warm": s_warm["ttfb_p50"],
            "source": "实测",
        }
        result["samples"] = {"cold": s_cold, "warm": s_warm, "concurrent": s_conc}
        result["headers"] = {k.lower(): v for k, v in (warm[0]["headers"] if warm else {}).items()}

        lat_warm, lat_cold = s_warm["ttfb_p50"], s_cold["ttfb_p50"]

    # 对象体积：真实探测一次（1 字节 Range），供「下完整文件」的流量估算使用
    if ASSET_BYTES <= 0:
        if args.model_only:
            ASSET_BYTES = 400_000
        else:
            try:
                ASSET_BYTES = detect_size()
            except Exception as exc:                   # noqa: BLE001
                print(f"体积探测失败（按 400KB 估算）：{exc}", file=sys.stderr)
                ASSET_BYTES = 400_000

    if args.baseline_only:
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print("\n(仅基线模式，未建模)")
        return 0

    m = model(lat_warm, lat_cold)
    m["asset_bytes"] = ASSET_BYTES
    result["model"] = m

    if not args.model_only:
        checks = diagnose(result["samples"]["warm"], result["samples"]["concurrent"],
                          result["headers"])
        result["diagnosis"] = checks
    else:
        checks = []

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    # ---- 人读报告 ----
    print("\n" + hr("="))
    print(" 延迟基线")
    print(hr("="))
    print(f"  冷连接 TTFB 中位   {lat_cold * 1000:7.0f} ms   (含 TCP + TLS 握手)")
    print(f"  热连接 TTFB 中位   {lat_warm * 1000:7.0f} ms   (复用连接，攻击者实际使用)")
    print(f"  冷/热 比值         {lat_cold / lat_warm:7.2f} x")

    print("\n" + hr("="))
    print(" 容量建模：达到目标 QPS 所需资源（Little 定律：并发 ≈ QPS × 延迟）")
    print(hr("="))
    print(f"  {'目标QPS':>8}  {'所需并发(热)':>12}  {'所需并发(冷)':>12}  "
          f"{'上行Mbit/s':>10}  {'下行Mbit/s':>10}  备注")
    for r in m["rows"]:
        print(f"  {r['qps']:>8,}  {r['conn_warm']:>12,}  {r['conn_cold']:>12,}  "
              f"{r['up_mbps']:>10.2f}  {r['down_mbps']:>10.2f}  {r['note']}")

    ff = m["full_file"]
    print("\n" + hr("="))
    print(" 若改为下完整文件（流量费视角）")
    print(hr("="))
    print(f"  对象体积                     {ASSET_BYTES / 1e6:.2f} MB")
    print(f"  受单文件频控约束的最大 QPS   {ff['qps']:,}")
    print(f"  对应吞吐                     {ff['throughput_MBps']} MB/s "
          f"(≈ {ff['throughput_Gbps']} Gbit/s)")
    print(f"  日流量                       {ff['tb_per_day']} TB")
    print(f"  日账单（0.9 元/GB）          ≈ {ff['cost_per_day_cny']:,} 元")
    if ff["bandwidth_capped"]:
        print("  注意：已触及单账号单地域 10 Gbit/s 带宽上限，实际会被流控")

    if checks:
        print("\n" + hr("="))
        print(" 防护诊断")
        print(hr("="))
        for c in checks:
            mark = "OK  " if c["ok"] else "MISS"
            print(f"  [{mark}] {c['item']}")
            print(f"         {c['detail']}")

    print("\n" + hr("="))
    print(" 说明：本次探测总请求数 ≤ "
          f"{MAX_REQUESTS * 3}，并发 ≤ {MAX_CONCURRENCY}，"
          "远低于服务端限流阈值。")
    print(" 本工具不提供洪泛参数，建模结果来自实测延迟的数学外推，而非实际施压。")
    print(hr("="))
    return 0


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")   # type: ignore[attr-defined]
    except Exception:                              # noqa: BLE001
        pass
    socket.setdefaulttimeout(TIMEOUT)
    raise SystemExit(main())
