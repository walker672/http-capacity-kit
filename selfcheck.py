#!/usr/bin/env python3
"""selfcheck.py — 离线自检（等价于 CI 的 6 条断言）

只做静态与计划层面的校验，**不发任何压测流量**，不需要 --authorized：

  1. 两个脚本都通过字节码编译
  2. `--help` 正常退出
  3. `--dry-run` 打印的计划字段符合预期（不改回默认值）
  4. 非法 `--probe-bytes` 必须以 exit 2 拒绝
  5. 500 并发不带 `--authorized` 必须以 exit 3 拒绝
  6. `a2o_probe.py --model-only` 的建模输出可用

用法：
  python selfcheck.py          # 全部跑一遍，全通过返回 0
  python selfcheck.py -v       # 额外打印每条命令的 stderr
"""

import json
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
PY = sys.executable
VERBOSE = "-v" in sys.argv
FAILED: list[str] = []


def run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run([PY, *args], cwd=HERE, capture_output=True,
                          text=True, encoding="utf-8", errors="replace")


def check(name: str, ok: bool, detail: str = "", proc=None) -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAILED.append(name)
        if proc is not None:
            err = (proc.stderr or "").strip().splitlines()
            out = (proc.stdout or "").strip().splitlines()
            for line in (err[-6:] or out[-6:]):
                print(f"       | {line}")
    elif VERBOSE and proc is not None:
        for line in (proc.stderr or "").strip().splitlines()[-3:]:
            print(f"       | {line}")


def main() -> int:
    print(f"python {sys.version.split()[0]}  ·  {HERE}\n")

    # 1 编译
    files = ["a2o_loadtest.py", "a2o_probe.py"]
    p = run(["-m", "py_compile", *files])
    check("字节码编译", p.returncode == 0, " ".join(files), p)

    # 2 帮助屏
    for f in files:
        p = run([f, "--help"])
        check(f"--help {f}", p.returncode == 0 and "usage" in p.stdout.lower(), "", p)

    # 3 dry-run 计划
    p = run(["a2o_loadtest.py", "--url", "https://example.com/", "--concurrency", "100",
             "--duration", "10", "--max-egress-mb", "100", "--authorized",
             "--dry-run", "--json"])
    plan = {}
    try:
        plan = json.loads(p.stdout)
    except Exception:                                  # noqa: BLE001
        pass
    ok = (p.returncode == 0 and plan.get("concurrency") == 100
          and plan.get("engine") == "threads" and plan.get("segments") == 1)
    check("dry-run 计划字段", ok,
          f"probe={plan.get('probe_bytes')}B engine={plan.get('engine')} "
          f"segments={plan.get('segments')} fuse={plan.get('fuse')}", p)

    # 4 非法体积参数
    p = run(["a2o_loadtest.py", "--url", "https://example.com/",
             "--probe-bytes", "wat", "--dry-run"])
    check("非法 --probe-bytes 返回 2", p.returncode == 2, f"exit={p.returncode}", p)

    # 5 未授权高并发
    p = run(["a2o_loadtest.py", "--url", "https://example.com/",
             "--concurrency", "500", "--dry-run"])
    check("未授权高并发返回 3", p.returncode == 3, f"exit={p.returncode}", p)

    # 6 建模（不联网）
    p = run(["a2o_probe.py", "--model-only", "--json"])
    size = 0
    try:
        size = json.loads(p.stdout)["model"]["asset_bytes"]
    except Exception:                                  # noqa: BLE001
        pass
    check("model-only 输出", p.returncode == 0 and size > 0,
          f"asset_bytes={size:,}" if size else "", p)

    # 清掉编译缓存
    for cache in HERE.rglob("__pycache__"):
        for f in cache.glob("*"):
            try:
                f.unlink()
            except OSError:
                pass
        try:
            cache.rmdir()
        except OSError:
            pass

    print()
    if FAILED:
        print(f"{len(FAILED)} 项失败：" + "、".join(FAILED))
        return 1
    print("全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
