# http-capacity-kit

![self-check](https://img.shields.io/badge/self--check-offline%20%C2%B7%207%20assertions-blue)

Two zero-dependency Python scripts that answer one question: **how much can my site take, and which side is the bottleneck?**

| Script | Role | Intensity |
|---|---|---|
| `a2o_loadtest.py` | Measures under load: concurrency / QPS / latency percentiles / throughput / egress | Adjustable, up to 20,000 concurrent (asyncio engine) |
| `a2o_probe.py` | Low-intensity sampling + modeling: derives the concurrency and bandwidth needed for a target QPS | Hard-capped at 5 connections / 150 requests, not adjustable |

One pushes, one calculates. `a2o_probe.py` sends no pressure at all — it measures and models, which makes it a good first step before you load anything.

> Chinese README: [README.md](README.md)

## ⚠️ Scope of use

**Both tools are for sites you own or have written authorization to test.**

Load-testing a third-party site is not "testing" — in most jurisdictions it is an attack on a computer information system. `a2o_loadtest.py` requires an explicit `--authorized` flag above 5 concurrent virtual users, but that flag is a declaration, not a legal shield.

If you just want to know whether some public site holds up, use the browser Network panel, `curl -w`, or ask the operator.

## Requirements

- Python 3.10+ (uses `X | None` annotations)
- No third-party packages — standard library only (`http.client`, `asyncio`, `ssl`)

```bash
git clone https://github.com/walker672/http-capacity-kit.git
cd http-capacity-kit
python a2o_loadtest.py --help
```

## Quick start

```bash
# 1. Build expectations first: measure latency, model capacity (sends no pressure)
python a2o_probe.py --url https://example.com/assets/app.js

# 2. Confirm real behavior at low concurrency
python a2o_loadtest.py --url https://example.com/ --concurrency 5 --duration 10

# 3. Step up in tiers to find the knee (100 -> 300 -> 600 -> 1000, one run each)
python a2o_loadtest.py --url https://example.com/assets/app.js \
    --concurrency 200 --duration 60 --ramp-up 20 --authorized
```

## a2o_loadtest.py

### Key options

| Option | Meaning |
|---|---|
| `--url` | Target. Comma-separate several and they rotate (spreads single-object rate limits) |
| `--url-file` | URL list, one per line, `#` for comments |
| `--concurrency` | Virtual users. threads: 1..2000, asyncio: 1..20000; switches to asyncio above 500 |
| `--duration` | Steady-state seconds; `0` = run until Ctrl+C, then print the summary |
| `--ramp-up` | Staggered start over N seconds, so the knee shows up at a concurrency level |
| `--probe-bytes` | Bytes read per request: `100` (default) / `64k` / `1m` / `full` (whole file) |
| `--segments` | N sequential range requests per loop — N× data per round |
| `--max-egress-mb` | Stop once cumulative egress exceeds N MB (protects against surprise bills) |
| `--timeout` | Per-request timeout. Lower it to 5 when bandwidth is tight |
| `--fuse` | Auto-abort on error-rate threshold or `--abort-on` status codes (off by default) |
| `--json` | Machine-readable output |
| `--dry-run` | Print the plan, send nothing |

### Scan first, then load

A site's HTML is usually just a shell; the real bulk (images, video, audio) lives inside `app.js`, CSS, and `<img>` tags. `--scan` digs them out and weighs them:

```bash
python a2o_loadtest.py --scan --url https://example.com/ --min-size 1m --out urls.txt
```

Pipeline: fetch the entry HTML → extract `src` / `href` / `poster` / `srcset` / `url()` / absolute URLs → follow JS and CSS (one level by default) and extract again → `HEAD` or `Range 0-0` each candidate for its size → sort by size and write the list.

- Sends a few dozen ordinary GETs. It is not load testing and needs no `--authorized`
- Falls back to any-host scanning when nothing is found on the same host (CDNs and object storage often live on another domain)
- `--max-items` caps the list, `--scan-follow 0` skips JS/CSS

### Moving large volumes

```bash
# Whole-file: response equals the real file, closest to real user behavior and real egress cost
python a2o_loadtest.py --url-file urls.txt --probe-bytes full \
    --concurrency 100 --duration 60 --authorized

# Segmented: fixed bytes per request plus N segments per round
python a2o_loadtest.py --url-file urls.txt --probe-bytes 1m --segments 4 \
    --concurrency 200 --duration 0 --authorized --max-egress-mb 2000
```

`--segments` probes the object size first and reduces the segment count automatically, so trailing requests do not hit 416.

### Output

The console report is in Chinese; pass `--json` when you need stable machine-readable keys.

```
 汇总
 目标               https://example.com/assets/app.js
 并发 / 实际时长      200 / 80.2s
 完成请求 / 实际 QPS  41,882 / 522 req/s
 状态码分布           {200: 41882}
 错误               0.00%
 延迟 基线/p50/p95/p99  85 / 96 / 210 / 640 ms
 本次测试出流量       287.4 MB（3.58 MB/s · 28.7 Mbit/s）

 判定：200 并发未报错，但 p95 放大 2.5x → 接近饱和
```

The per-second timeline is for finding the **degradation knee**, not the average — capacity problems always show up first in p99 and in success-rate jitter.

## a2o_probe.py

```bash
python a2o_probe.py --url https://example.com/assets/app.js
python a2o_probe.py --url https://example.com/ --baseline-only   # latency only
python a2o_probe.py --model-only                                 # no network, model only
```

Measures two TTFB figures — cold (full TCP + TLS handshake) and warm (keep-alive reuse) — then applies Little's law (`concurrency ≈ QPS × latency`) to model capacity, plus a header check: rate-limit responses, edge caching, compression, framing protection, HSTS, range support, long-lived caching.

The ceiling of 5 connections / 150 requests is hard-coded, with no `--qps` or `--duration` flood parameters. That is deliberate: if you can model it, you do not need to hit it.

## Self-check

```bash
python selfcheck.py
```

Seven assertions, fully offline, **no request is sent** and `--authorized` is not required: byte-compile, both `--help` screens, the `--dry-run` plan fields, malformed `--probe-bytes` must exit 2, 500 concurrency without `--authorized` must exit 3, and `--model-only` must produce usable output.

```
[PASS] byte-compile  a2o_loadtest.py a2o_probe.py
[PASS] --help a2o_loadtest.py
[PASS] --help a2o_probe.py
[PASS] dry-run plan  probe=100B engine=threads segments=1 fuse=off
[PASS] malformed --probe-bytes exits 2  exit=2
[PASS] unauthorized high concurrency exits 3  exit=3
[PASS] model-only output  asset_bytes=400,000

all passed
```

Run it after touching either script to confirm the safety boundaries are intact. `.github/workflows/ci.yml` calls the same script, so local and CI results match. (The console output itself is Chinese.)

## FAQ

**Q: 1000 concurrency is slower than 200 and full of TimeoutError — is the site rate-limiting me?**
Check the client first. Consumer broadband, the ~16k ephemeral port range on Windows, and NAT session tables each saturate before the server does. Watch the egress MB/s figure: if it is already near your local uplink, the bottleneck is on your side and more concurrency only buys more timeouts.

**Q: Why tier the concurrency instead of jumping to 1000?**
Jumping straight to 1000 only tells you "it broke". It does not tell you where it started degrading. Runs at 100 / 300 / 600 / 1000 produce a knee you can act on.

**Q: `--segments` returns 416?**
Segments × bytes per segment exceeded the object size. The script probes and reduces automatically, but when targets differ a lot in size, lower `--segments` or `--probe-bytes` yourself.

**Q: Does memory grow during an unlimited run?**
No. The per-second window keeps the last 900 seconds and latency samples are capped at 2M entries (`array('f')`, ~8 MB), then dropped.

**Q: Is the egress cap accurate?**
Yes. Quota is reserved before each request, so in-flight requests cannot overshoot together. Measured: 8 concurrent × 1 MB segments against a 20 MB cap stopped at 19.92 MB.

## License

MIT. See [LICENSE](LICENSE).
