# http-capacity-kit

[![ci](https://github.com/walker672/http-capacity-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/walker672/http-capacity-kit/actions/workflows/ci.yml)

两个零依赖的 Python 脚本，用来回答同一个问题：**我的站点到底能扛多少，瓶颈在哪一侧。**

> English: [README.en.md](README.en.md)

| 脚本 | 定位 | 强度 |
|---|---|---|
| `a2o_loadtest.py` | 加压测量：并发 / QPS / 延迟分位 / 吞吐 / 出流量 | 可调，最高 20000 并发（asyncio 引擎） |
| `a2o_probe.py` | 低强度采样 + 数学建模：用实测延迟反推达到目标 QPS 需要多少并发与带宽 | 硬编码 ≤ 5 并发、≤ 150 请求，不可放宽 |

一个负责「压出来」，一个负责「算出来」。`a2o_probe.py` 不发压力，纯粹测量与建模，适合先跑一遍建立预期。

## ⚠️ 使用边界

**这两个工具只允许用于你自己拥有、或已获得书面授权的站点。**

对第三方站点施压不是"测试"，在几乎所有司法辖区都属于对计算机信息系统的破坏行为。`a2o_loadtest.py` 在并发 > 5 时强制要求 `--authorized` 声明，这行代码不能替代你的法律责任。

如果你只是想验证某个公开站点"扛不扛得住"，请改用浏览器的 Network 面板、`curl -w`，或者直接问站方。

## 环境

- Python 3.10+（用到 `X | None` 类型语法）
- 零第三方依赖，标准库即可（`http.client` / `asyncio` / `ssl`）

```bash
git clone https://github.com/walker672/http-capacity-kit.git
cd http-capacity-kit
python a2o_loadtest.py --help
```

## 快速开始

```bash
# 1. 先建预期：实测延迟 + 反推容量（不发压力）
python a2o_probe.py --url https://example.com/assets/app.js

# 2. 低并发确认真实行为
python a2o_loadtest.py --url https://example.com/ --concurrency 5 --duration 10

# 3. 分级加压找拐点（100 → 300 → 600 → 1000，每档单独跑）
python a2o_loadtest.py --url https://example.com/assets/app.js \
    --concurrency 200 --duration 60 --ramp-up 20 --authorized
```

## a2o_loadtest.py

### 核心参数

| 参数 | 说明 |
|---|---|
| `--url` | 目标，逗号分隔多个则轮换请求（分散单对象限流） |
| `--url-file` | URL 清单，每行一个，`#` 开头为注释 |
| `--concurrency` | 虚拟用户数。threads 引擎 1..2000，asyncio 引擎 1..20000；> 500 自动切 asyncio |
| `--duration` | 稳态秒数；`0` = 无限运行，Ctrl+C 停止并输出汇总 |
| `--ramp-up` | 阶梯爬坡秒数，错峰启动，能看出拐点落在哪个并发区间 |
| `--probe-bytes` | 每请求读取字节数。`100`（默认）/ `64k` / `1m` / `full`（整档下载） |
| `--segments` | 一轮循环内连续请求 N 段（不同 Range 偏移），单轮搬运量 ×N |
| `--max-egress-mb` | 累计出流量上限，超过即停（长跑防账单失控） |
| `--timeout` | 单请求超时。带宽吃紧时建议 5，避免大量 TimeoutError 污染结论 |
| `--fuse` | 自动熔断（默认关闭）：错误率超阈值或命中 `--abort-on` 状态码即停 |
| `--json` | 机器可读输出 |
| `--dry-run` | 只打印计划，不发包 |

### 先扫描，再压测

站点首页通常只是壳，真正的大体积数据（图片 / 视频 / 音频）散在 `app.js`、CSS、`<img>` 里。`--scan` 把它们捞出来称重：

```bash
python a2o_loadtest.py --scan --url https://example.com/ --min-size 1m --out urls.txt
```

流程：抓入口 HTML → 正则抽取 `src` / `href` / `poster` / `srcset` / `url()` / 绝对 URL → 跟读 JS/CSS（默认 1 层）再抽一遍 → 逐个 `HEAD` 或 `Range 0-0` 称重 → 按体积降序写清单。

- 只发几十次普通 GET，不算压测，不需要 `--authorized`
- 同域扫不到资源时自动放宽到任意主机（CDN / 对象存储常与站点不同域）
- `--max-items` 控制清单长度，`--scan-follow 0` 只看首页不跟 JS

### 搬运大体积数据

```bash
# 整档下载：回包 = 文件实际大小，最接近真实用户行为，也最接近真实流量费
python a2o_loadtest.py --url-file urls.txt --probe-bytes full \
    --concurrency 100 --duration 60 --authorized

# 分段搬运：每请求固定字节 + 单轮多段，控制单请求成本
python a2o_loadtest.py --url-file urls.txt --probe-bytes 1m --segments 4 \
    --concurrency 200 --duration 0 --authorized --max-egress-mb 2000
```

`--segments` 会先探目标体积再收敛段数，避免尾部请求撞 416。

### 输出

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

逐秒时序表用来找**劣化拐点**，而不是只看平均值——容量问题总是先出现在 p99 和成功率的抖动上。

## a2o_probe.py

```bash
python a2o_probe.py --url https://example.com/assets/app.js
python a2o_probe.py --url https://example.com/ --baseline-only   # 只测延迟
python a2o_probe.py --model-only                                 # 不联网，用默认延迟建模
```

测冷连接（TCP + TLS 全握手）与热连接（keep-alive 复用）两套 TTFB，再用 Little 定律 `并发 ≈ QPS × 延迟` 反推容量，附带一份响应头体检：限流响应、边缘缓存、内容压缩、防嵌套、HSTS、Range 支持、长缓存。

硬上限 `5 并发 / 150 请求`，代码里写死，没有 `--qps` / `--duration` 这类洪泛参数。这是刻意的设计：能建模就不需要真压。

## 常见问题

**Q：并发拉到 1000 反而更慢、满是 TimeoutError，是站点限流了吗？**
先看客户端。个人宽带 100–500 Mbit/s、Windows 临时端口 16384 个、NAT 会话表，任何一项都会先于服务端饱和。测大流量时观察「出流量 MB/s」是否已贴近你本机带宽上限——如果是，瓶颈就在你这一侧，加压只会得到更多超时，不会得到更多信息。

**Q：为什么建议分级加压而不是一次跳到高并发？**
一次跳到 1000 并发只能得到"崩了"这个结论，得不到"从哪一档开始劣化"。100 / 300 / 600 / 1000 各跑一轮，拐点才有意义。

**Q：`--segments` 报 416？**
段数 × 每段字节超过了文件体积。脚本会自动探体积收敛，但如果文件在不同目标间大小差异大，手工调小 `--segments` 或 `--probe-bytes`。

**Q：无限运行时内存会涨吗？**
不会。逐秒时序窗口只保留最近 900 秒，延迟样本上限 200 万条（`array('f')`，约 8MB），超出即丢弃。

**Q：出流量封顶准吗？**
准。额度在每次请求发出前预留，并发在途请求不会一起冲过上限。实测 8 并发 × 1MB 段、上限 20MB，停在 19.92MB。

## License

MIT。见 [LICENSE](LICENSE)。
