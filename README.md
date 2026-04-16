# traffic-go

`traffic-go` 是一个面向 Linux VPS 的流量监控工具。它从 `/proc/net/nf_conntrack` 和 `/proc` 读取连接与 socket 元数据，把分钟级历史流量写入 SQLite，并通过内嵌 WebUI 和 HTTP API 提供回查能力。

它的目标不是做内核级抓包，而是在尽量少依赖、兼容老机器的前提下回答这些问题：

- 哪个进程访问了哪个远端 IP。
- 走的是哪个本地端口和协议。
- 入站、出站、forward/NAT 流量分别是多少。
- 一段时间后还能按分钟或按小时回查。

## 适用场景

- CentOS 7、旧内核 VPS、无法方便部署 eBPF 的环境。
- 代理机、转发机、轻量网关。
- 需要通过 WebUI 快速回看“某个进程/端口/远端 IP 在某时间段用了多少流量”的场景。

## 核心能力

- Linux-only，单个 Go 二进制运行。
- 不依赖 eBPF、Kafka、外置数据库。
- 入站 `in`、出站 `out`、转发 `forward` 分开统计。
- 分钟表 `usage_1m` 持久化，小时表 `usage_1h` 自动聚合。
- WebUI 和 JSON API 一起由同一进程提供。
- 支持按进程、PID、可执行文件、端口、远端 IP、方向、协议过滤。
- 超过分钟保留窗口后，自动切换到小时聚合查询。
- 非 Linux 或 `mock_data: true` 时自动使用 mock collector，方便前端开发。

## 工作原理

1. collector 按 `tick_interval` 读取 `/proc/net/nf_conntrack`。
2. 根据本机 IP 判断每条流是 `in`、`out` 还是 `forward`。
3. 对非 forward 流量，结合 `/proc/net/{tcp,udp}` 和 `/proc/[pid]/fd` 做 best-effort 进程归因。
4. 每个连接第一次出现时只建立 baseline，不把监控启动前的累计计数算进来。
5. 增量先累积到当前分钟，跨分钟时刷入 SQLite。
6. 后台任务每分钟补跑小时聚合、每小时清理过期数据、每 7 天执行一次 `VACUUM`。

这意味着：

- `active_connections` / `active_processes` 是运行态实时值。
- `overview` / `usage` / `timeseries` 的历史字节数依赖分钟落盘，通常要等到下一个分钟边界才稳定可见。

## 运行前提

至少需要满足这些内核条件：

- `/proc/net/nf_conntrack` 可读。
- `nf_conntrack` 模块已加载。
- `net.netfilter.nf_conntrack_acct=1` 已启用。

如果没有打开 `nf_conntrack_acct`，连接还能被看到，但 `bytes_*` 和 `pkts_*` 会一直是 `0`。

## 快速开始

### 本地开发

后端：

```bash
go test ./...
go run ./cmd/traffic-go -config deploy/config.example.yaml
```

前端：

```bash
npm --prefix web install
npm --prefix web run dev -- --host 127.0.0.1
```

Vite 开发服务器会把 `/api` 代理到 `deploy/config.example.yaml` 里的 `listen` 地址。
如果你想临时覆盖它，可以设置：

```bash
TRAFFIC_GO_DEV_PROXY=http://127.0.0.1:<your-port> npm --prefix web run dev -- --host 127.0.0.1
```

前端默认会请求真实后端 API，不会再自动切到浏览器内置 mock 数据。
如果你只想单独预览 UI，再显式开启前端 mock：

```bash
VITE_TRAFFICGO_USE_MOCK=1 npm --prefix web run dev -- --host 127.0.0.1
```

如果你在非 Linux 上启动后端，collector 会自动进入 mock 模式，便于开发 UI 和 API 调试。

### Makefile

```bash
make test-backend
make test-frontend
make build-frontend
make sync-frontend
make build
make run
make dev-web
```

其中：

- `make build` 会先构建前端，再把 `web/dist/` 同步到 `internal/embed/dist/`，最后编译 Go 二进制。
- 如果你直接用 `go run` 启动后端，想看到最新前端，需要先执行 `make sync-frontend`。

## 从 Windows 打 Linux 包

仓库内提供了面向 Windows Git Bash 的打包脚本：

```bash
bash deploy/build-linux-gitbash.sh
```

脚本会：

- 构建前端。
- 同步内嵌静态资源。
- 交叉编译 `linux/amd64` 二进制。
- 产出安装包和压缩包。

输出位置：

- `release/linux-amd64/traffic-go`
- `release/linux-amd64/config.yaml`
- `release/linux-amd64/install-centos7.sh`
- `release/traffic-go-linux-amd64.tar.gz`

当前前端构建默认使用相对静态资源路径，配合运行时基路径探测，可以让同一个构建挂到不同的 Nginx 子路径下，不需要为 `/traffic/`、`/foo/`、`/anything/` 分别重建。

## 在 CentOS 7 上安装

最省事的路径是直接用打包产物里的安装脚本。它不会创建新用户，而是把服务放到 `/root/traffic-go-release` 下运行。

```bash
mkdir -p /root/traffic-go-release
tar -xzf traffic-go-linux-amd64.tar.gz -C /root/traffic-go-release
cd /root/traffic-go-release
bash install-centos7.sh
```

这个脚本会自动：

- 安装/覆盖 `/root/traffic-go-release/traffic-go`
- 安装/覆盖 `/root/traffic-go-release/config.yaml`
- 把 `db_path` 改写为 `/root/traffic-go-release/traffic.db`
- 写入 `/etc/systemd/system/traffic-go.service`
- `modprobe nf_conntrack`
- 写入 `/etc/modules-load.d/nf_conntrack.conf`
- 执行 `sysctl -w net.netfilter.nf_conntrack_acct=1`
- 写入 `/etc/sysctl.d/90-conntrack-acct.conf`
- `systemctl enable --now traffic-go`

如果你已经把文件单独传到服务器，也可以这样安装：

```bash
cd /root/traffic-go-release
bash install-centos7.sh ./traffic-go ./config.yaml
```

### 手动 systemd 部署

如果你想走更标准的 FHS 路径，仓库里还有一个手写模板：

- `deploy/traffic-go.service`

这个模板假定你使用：

- `/usr/local/bin/traffic-go`
- `/etc/traffic-go/config.yaml`
- `/var/lib/traffic-go/traffic.db`
- `traffic-go` 用户

它更接近长期生产部署，但需要你自己创建用户、放置文件和处理 capability。

## Nginx 反代到 `/traffic/`

如果你想把站点挂到 `https://example.com/traffic/`，推荐做法是：

1. 正常构建发布包，不需要额外传基路径参数：

```bash
bash deploy/build-linux-gitbash.sh
```

2. 后端仍然监听本机地址，例如：

```yaml
listen: "127.0.0.1:18080"
```

3. 用 Nginx 把 `/traffic/` 前缀转发到后端根路径：

```nginx
location = /traffic {
    return 301 /traffic/;
}

location /traffic/ {
    proxy_pass http://127.0.0.1:18080/;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-Prefix /traffic;
}
```

这个配置依赖两点同时成立：

- 访问 `/traffic` 时要先 `301` 到 `/traffic/`。因为前端静态资源是相对路径，直接打开不带尾斜杠的 `/traffic`，浏览器会把 `./assets/...` 解析成 `/assets/...` 的上级同级路径，最终导致资源 `404`。
- `proxy_pass` 末尾必须带 `/`，这样 Nginx 才会把 `/traffic/...` 转成发给后端的 `/...`。

下面这两种写法的效果不同：

```nginx
proxy_pass http://127.0.0.1:18080/;  # 正确：剥离 /traffic 前缀
proxy_pass http://127.0.0.1:18080;   # 错误：把 /traffic/... 原样转给后端，Go 服务会 404
```

现在同一个构建可以挂到任意前缀路径，只要 Nginx 也按同样方式配置。例如下面这些都可以：

```nginx
location /traffic/ { ... }
location /ops/traffic/ { ... }
location /abc-123_x/ { ... }
```

如果你要挂到更深的路径，例如 `/ops/traffic/`，规则完全一样：

```nginx
location = /ops/traffic {
    return 301 /ops/traffic/;
}

location /ops/traffic/ {
    proxy_pass http://127.0.0.1:18080/;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

## 配置

示例文件见 [deploy/config.example.yaml](./deploy/config.example.yaml)。

当前支持的主要配置项：

- `listen`: HTTP 监听地址，默认 `127.0.0.1:8080`
- `db_path`: SQLite 文件路径
- `tick_interval`: 采集周期，默认 `2s`
- `proc_fs`: `procfs` 根目录，默认 `/proc`
- `conntrack_path`: conntrack 文件路径，默认 `/proc/net/nf_conntrack`
- `mock_data`: 强制 mock collector
- `retention.flows_days`: 分钟数据保留天数，也是 PID/EXE 过滤可用的最长窗口
- `retention.hourly_days`: 小时数据保留天数

补充说明：

- `flows_days` 或 `hourly_days` 设为 `0` 时，当前实现会回退到默认值 `30` 和 `180`。
- `log_level` 字段会被解析，但当前后端仍使用基础 stdout logger，没有真正的按级别过滤。

## API 概览

主要接口如下：

- `GET /api/v1/healthz`
- `GET /api/v1/processes`
- `GET /api/v1/stats/overview`
- `GET /api/v1/stats/timeseries`
- `GET /api/v1/usage`
- `GET /api/v1/top/processes`
- `GET /api/v1/top/remotes`
- `GET /api/v1/top/ports`
- `GET /api/v1/forward/usage`

常见查询方式：

- `?range=1h`
- `?range=24h`
- `?range=7d`
- `?start=2026-04-15T00:00:00Z&end=2026-04-15T12:00:00Z`

常见过滤参数：

- `comm`
- `pid`
- `exe`
- `remote_ip`
- `local_port`
- `direction`
- `proto`
- `attribution`
- `cursor`
- `limit`

下面示例里的请求地址都用 shell 变量表示，避免把端口写死：

```bash
LISTEN_ADDR=<listen-address>
```

### 示例

健康检查：

```bash
curl "http://${LISTEN_ADDR}/api/v1/healthz"
```

过去 1 小时总览：

```bash
curl "http://${LISTEN_ADDR}/api/v1/stats/overview?range=1h"
```

查询某个进程的分钟级历史：

```bash
curl "http://${LISTEN_ADDR}/api/v1/usage?range=6h&comm=ss-server&limit=50"
```

按方向看趋势：

```bash
curl "http://${LISTEN_ADDR}/api/v1/stats/timeseries?range=24h&group_by=direction"
```

查询 forward/NAT 流量：

```bash
curl "http://${LISTEN_ADDR}/api/v1/forward/usage?range=1h&limit=50"
```

## 查询语义

- 分钟数据源是 `usage_1m`，小时数据源是 `usage_1h`。
- 当查询窗口超过 `retention.flows_days` 时，系统会自动降级到小时表。
- 一旦降级到小时表，`pid`、`exe`、`attribution` 等分钟级维度不可用。
- `/api/v1/processes` 返回的是当前活跃且成功归因的进程，不是历史进程目录。
- `forward` 流量单独落到 `usage_1m_forward` / `usage_1h_forward`，不会和普通入站/出站重复计算。

## 已知限制

- 仅支持 Linux。设计目标是老内核和老发行版，不走 eBPF。
- UDP 归因是 best-effort。未 connect 的 UDP、DNS、QUIC、某些代理 relay 流量可能只能统计，不能准确归属到进程。
- 短连接可能在两个 tick 之间出现又消失，导致完全看不到。
- 已经存在的连接在服务启动后的第一次观测只会建立 baseline，不会回补监控启动前的历史流量。
- 当前分钟内的流量在跨分钟前主要体现在 runtime 连接数，不一定已经体现在历史字节数接口里。

## 安全说明

不要把 `traffic-go` 直接监听到公网地址。

它暴露的是本机网络元数据，包括：

- 哪个进程在通信
- 通向哪个远端 IP
- 使用了哪个端口
- 流量和包数量

默认监听地址是配置里的 `listen`。如果需要远程访问，建议使用：

- SSH 端口转发
- 带认证的反向代理
- 内网访问控制

## 排障

### 1. `healthz` 正常，但 `bytes_up` / `bytes_down` 一直为 0

先检查：

```bash
sysctl net.netfilter.nf_conntrack_acct
head -n 5 /proc/net/nf_conntrack
```

应该看到：

- `net.netfilter.nf_conntrack_acct = 1`
- 新连接行里带 `bytes=` / `packets=`

如果没有：

```bash
sysctl -w net.netfilter.nf_conntrack_acct=1
```

### 2. API 能访问，但刚打完流量还是 0

这是常见现象，不一定是故障：

- 首次观测会先建 baseline
- 分钟增量在跨分钟前不会完全落盘

更可靠的验证方式是：

```bash
curl -L "https://speed.cloudflare.com/__down?bytes=50000000" -o /dev/null
sleep 70
curl "http://${LISTEN_ADDR}/api/v1/stats/overview?range=1h"
```

### 3. 服务启动失败，日志提示 `conntrack path ... unavailable`

说明 `/proc/net/nf_conntrack` 不存在或不可读。检查：

```bash
ls -l /proc/net/nf_conntrack
modprobe nf_conntrack
```

### 4. 当前端口被占用

改配置里的 `listen`，例如：

```yaml
listen: "127.0.0.1:18080"
```

然后重启服务：

```bash
systemctl restart traffic-go
```

### 5. 看不到进程，只能看到连接数

先看：

```bash
curl "http://${LISTEN_ADDR}/api/v1/processes"
```

如果 `active_connections` 有值但进程列表为空，通常是：

- 这些流量是 `forward`
- UDP 无法稳定归因
- `/proc/[pid]/fd` 没有足够权限

## 开发验证

后端：

```bash
go test ./...
go build ./cmd/traffic-go
```

前端：

```bash
npm --prefix web run test
npm --prefix web run build
```

如果你需要重新生成内嵌前端：

```bash
make sync-frontend
```

## 仓库结构

下面这棵树对应当前仓库里主要的版本化文件：

```text
traffic-go/
├── cmd/
│   └── traffic-go/
│       └── main.go                  # 程序入口，解析 -config 并启动 app
├── deploy/
│   ├── build-linux-gitbash.sh       # Windows Git Bash 打 Linux 发布包
│   ├── config.example.yaml          # 示例配置
│   ├── install-centos7.sh           # CentOS 7 安装脚本
│   └── traffic-go.service           # 标准 systemd unit 模板
├── internal/
│   ├── api/                         # HTTP API、参数解析、SPA 路由
│   ├── app/                         # 应用装配、HTTP 服务、聚合/清理任务
│   ├── collector/                   # conntrack 采集、方向判断、进程归因
│   ├── config/                      # 配置加载、默认值、校验
│   ├── embed/
│   │   ├── dist/                    # 内嵌前端静态资源，占位文件会被构建产物替换
│   │   └── static.go                # embed.FS 封装
│   ├── model/                       # 共享类型定义
│   └── store/                       # SQLite schema、分钟落盘、小时聚合、查询
├── web/
│   ├── src/
│   │   ├── components/              # 通用 UI 组件
│   │   ├── data/                    # 前端 mock 数据
│   │   ├── pages/                   # Dashboard / Usage / Processes 等页面
│   │   └── test/                    # 前端测试
│   ├── index.html                   # Vite 入口 HTML
│   ├── package.json                 # 前端依赖与脚本
│   ├── package-lock.json            # npm 锁文件
│   ├── tsconfig.json                # TypeScript 配置
│   └── vite.config.ts               # Vite 配置与 /api 代理
├── .gitignore                       # 忽略本地工具、构建产物和运行数据
├── go.mod                           # Go 模块定义
├── go.sum                           # Go 依赖校验
├── Makefile                         # 常用开发命令
└── README.md                        # 项目说明
```

各层职责可以简单理解成：

- `cmd/` 只做启动，不放业务逻辑。
- `internal/collector/` 负责“看到流量并归因”。
- `internal/store/` 负责“把流量落库并能查出来”。
- `internal/api/` 负责“把查询能力变成 HTTP API”。
- `internal/app/` 负责把 collector、store、API 和维护任务串起来。
- `web/` 负责 WebUI，最终会被打包并拷到 `internal/embed/dist/`。
- `deploy/` 负责发布、安装和 systemd 运行模板。

另外，仓库根目录可能还会存在这些本地目录，但默认不纳入版本控制：

- `.claude/`
- `.codex/`
- `.tools/`
- `docs/`
- `web/node_modules/`
