# 话题并行执行方（TF-1）

一个指挥主会话通过 `cc-connect topic new` 创建原生飞书话题，在话题中 @另一个 agent。执行方按 thread_id 使用独立逻辑会话和 Codex 会话；指挥侧继续使用原来的主线会话键。

## 配置

只在执行方的既有 Feishu options 段新增：

```toml
[projects.platforms.options]
thread_isolation_mode = "topic_only"
topic_concurrency = 4
```

不要重复声明已存在的 options 表。不要同时设置 `thread_isolation = true`。执行方应继续使用需要 @ 的接收方式（默认 `group_reply_all = false`），否则无 @ 的顶层任务标题也可能作为群普通消息接收。指挥侧不设置 topic_only。现有 share_session_in_channel 值保持原样。

- 群中 thread_id 非空：`feishu:<chat>:thread:<thread_id>`。
- 顶层消息、只有 root_id/parent_id 的普通回复：沿用原键。
- 旧 thread_isolation=true：仍按 root_id/message_id 拆分。
- 话题输出、附件及重建 reply context 使用真正消息 ID 作锚点，再设置 SDK reply_in_thread；不把 omt_ ID 当 om_ ID。
- 上限是本项目、每平台每群的活动话题轮次，主线不占名额。闲置但保留的后端连接不占名额。超限话题 FIFO 等待；N 是排队队列中前面的不同话题数，不含当前活动话题。
- 同一话题沿用已有中途注入/忙时队列规则；普通消息队列原有容量和明确拒绝提示保持不变。
- 话题准入队列在内存。/stop、stop-all 和正常关闭明确取消队列消息；正常关闭记录条数、尝试发送取消回执。崩溃/SIGKILL 不提供队列恢复保证。维护前应先停止/核对排队消息。

## 指挥侧 CLI

```sh
cc-connect topic new --to 'henry codex' --title '检查构建脚本' --file spec.txt
cc-connect topic new --to ou_executor --title '另一个任务' --stdin < spec.txt
cc-connect topic list
```

默认 CC_PROJECT、CC_SESSION_KEY、CC_DATA_DIR；可 -p、-s、--data-dir 覆盖。--to 是 open_id 或本实例 mention_map 名字，不能指向自己。正文必须是非空 UTF-8，最多 64 KiB；标题单行、最多 100 字。创建结果始终输出 JSON；即使失败也保留可用的部分回执，非零退出。

先发无 @ 的顶层文本“【子任务 标题】”，再用 SDK Reply 创建 reply_in_thread=true 的正文，开头原生 at。返回 topic.root_message_id、topic.thread_id、topic.message_id、topic.status。sent 表示飞书 API 接受，不能据此声称执行方已开始处理。

--request-id 可指定 32 位十六进制幂等 ID，默认随机。内部 API 先记录 creating 意图，再执行网络发送，最后保存 sent/failed 回执。请求超时先 topic list；creating/failed 不自动重放，以免重复开话题。同 ID 换正文/标题/目标会报错。记录含正文 hash，不保存规格正文，路径为 data_dir/topics/<project-name-hex>.json。

topic list 只列本会话键创建的记录，按需从飞书读取话题最新发送者和时间（平台返回的毫秒时间戳）；失败保留 latest.error。指挥侧收到该实例创建过的话题消息时，在正文加入“[话题 标题 thread=...]”；标题从根消息读取，失败使用登记标题。指挥侧会话键不变，忙时沿用原有插话/排队规则。未登记的历史话题不增加前缀。

## 内部 API 与停止

均走现有 data_dir/run/api.sock，无新网络监听端口：

- POST /topics/new：project、session_key、request_id、to、title、body。
- GET /topics?project=...&session_key=...：创建记录和最新消息元数据。
- POST /topics/stop：project、platform（feishu）、chat_id。
- GET /topics/sessions?project=...&platform=feishu&chat_id=...：已知话题键、state、process_alive。

话题内 /stop 不经过并发等待，只取消该话题。stop API 取消该群所有已知话题及准入队列，不影响顶层主线。响应 stopped 是已发起停止的会话数；cleared_messages 是准入队列取消数，已有后端忙时队列沿用原停止行为。重复/并发全停按群串行。停止期间新消息明确报停止中。

关闭后端原本是异步操作；状态保留 closing，直到 Close 返回。不能只凭 stopped 数字宣称所有进程已退出。halt.py 应先设置群停工标记，再调用 stop API，verify 确认 state 无 running/queued/closing 且 process_alive=false。本批没有改 halt.py。

## 生命周期与回归范围

保留 interactiveMu、sessionSpawnMu 对启动环境/provider 的保护。Codex StartSession 复制配置后释放 Agent 锁；exec Send 启动独立子进程；app-server stdio 每个会话启动独立进程/原生 thread。模型执行阶段没有项目级串行锁。

在 rot2 9917e18e 上追加，保留 session new 原子退役/outbox 和预览确认语义。该基线没有 henry goal3 分支的原生 goal 接口；本批不移植另一条分支、不声称做了缺失功能的回归。

测试以实际 Engine、SessionManager、Unix socket CLI、SDK HTTP 和真实 Codex 适配器子进程为主体，仅替换飞书/CLI 模型边界。core/cuj_test.go 的 TestCUJ_TF1_ParallelExecutorTopics 证明主线+3话题重叠、交付前缀、单停、全停。agent/codex/topic_concurrency_test.go 覆盖 exec 与 app-server stdio 独立进程。没有调用真实模型或真实飞书创建测试话题。

## 上线与回滚草稿（由指挥方批准执行）

1. 核对候选版本、SHA 和当前程序/配置备份；先查 henry 可用空间（本批产物均在 ser7）。
2. 先停止词元 henry codex 的活动/排队话题，记录在线 PID、启动时间、程序和配置 hash。保留原程序，准备原配置副本。
3. 将 darwin/arm64 候选放到新版本路径；只在该实例 Feishu options 加上上述两个字段。保持 agent backend=app_server、app_server_url=stdio 及其他原值；指挥侧程序须有 topic CLI/API，键规则不变。
4. 由指挥方安排一次该执行方的服务重启。检查日志、主线连续上下文、三话题并行、原生 @ 回指挥、单停/全停、真实附件跟帖。任一关键路径失败即停止扩围。
5. 回滚：停止测试话题，将程序路径和配置恢复为备份，重启该实例；核对旧版本、主线与旧配置行为。新建话题逻辑记录仍可保留，但旧版不会使用新队列/调度。不要回滚覆盖 sessions JSON，避免复活 rot2 退役会话。

本批不替换程序、配置/plist 或重启在线服务。真实飞书的 thread_id 与机器人话题互收仍须上线冒烟验证：henry 现有日志无入站 debug 字段，模拟 SDK/event 不能替代真实平台证据。
