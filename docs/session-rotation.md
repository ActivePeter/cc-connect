# 手动换新会话并交接

~~~sh
cc-connect session new --handoff ./handoff.md --name 下一批 --json
cc-connect session new --prompt "目标、当前进度和下一步" -p 项目 -s 会话key --data-dir /path
~~~

--handoff 和 --prompt 必须二选一，内容为非空 UTF-8，最多 64 KiB。文件在提交申请时读成文本，不会在切换时重新读取。默认从 CC_PROJECT、CC_SESSION_KEY、CC_DATA_DIR 定位运行中的进程；显式参数优先。不存在的 key 报错，不自动创建。

会话自己这一轮调用时立即返回 SCHEDULED，当前回复被平台接口接受后才切换；空闲时直接切换并返回 SWITCHED。后者只表示逻辑会话已提交，不表示新模型已经回复。新会话先处理交接，再处理原队列；群内会看到切换通知。请求走数据目录下权限为 0600 的 Unix socket，不通过机器人自发消息或 timer 模拟 /new。

内部 API 为 GET /session/current、POST /session/new、GET /session/rotation。POST 使用 expected_session_id 防迟到请求，request_id 保证相同请求幂等；同一源会话尚未提交的不同请求保留最新版。CLI 注入的 CC_SESSION_ID 对应逻辑会话，CC_SESSION_SCOPE 区分工作区存储，防止不同工作区都有 s1 而误认；显式切到另一项目、key 或数据目录时会重新查询目标。

提交前出错保留旧逻辑会话和 native ID，旧进程可能需要重建。提交后的模型或通知投递失败保留新会话及待发交接，不回退已退役 ID。退役记录、新 active 和交接 outbox 一次原子落盘。受该会话存储管理的启动、切换、relay、迟到事件及重启路径都检查退役信息；不删除原生 transcript，不阻止 cc-connect 外的手工原生 CLI。不要用不认识退役元数据的旧程序改写该状态文件。

/new、/switch、/stop 等生命周期操作优先。进程重启会取消尚未确认最终回复送达的申请；已提交的待发交接保留。投递时崩溃造成的 in_flight 不确定性会明确报告，不自动重复执行；请先核对原生 transcript/日志，再决定重新组织交接或手动换新。

异步语音、已分离且无法确认完整交付的分段预览，以及无法确认最终交付的卡片路径保守不切，记录失败。本文仅包含手动 A；自动定时换新、静默写交接和自动策略 B 尚未实现。

## 隔离真模型验证

tests/manual/session_rotation/main.go 是显式运行的驱动，不会在常规测试中调用模型。真实 Engine、SessionManager、Unix API、后端和命令行子进程参与，只有平台发送为 fake。必须指定 /mnt/pool/ccrot/ 或 henry 的 ~/ccrot/ 下的独立根；phase 1 让旧轮自调用命令并检查回复/交接顺序，phase 2 在另一个驱动进程中检查恢复与退役 ID。Codex 的 --codex-home 必须位于隔离根内，--source-codex-home 只在同机复用现有登录；隔离凭据和原生配置绝不能收进证据包或传往 ser7。Claude 的可选 --credential-config 只读既有配置里的 API 环境，不借用其中的 work_dir；Codex 使用事先准备好的合法测试凭据和独立配置。

所有模型供应方、native ID、PID、原始回复和重启结果均写在对应根下，不能把 API 兼容供应方称为 Anthropic 模型。上线或替换运行中的实例需另行授权。
