# Platform Go SDK

跨服务稳定能力的公共 Go 模块。事件总线基于 NATS JetStream 和统一 Protobuf Envelope，提供消息 ID 去重、显式确认、受限并发、处理超时、指数退避、最终死信、损坏消息留存，以及 Request ID/W3C Trace Context 传播。死信使用共享 `platform.common.v1.DeadLetterEvent`，成功发布死信后才终止原消息，避免事件静默丢失。

消费者应优先使用 `ConsumeWithOptions` 并配置 `OnError` 接入服务日志和指标。`Consume` 保留为兼容入口。领域副作用使用 `inbox.SQLStore.Process` 与本服务领域写入共享 `*sqlx.Tx`：成功结果和 Inbox 状态原子提交，失败领域写入回滚但保留失败尝试，重复事件不会再次执行处理函数。每个服务仍需在自己的迁移中创建 Inbox 表；公共 SDK 不跨服务建表或查询数据库。

领域模型、SQL、迁移、前端 DTO 和具体业务编排不得放入本模块。
