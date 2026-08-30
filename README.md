# Platform Go SDK

跨服务稳定能力的公共 Go 模块。事件总线基于 NATS JetStream 和统一 Protobuf Envelope，提供消息 ID 去重、显式确认、受限并发、处理超时、指数退避、最终死信、损坏消息留存，以及 Request ID/W3C Trace Context 传播。死信使用共享 `platform.common.v1.DeadLetterEvent`，成功发布死信后才终止原消息，避免事件静默丢失。

消费者应优先使用 `ConsumeWithOptions` 并配置 `OnError` 接入服务日志和指标。`Consume` 保留为兼容入口。领域副作用仍需服务自身的事务 Inbox 或幂等边界；共享 Consumer 不会把“处理函数返回成功”错误地等同于跨数据库事务原子提交。

领域模型、SQL、迁移、前端 DTO 和具体业务编排不得放入本模块。
