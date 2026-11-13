# vsegment 开发计划

## 目标与边界

按最新方案实现 Partition owner SubMaster 管理的 vsegment。psegment 空间按
Partition 静态切分；布局参数严格采用配置值，资源不足明确报错，不自动降低
`member_count`。代码替换 vchunk，但不改变现有 Object Replica 的副本语义。

## 迭代与提交节奏

每 2～3 个迭代形成一次本地提交，单次提交不超过 1000 行，不推送远端。

### 阶段 A：独立领域核心

- [x] 迭代 1：定义 profile、Partition 静态配额、extent 与 View 模型。
- [x] 迭代 2：实现配置、View、checksum 和布局不变量校验。
- [x] 迭代 3：实现 Partition 配额内的原子物理 extent 分配与释放。
- [x] 迭代 4：实现逻辑 `free_ranges` 与 Put reservation 生命周期。
- [x] 迭代 5：按条带和 Client Slice 边界展开传输子请求。
- [x] 迭代 6：实现相同 `creation_key` 的进程内单创建协调。
- [x] 迭代 7：实现 Partition 本地 `VSegmentManager` 编排。
- [x] 迭代 8：定义可序列化 Snapshot 状态并支持恢复重建。
- [x] 迭代 9：恢复时校验配置代次、View checksum 与 extent 占用冲突。

### 阶段 B：接入现有 SubMaster

- [ ] 迭代 10：在 MasterConfig 加载 vsegment profile 与 Partition 配额。
- [ ] 迭代 11：把 `VSegmentManager` 挂接到 KV Partition 生命周期。
- [ ] 迭代 12：增加创建、查询 View、Put reservation 的内部服务接口。
- [ ] 迭代 13：将 View 与 ReplicaDescriptor 接入 PutStart/ObjectMetadata。
- [ ] 迭代 14：将条带子请求接入 TransferSubmitter，并汇总部分失败。
- [ ] 迭代 15：Get 按精确 View/缓存结果执行同一映射路径。

### 阶段 C：HA、清理与可观测性

- [ ] 迭代 16：为 View、物理分配和 reservation 定义 OpLog 事件。
- [ ] 迭代 17：接入现有 SubMaster Snapshot/OpLog 存储与恢复框架。
- [ ] 迭代 18：恢复时使用 ObjectMetadata 重建已提交逻辑占用。
- [ ] 迭代 19：增加 reservation 超时回收和失败传输清理。
- [ ] 迭代 20：增加容量、创建冲突、配额不足与恢复指标。

## 验证门槛

- 单元测试覆盖配置错误、配额不足、并发创建、并发 reservation、映射边界、
  checksum、Snapshot 恢复和冲突拒绝。
- 接入测试覆盖 Put/Get、多 Slice、跨条带、部分传输失败和 SubMaster 重启。
- 每次提交执行 `git diff --check`、相关测试与 vchunk 残留扫描；如果本机缺少
  构建工具，必须在交付说明中明确列出未执行项。
