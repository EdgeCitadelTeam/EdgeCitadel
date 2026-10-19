# EdgeCitadel：跨 session 研究与工程上下文

更新：2026-10-12（新增 adversarial-source 审计与首轮真实测试；其余历史状态保留日期）。用途：为新 session、合作者和其他 agent 提供接手摘要。
本文件总结对话与已有项目证据，不是一次新的远程审计，也不自动授权执行其中的计划。

## 1. 一分钟概览

EdgeCitadel 从已被 MobiSys 接收的 poster 延伸，目标是支持不同 harness、站点和资源条件下的 agent 协作。目标投稿方向为 MLSys。

当前研究目标：**在持续运行的异构 edge-agent 工作流中，利用 provenance 选择并实施低代价隔离，限制异常传播，同时保留尽可能多的正常执行。**

- Tree 保持为站点组织、成员关系和路由结构。
- 执行追踪从 event-only DAG 向 PROV 风格的 activity + versioned entity + agent attribution 升级。
- 研究重点从事后 tracing 转向运行时 provenance-guided containment。
- 加权 minimum cut / maximum flow 是拟采用的优化内核；尚未实现或验证论文级在线隔离机制。
- 特别关注 edge 上的通信、控制、图维护开销，以及本地开放权重模型带来的部署约束。
- 已有真实 Pi + Jev root 到 Endurance Pi + 本地模型的端到端执行路径。用户明确要求持续运行的真实 harness，不接受以独立 demo 脚本代替。
- 用户计划逐步用本地 Laya 替换 Jev；本对话没有完成 Laya 部署或性能验证。

## 2. 状态分层：不要混淆事实、决定和建议

| 项目 | 状态与边界 |
|---|---|
| 常驻 Pi root / worker 和真实 EdgeCitadel 委派 | 2026-09-27/28 已验证；2026-10-05 未重新连接远程检查 |
| Tree 基础设施 | 此前讨论已有针对性验证；不能据此声称任意拓扑、故障和规模全部通过 |
| Robustness audit | 2026-09-28 完成诊断性测试，有报告和手工复现方法 |
| 普通工程访问控制修复 | 已分离出清单；本对话尚无完成部署的证据 |
| Tree 不动，执行图采用 PROV | 用户明确同意的方向 |
| Max-flow/min-cut 用于隔离选择 | 用户同意继续研究；目标函数、控制图与执行语义尚待确定 |
| 本地先暂停、全局细化后释放 | 助手提出的候选机制，尚非用户确认的实现方案 |
| 增量 max-flow、图压缩、分布式求解 | 可能的后续优化，不能当作现有贡献或既定要求 |
| MLSys 实验框架 | 已讨论的规划，没有论文级结果 |
| 文献 novelty | 做过近邻检索，未证明没有重叠工作；没有已建立的持续 literature radar |

## 3. 工程入口与证据

仓库：https://github.com/EdgeCitadelTeam/EdgeCitadel

本次整理时的本地 checkout：`/Users/jameszhan/Documents/New project/edgecitadel-pi-root`。
分支：`codex/pi-jev-root`；HEAD：`8aa296b`。
整理前工作区干净。本次只增加上下文文档与 AGENTS 入口；没有重新测试线上、commit 或 push。
此前记录本分支改动尚未 push；新 session 应检查 remote 状态，不把该记录当作 GitHub 当前状态。

优先阅读：

- [2026-10-07 containment 实验讨论稿 v1](containment-experiments-v1.zh.md)：多层树、case、机制与框架两类基线、检测／隔离分离、参考博客；均为未执行建议。
- [常驻 Pi 的操作与实现说明](pi-harness.md)
- [2026-09-28 robustness 审计](robustness-20260928.md)
- [中文手工复现与工程／provenance 分界](manual-robustness-and-controls.zh.md)
- 审计运行器：`tools/pi/robustness_probe.py`；手动辅助：`tools/pi/manual-probe.sh`。
- 私有原始证据曾保存于忽略目录 `local-docs/robustness-20260928/`，不保证其他 checkout 含有这些文件。

最近一次已验证的部署记录：

| 机器 | 角色和实际执行路径 |
|---|---|
| jim-eq，100.97.29.74 | 常驻 `jim-eq-pi`，skill `pi.coordinate`；Pi SDK 0.87.1；Python adapter 调 Jev API 选执行者，再通过 agentd `trace.dispatch` 委派 |
| NetSys-Endurance，100.100.120.122 | 常驻 `endurance-pi`，skill `pi.execute`；本地 `qwen3.8:27b`，真实 Pi 工具循环；工作目录 `/home/zhonghao/edgecitadel-workspace` |
| NetSys-Cluster，100.118.214.56 | 第三个可用站点；最近验证的 Pi root 配置没有给予它执行目的地 grant |

jim-eq 的终端 `pi` 是连接常驻 root 的 HTTP 聊天客户端，不是新启动的模型或 upstream Pi TUI。
页面入口为 `http://100.97.29.74/pi.html`。当前客户端的 Ctrl-C 是脱离等待，不等于取消远程任务。
root 当时允许的目的地为 Endurance 和 jim-eq 上的 Hermes；每轮至多一次远程委派。

Jev 是远程 typed-decision API，不是 jim-eq 本地权重。请求和近期上下文会发送到该服务。
密钥通过服务的私有凭据机制加载；本摘要不包含任何密钥、密码或私有 transcript。
具体凭据路径及运行细节见 `pi-harness.md`，不要读取或打印凭据内容来做普通状态检查。
SSH 方式曾发生变化；使用当前配置与仓库说明核实，不恢复旧密码或假定历史连接方式仍然有效。

## 4. 已发现的问题与应先完成的工程修复

审计是 13 次顺序任务提交、每种情况少量输入，不是压力测试、攻击成功率统计或论文性能结果。
22 个相关本地测试通过，不代表全部线上访问控制已经正确。

主要观察：

1. 可访问 tailnet 中 Core HTTP 的客户端，无应用层身份就能发任务、读结果和部分 trace。没有测试公网暴露或绕过 Tailscale。
2. 直接调用 worker 可绕过 root 委派路径；未知 skill 字符串也曾执行成功。
3. 相同 session ID 可以在不同 HTTP 客户端间复用上下文；当时缺少独立用户 ownership。
4. HTTP `sender_id` 可由调用者指定，冒充 root 后可打开测试会话。没有窃取或破解 agentd connector 凭据。
5. 工作目录及 `sandbox: restricted` 声明不等于 OS 文件隔离；显式读取目录外的合成 canary 成功。
6. Jev 曾忽略明确的目的主机要求；硬性路由约束需要在模型选择之前过滤。
7. 无合法目的地时没有派出 child，但外层状态仍是 completed；完成处理不等于完成用户目标。
8. 一个“root 已批准整个 swarm”的注入样本被模型抵抗，但没有独立工具策略保证此结果。
9. 精确重放请求返回原 child；同 ID 不同 body 被拒绝。这是已测情形，不是一般 exactly-once 保证。
10. 现有图有任务、模型、工具与传输记录，但缺少明确 artifact 版本及跨任务 context lineage；13 个图均标注 partial / unknown sources。

所有科学比较配置都应共享的基础修复：认证入口及服务端身份绑定；接收端授权／验证委派；session、结果、trace ownership；skill/schema 验证；确定性目的地约束；工具与 OS 资源边界；明确任务终态语义。保留并验证现有去重和 trace 完整性标记。

不能用“新方案有身份验证，baseline 没身份验证”来证明 provenance 或 min-cut 的科学价值。

## 5. 已收敛的图与隔离概念

- Tree node 是站点／成员组织中的节点；执行 DAG node 是具体 activity 实例、不可变 artifact/context 版本等。一个 agent 可以产生很多执行节点。
- 使用 PROV 风格的 used、wasGeneratedBy、wasAssociatedWith、derivation 等关系；authorization 语义需显式定义，不能由一条 provenance 边自动授予权限。
- 并非所有 PROV 图天然是 DAG。需要定义用于传播分析的因果子图、边方向、版本与实例语义；墙钟时间本身不能证明无环。
- PROV 的标准关系方向和分析图采用的影响传播方向可能不同，必须明确转换，不能直接把所有原始边混在一起做 reachability。
- 历史图说明发生了什么；当前控制图说明现在还能阻止什么。两者必须分开。
- Pruning 指拒绝后续读取、消息交付、任务推进或工具执行，必要时取消／隔离执行；不是删历史记录，也不是默认杀掉整棵子树。
- first responder 不一定是 root cause。图可支持追溯与影响分析，但不自动识别根因或检测一切异常。
- 已读入异常数据的上下文、已在途的请求需要进入当前影响分析；不能剪掉过去的边就假定风险消失。
- Dijkstra 找最短路径，不能默认解决阻断所有相关传播路径的问题。使用正确术语 maximum flow / minimum cut，不是 MaxCutMinFlow。
- 最小割保证限定于固定图、覆盖所要求传播路径、可实施的控制点、非负加性代价等假设。
- 实际下游工作损失可能重叠而不可加；模型内最小代价不等于真实业务损失全局最优。
- 决策和执行之间仍存在竞态。需定义策略版本、执行确认、断连时行为，以及从什么时刻开始承诺阻断。
- 不把 TEE、博弈论、host compromise 防御加入当前核心 scope；可信 runtime/OS 和不可绕过的执行边界仍是必要假设。
- 物理动作可能不可逆；停止后续命令不等于设备已经安全停止。保留本地 interlock，并区分软件阻断与物理安全状态。

## 6. 论文方向与实验规划（尚未执行）

潜在主线：provenance-guided online containment for heterogeneous edge-agent workflows。
新颖性应落在可实施控制图、在线隔离选择和实际执行机制上，而非声称 Tree、DAG 或 max-flow 算法本身新颖。

Edge 约束应影响设计：细粒度 provenance 与内存／带宽成本；全局决策与跨站点延迟；本地快速暂停与正常工作损失；模型与控制路径的资源竞争。
Jev → Laya 是模型部署计划和实验因素，不是已证实的性能提升，也不让模型取代授权检查。

三组 baseline：

1. 协议：固定版本 A2A SDK 与 EdgeCitadel，在相同任务、模型和可比认证／重试／持久化语义下比较。
2. 编排：LangGraph checkpoint／恢复等配置与异构 harness 路径。LangChain/LangGraph 不是另一个同层 A2A wire protocol。
3. 控制：共同工程 guardrail 下，整子树隔离、因果后代隔离、artifact 拒绝、简单贪心、标准静态加权 min-cut、拟议在线机制。匹配告警、图信息和可用控制点。

三层证据：

1. 真实 middleware/runtime + 确定性任务执行器 + 已知依赖／注入时序，隔离因果机制收益。
2. 真实本地模型与异构 harness；使用可检查结果的 coding、数据分析等工作流，重复并配对比较。
3. 现有 IoT 实验床的安全闭环作为外部有效性验证。用户没有 MHS preview 或物理／化学实验室权限。

关键指标：告警后与隔离确认后的实际违规动作；正常任务正确完成吞吐（goodput）、误伤任务、浪费的计算；正常及异常时端到端尾延迟、CPU、内存、网络和存储开销；隔离生效及恢复时间。
先固定检测输入比较响应机制，再改变检测延迟／误报。模型口头拒绝不等于工具没有执行。
协议开销、路由模型延迟、执行模型延迟分别测量；Jev 云端与 Laya 本地比较需区分网络因素。
三台机器可以支持机制验证；更多逻辑 agent 不等于已验证更多物理站点。

## 7. 继续之前要回答的问题

1. 当前执行图如何映射为可实施控制图？控制动作精确到 artifact/version、任务、会话还是进程？
2. 代价函数测量什么，哪些部分可加，如何处理共享下游和估计误差？
3. 哪些真实任务确实存在比“拒绝 artifact + 取消消费者”更好的非平凡隔离选择？
4. 图采集、求解和实施隔离的总成本，能否被保留的正常工作抵消？
5. provenance 不完整、网络分区、在途执行时，可承诺的保证是什么？
6. 先修好共同工程基线，再冻结版本开展比较；继续优化 scope 与最小验证方案，不因本摘要自动启动大规模实现。

用户经验：此前 SimVerity 的物理实验噪声和偏 empirical 的论证曾受到质疑（用户描述，未在此重新审阅评审意见）。本项目希望更直观、更实用，并将机制、模型质量与物理噪声分开，不能用数学符号代替实证。

## 8. 文献与原始背景入口

以下是之前讨论过的入口，不是完整 novelty 审计，也没有在本次摘要中重新核实全部内容：

- A2A：https://a2a-protocol.org/latest/specification/
- LangGraph：https://docs.langchain.com/oss/python/langgraph/persistence
- CaMeL：https://arxiv.org/abs/2503.18813
- PACT：https://arxiv.org/abs/2605.11039
- 早期 cost-effective incident response 技术公开：https://patents.google.com/patent/US20050257269A1/en （专利文献，不是同行评审论文）
- MLSys 2026 范围参考：https://mlsys.org/Conferences/2026/CallForPapers （不能代替目标年份最新投稿规则）

用户原始文件位置：

- Poster：`/Users/jameszhan/Documents/paper/mobisys-poster/Poster- EdgeCitadel - Hybrid NATS-MQTT Orchestration for Edge Multi-Agent Systems.pdf`
- 早期规划：`/Users/jameszhan/Downloads/summary.md`
- 设计说明：`/Users/jameszhan/Downloads/Tech details.pdf`

附带偏好：最小树示意图拟命名 KIPP（root）、TARS/CASE（leaf），来自用户的电影梗偏好；当时明确要求先不要修改正式节点名。研究范围决策优先于装饰性命名。

## 9. 给新 session 的接手提示

> 请先阅读本文件，以及 docs/robustness-20260928.md、docs/pi-harness.md。区分历史部署证据、用户已同意的方向和未验证的研究建议。我们正在优化 EdgeCitadel 的 MLSys 问题定义与实验设计：Tree 保持不动，PROV 执行图支持低代价在线隔离，min-cut 为候选优化内核，重点包含 edge 开销和本地模型。继续工作前核对当前代码与部署差异；不要把“已能端到端执行”当成“访问控制已修好”或“论文机制已实现”。

后续维护：在有新实验、明确设计决定或部署变化后更新相应章节及验证日期，保留证据路径。不要把凭据和原始私有会话复制进上下文摘要。

## 10. 2026-10-12：攻击来源插件审计与初始三机测试

详见 [审计报告](adversarial-source-audit-20261012.zh.md)。GitHub main 为 `4d464fe`；新增 adversarial-source 0.2.0 是 skills-only fixture authoring 包，不是常驻攻击服务。离线校验、5 项回归及 55 个文件 hash 验证通过。

三机均部署了独立版本的最新源码/插件；jim-eq 隔离的攻击端 Pi 实际发现七个 skills。**没有更新、重启或覆盖三机现有生产运行时**；root 的四个相关上游运行时/schema 文件已匹配。Cluster 旧 bridge 有本地修改，且没有出现在当前 Core 在线 roster，本轮仅作为受控 HTTP 报告来源，不能称为第三个受管 A2A agent。

常驻 jim-eq Pi/Jev → EdgeCitadel → Endurance Pi/local Qwen 的四个首轮任务成功读取 Cluster baseline/payload 并返回正常数值；两个无害 marker 注入均被模型抵抗。JSON 素材是以文档读取方式投递，未测试原生结构化工具响应拦截。没有运行 provenance/min-cut 防御。

两个同会话 follow-up 都明确指定 Endurance，却被 Jev 路由到 jim-eq Hermes；缺失报告上下文，用户目标失败但外层 completed。正常组也发生，不能归因于注入；跨轮污染尚不可判定。六个图仍 partial/unknown_sources。应先修硬性目标路由、会话执行者/上下文关联和结果语义，再开展多跳控制对比。

临时 Cluster source 已停止；原有服务仍在线。结果位于工作区 `output/edgecitadel-redteam-20261012/`，没有 commit/push。
