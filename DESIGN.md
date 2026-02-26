# SimpleDB 设计文档

## 目录

- [1. 项目概述](#1-项目概述)
- [2. 事务管理](#2-事务管理)
- [3. 数据管理](#3-数据管理)
- [4. MVCC 多版本并发控制](#4-mvcc-多版本并发控制)
- [5. SQL 解析与执行](#5-sql-解析与执行)
- [6. 网络通信](#6-网络通信)
- [7. 项目架构图](#7-项目架构图)

---

## 1. 项目概述

SimpleDB 是一个使用 Go 语言实现的轻量级关系型数据库，灵感来源于 MySQL、PostgreSQL 和 SQLite。项目实现了一个完整的数据库引擎，涵盖事务管理、数据存储、MVCC 并发控制、B+ 树索引、SQL 解析以及客户端/服务器通信等核心模块。

### 核心特性

- **数据可靠性**：基于 WAL (Write-Ahead Logging) 的崩溃恢复机制
- **并发控制**：两阶段锁 (2PL) 实现可串行化调度
- **MVCC**：多版本并发控制，支持读已提交 (RC) 和可重复读 (RR) 两种隔离级别
- **死锁检测**：基于等待图的 DFS 死锁检测与自动处理
- **SQL 支持**：基本的 SQL 语句解析与执行（CREATE、SELECT、INSERT、DELETE、UPDATE 等）
- **C/S 架构**：基于 TCP 的客户端/服务器通信

### 分层架构

系统采用分层架构设计，各层职责清晰、解耦良好：

```
Client (客户端) → Transport (传输层) → Server (服务层) → Table Manager (表管理)
    → Version Manager (版本管理) → Data Manager (数据管理) → Transaction Manager (事务管理)
```

| 层级 | 目录 | 职责 |
|------|------|------|
| 客户端 | `client/` | 交互式 Shell，发送 SQL 命令 |
| 传输层 | `transport/` | TCP 通信，Hex 编码，Package 封装 |
| 服务层 | `backend/server/` | 连接管理，SQL 路由与执行 |
| 表管理 | `backend/tbm/` | 表元数据、字段定义、SQL 执行协调 |
| 版本管理 | `backend/vm/` | MVCC、可见性判断、死锁检测 |
| 数据管理 | `backend/dm/` | 分页存储、缓存管理、日志与恢复 |
| 事务管理 | `backend/tm/` | 事务状态持久化（XID 文件） |
| 索引管理 | `backend/im/` | B+ 树索引 |
| 公共模块 | `backend/common/`, `commons/` | 引用计数缓存、工具函数 |

---

## 2. 事务管理

> 源码：`backend/tm/TransactionManagerImpl.go`

### 2.1 XID 文件

事务管理器 (Transaction Manager) 通过 `.xid` 文件持久化所有事务的状态。文件格式如下：

```
XID 文件格式：
┌──────────────────────┬────────┬────────┬────────┬─────┐
│  Header (8 bytes)    │ XID=1  │ XID=2  │ XID=3  │ ... │
│  xidCounter (int64)  │ 1 byte │ 1 byte │ 1 byte │     │
└──────────────────────┴────────┴────────┴────────┴─────┘
```

- **Header（8 字节）**：存储当前最大事务 ID（`xidCounter`），使用 BigEndian 编码
- **事务状态**：每个事务占 1 字节，在文件中的偏移量为 `8 + (xid - 1)`
- **三种事务状态**：
  - `FieldTranActive = 0`：事务进行中
  - `FieldTranCommitted = 1`：事务已提交
  - `FieldTranAborted = 2`：事务已回滚

```go
type TransactionManagerImpl struct {
    file        *os.File       // XID 文件句柄
    xidCounter  int64          // 事务 ID 计数器
    counterLock sync.Mutex     // 保护计数器的互斥锁
}
```

### 2.2 SuperXid

`SuperXid = 0` 是一个特殊的事务 ID，它始终处于已提交状态。当执行非显式事务的 SQL 语句时，系统会自动使用 SuperXid 或创建临时事务来包装操作。

### 2.3 WAL (Write-Ahead Logging)

> 源码：`backend/dm/Recover.go`, `backend/dm/logger/`

SimpleDB 采用 Undo/Redo 双日志机制来保证崩溃恢复的正确性。

#### 日志格式

**Insert 日志**：

```
[LogType:1B] [XID:8B] [PageNumber:4B] [Offset:2B] [Raw:nB]
```

**Update 日志**：

```
[LogType:1B] [XID:8B] [UID:8B] [OldRaw:nB] [NewRaw:nB]
```

其中 `LogType` 取值：`LogTypeInsert = 0`，`LogTypeUpdate = 1`。

#### 日志文件结构

> 源码：`backend/dm/logger/DBLogger.go`

```
日志文件格式：
┌───────────────────┬──────────────────────────────────────────────┬──────────┐
│ XCheckSum (4B)    │ LogEntry1                                    │ LogEntry2│ ...
│ 全局校验和         │ [Size:4B][CheckSum:4B][Data:nB]              │          │
└───────────────────┴──────────────────────────────────────────────┴──────────┘
```

- **全局校验和（4 字节）**：所有日志条目校验和的异或值
- **每条日志**：`[Size(4B)] [CheckSum(4B)] [Data(nB)]`
- **校验算法**：使用种子值 `SEED = 13331` 对数据逐字节异或

```go
type DBLogger struct {
    file            *os.File
    lock            commons.ReentrantLock
    currentPosition int64    // 当前读取位置
    fileSize        int64    // 文件大小
    xCheckSum       int32    // 全局校验和
}
```

#### 崩溃恢复

当系统启动时检测到 PageOne 校验失败（非正常关闭），将触发恢复流程：

1. **Redo（重做）**：正向遍历所有日志条目，对已提交事务的操作重新执行
   - Insert：将原始数据重新写入对应页面的对应偏移位置
   - Update：将 NewRaw 写入对应 UID 位置
2. **Undo（撤销）**：按事务分组收集活跃事务的日志，逆序回滚
   - Insert：将对应数据标记为无效（`SetDataItemRawInValid`）
   - Update：将 OldRaw 写回对应 UID 位置
   - 最后将这些事务标记为 Aborted

### 2.4 ACID 保证

| 特性 | 实现机制 |
|------|----------|
| **原子性 (Atomicity)** | 通过 Undo 日志回滚未完成事务的所有操作 |
| **一致性 (Consistency)** | 通过 Redo 日志重放已提交事务 + PageOne 校验检测异常关闭 |
| **隔离性 (Isolation)** | 通过 MVCC 多版本并发控制（详见第 4 节） |
| **持久性 (Durability)** | 每次写入后调用 `file.Sync()` 强制刷盘 |

---

## 3. 数据管理

### 3.1 分页机制

> 源码：`backend/dm/dmPage/`，`backend/dm/constants/`

SimpleDB 使用固定大小的页作为磁盘 I/O 的基本单位：

```go
PageSize = 1 << 13  // 8192 字节 = 8KB
```

```go
type Page struct {
    pageNumber int                  // 页号
    data       []byte               // 页数据缓冲区
    dirty      bool                 // 脏页标记
    mu         commons.ReentrantLock
    pageCache  *PageCache           // 所属页缓存
}
```

#### PageOne（启动校验页）

> 源码：`backend/dm/dmPage/PageOne.go`

第一页用于检测数据库是否正常关闭：

```
PageOne 格式：
┌────────────────────────────────────────────┐
│             (前 100 字节保留)                 │
├────────────────────────────────────────────┤
│ ValidCheck 区域A (offset 100, 8 bytes)      │  ← 启动时写入随机字节
├────────────────────────────────────────────┤
│ ValidCheck 区域B (offset 108, 8 bytes)      │  ← 正常关闭时复制区域A
├────────────────────────────────────────────┤
│             (剩余空间)                       │
└────────────────────────────────────────────┘
```

- **启动时**：向区域 A（偏移 100）写入 8 字节随机数据
- **正常关闭时**：将区域 A 的数据复制到区域 B（偏移 108）
- **下次启动**：比较区域 A 和区域 B，若不一致则说明上次非正常关闭，触发恢复

#### PageX（数据页）

> 源码：`backend/dm/dmPage/PageX.go`

数据页使用前 2 字节记录空闲空间起始偏移：

```
PageX 格式：
┌───────────────────────┬──────────────────────────────────┐
│ FreeSpaceOffset (2B)  │          Data Area               │
│ 空闲空间偏移           │     [DataItem1][DataItem2]...    │
└───────────────────────┴──────────────────────────────────┘
```

- `FreeSpaceOffset`：指向当前页中第一个空闲字节的偏移量
- 最大可用空间：`PageSize - 2 = 8190` 字节
- 插入数据时，将数据追加到空闲空间起始位置，并更新 `FreeSpaceOffset`

### 3.2 DataItem

> 源码：`backend/dm/DataItem.go`

DataItem 是数据管理器中最基本的读写单元，格式如下：

```
DataItem 格式：
┌──────────────┬──────────────┬───────────────────┐
│ ValidFlag 1B │ DataSize 2B  │   Data (nB)       │
└──────────────┴──────────────┴───────────────────┘
```

- **ValidFlag**：`0` 表示有效，`1` 表示已被删除/无效
- **DataSize**：2 字节 BigEndian 编码的数据长度
- **Data**：实际数据内容

**UID 编码**：DataItem 的唯一标识符（8 字节）由页号和页内偏移组合而成：

```
UID 格式（8 字节 = int64）：
┌──────────────┬──────────────┬──────────────┐
│ PageNumber   │  (空 2 字节)  │   Offset     │
│   4 bytes    │   2 bytes    │   2 bytes    │
└──────────────┴──────────────┴──────────────┘
```

```go
type DataItem struct {
    raw         []byte           // 当前数据（含头部）
    oldRaw      []byte           // 修改前的备份，用于回滚
    lock        sync.RWMutex     // 读写锁
    dataManager *DataManager     // 所属数据管理器
    uid         int64            // 唯一标识符
    page        *dmPage.Page     // 所在页
}
```

**事务安全修改**：DataItem 提供 `Before()` / `After()` / `UnBefore()` 三个方法来支持事务安全的数据修改：

- `Before()`：修改前保存数据快照到 `oldRaw`
- `After(xid)`：修改后记录 Update 日志
- `UnBefore()`：撤销修改，从 `oldRaw` 恢复

### 3.3 引用计数缓存

> 源码：`backend/common/AbstractCache.go`

SimpleDB 使用引用计数（而非传统 LRU）来管理缓存，确保正在被使用的资源不会被意外淘汰：

```go
type AbstractCache[T any] struct {
    cache       map[int64]T      // 资源缓存
    references  map[int64]int    // 引用计数
    getting     map[int64]bool   // 正在加载标记（防止重复加载）
    maxResource int              // 最大缓存数量
    count       int              // 当前缓存数量
    lock        commons.ReentrantLock
    iAbstractCache IAbstractCache[T]  // 回调接口
}
```

**工作流程**：

1. **Get（获取资源）**：
   - 若资源正在被其他协程加载，等待加载完成
   - 若资源已在缓存中，引用计数 +1 并返回
   - 若缓存已满，返回错误
   - 否则调用 `GetForCache()` 回调从磁盘加载，引用计数置为 1
2. **Release（释放资源）**：
   - 引用计数 -1
   - 当引用计数降为 0 时，调用 `ReleaseForCache()` 回调刷回磁盘，从缓存中移除

该缓存被 Page 缓存 (`PageCache`) 和 DataItem 缓存 (`DataManager`) 共同使用。

### 3.4 PageIndex

> 源码：`backend/dm/dmPageIndex/`

PageIndex 维护一个空闲空间索引，用于在插入数据时快速找到有足够空间的页：

```go
const (
    IntervalsNumber = 40                        // 将页空间分为 40 个区间
    IntervalSize    = PageSize / IntervalsNumber // 每个区间大小 ≈ 204 字节
)

type PageIndex struct {
    mu    commons.ReentrantLock
    lists [][]*PageInfo   // 二维数组：[区间编号][该区间内的页列表]
}

type PageInfo struct {
    PageNumber int32   // 页号
    FreeSpace  int32   // 可用空间大小
}
```

插入数据时，根据所需空间计算出最小区间编号，从该区间开始查找可用页。这避免了遍历所有页来寻找空间。

---

## 4. MVCC 多版本并发控制

### 4.1 Entry（版本链）

> 源码：`backend/vm/Entry.go`

每条记录被包装为一个 Entry，附加版本信息：

```
Entry 格式：
┌──────────────┬──────────────┬───────────────────┐
│  XMIN (8B)   │  XMAX (8B)  │     Data (nB)     │
└──────────────┴──────────────┴───────────────────┘
```

- **XMIN**：创建该记录的事务 ID
- **XMAX**：删除该记录的事务 ID（`0` 表示未被删除）

```go
type Entry struct {
    uid      int64
    dataItem *dm.DataItem
    vm       *VersionManager
}
```

删除操作通过 `SetXMax(xid)` 设置 XMAX 来实现逻辑删除，修改过程通过 `Before()`/`After()` 保证可恢复。

### 4.2 Transaction 与 Snapshot（快照）

> 源码：`backend/vm/Transaction.go`

```go
type Transaction struct {
    Xid         int64             // 事务 ID
    Level       int32             // 隔离级别：0=RC, 非0=RR
    SnapShot    map[int64]bool    // 快照：事务开始时的活跃事务集合
    Err         error             // 事务异常
    AutoAborted bool              // 是否因死锁被自动回滚
}
```

事务开始时，系统会记录当前所有活跃事务的 ID 到 `SnapShot` 中。该快照用于可重复读隔离级别下的可见性判断。

### 4.3 读已提交 (Read Committed)

> 源码：`backend/vm/Visibility.go` → `readCommitted()`

在 RC 隔离级别下，一条记录对当前事务可见的条件为：

```
可见条件（满足其一即可）：
1. XMIN == 当前事务 XID 且 XMAX == 0（自己创建且未删除）
2. XMIN 已提交 且（XMAX == 0 或 XMAX 未提交 或 XMAX == 当前 XID）
```

即：自己创建的未删除记录可见；或者已提交事务创建的、且尚未被提交的事务删除的记录可见。

### 4.4 可重复读 (Repeatable Read)

> 源码：`backend/vm/Visibility.go` → `repeatableRead()`

在 RR 隔离级别下，额外考虑快照信息：

```
可见条件（满足其一即可）：
1. XMIN == 当前事务 XID 且 XMAX == 0
2. XMIN 已提交 且 XMIN < 当前 XID 且 XMIN 不在快照中
   且（XMAX == 0 或 XMAX != 当前 XID 且（XMAX 未提交 或 XMAX > 当前 XID 或 XMAX 在快照中））
```

关键区别：RR 要求创建事务必须在当前事务开始之前启动，且不能在快照中（即已经在当前事务开始前提交）。

### 4.5 版本跳跃检测 (Version Skip)

> 源码：`backend/vm/Visibility.go` → `IsVersionSkip()`

在 RR 隔离级别下，为了防止丢失更新问题，需要检测版本跳跃：

```go
// 当满足以下条件时，存在版本跳跃：
// XMAX 已提交 且（XMAX > 当前 XID 或 XMAX 在快照中）
```

如果一条记录已被另一个已提交事务修改，但该修改对当前事务不可见（因为在快照之后发生），则当前事务不能再修改这条记录，需要回滚以避免丢失更新。

### 4.6 两阶段锁与死锁检测

> 源码：`backend/vm/LockTable.go`

LockTable 实现了基于等待图 (Wait-For Graph) 的死锁检测：

```go
type LockTable struct {
    x2u      map[int64][]int64   // 事务 → 持有的资源列表
    u2x      map[int64]int64     // 资源 → 持有该资源的事务
    wait     map[int64][]int64   // 事务 → 等待获取该资源的事务列表
    waitU    map[int64]int64     // 事务 → 正在等待的资源
    xidStamp map[int64]int       // DFS 时间戳（用于环检测）
    stamp    int                 // 当前时间戳
}
```

**死锁检测算法（DFS）**：

1. 当事务请求的资源被其他事务持有时，加入等待图
2. 对等待图执行深度优先搜索
3. 使用时间戳标记访问状态：
   - 未标记：首次访问，标记当前时间戳并递归
   - 标记 == 当前时间戳：发现环，存在死锁
   - 标记 < 当前时间戳：之前搜索已排除，跳过
4. 检测到死锁时，将请求方事务回滚（Abort）

---

## 5. SQL 解析与执行

### 5.1 B+ 树索引

> 源码：`backend/im/BPlusTree.go`, `backend/im/Node.go`

SimpleDB 实现了单字段 B+ 树索引，每个有索引的字段对应一棵 B+ 树。

**核心参数**：

```go
BalanceNumber = 32   // 平衡因子，每个节点最多 64 个键值对（BalanceNumber × 2）
```

**节点格式**：

```
Node 格式（总计 1043 字节）：
┌────────────┬────────────────┬──────────────┬─────────────────────────────────┐
│ LeafFlag   │ KeyNumber      │ SiblingUid   │ [Son0][Key0][Son1][Key1]...     │
│  (1B)      │  (2B)          │  (8B)        │ 每对 16B (Son 8B + Key 8B)      │
└────────────┴────────────────┴──────────────┴─────────────────────────────────┘
  offset 0     offset 1        offset 3        offset 11
```

- **LeafFlag**：`1` 为叶子节点，`0` 为内部节点
- **KeyNumber**：当前节点的键数量
- **SiblingUid**：兄弟节点的 UID（用于范围查询的链表遍历）
- **Son/Key 数组**：每个 Son 是子节点 UID（内部节点）或数据 UID（叶子节点）

**核心操作**：

- **Search(key)**：从根节点递归下降到叶子节点，返回匹配的 UID 列表
- **SearchRange(leftKey, rightKey)**：定位起始叶子节点，沿兄弟链表顺序扫描
- **Insert(key, uid)**：递归插入，当节点达到 64 个键时进行分裂
  - 分裂时，后半部分（32 个键）移到新节点
  - 更新兄弟链表指针
  - 将新节点的第一个键和 UID 返回给父节点

### 5.2 SQL 解析器

> 源码：`backend/parser/Parser.go`, `backend/parser/Tokenizer.go`

#### Tokenizer（词法分析）

Tokenizer 将 SQL 字符串分解为 Token 序列：

- **符号**：`=`, `<`, `>`, `*`, `(`, `)`, `,`
- **引号字符串**：用引号包裹的字符串常量
- **标识符/关键字**：字母数字组成的标记

#### Parser（语法分析）

支持以下 SQL 语句：

| 语句 | 语法示例 |
|------|----------|
| `BEGIN` | `begin [isolation level (read committed\|repeatable read)]` |
| `COMMIT` | `commit` |
| `ABORT` | `abort` |
| `CREATE TABLE` | `create table <name> <field1> <type1> (index <field1>) ...` |
| `DROP TABLE` | `drop table <name>` |
| `INSERT` | `insert into <table> values <val1> <val2> ...` |
| `SELECT` | `select * from <table> [where <conditions>]` |
| `DELETE` | `delete from <table> [where <conditions>]` |
| `UPDATE` | `update <table> set <field>=<value> [where <conditions>]` |
| `SHOW` | `show` |

WHERE 子句支持单条件表达式和 `AND`/`OR` 逻辑运算。

### 5.3 表管理

> 源码：`backend/tbm/TableManager.go`, `backend/tbm/Table.go`, `backend/tbm/Field.go`

#### TableManager

```go
type TableManager struct {
    VM            *vm.VersionManager
    DM            *dm.DataManager
    booter        *Booter
    tableCache    map[string]*Table      // 表名 → 表对象缓存
    xidTableCache map[int64][]*Table     // 事务 ID → 该事务创建的临时表
}
```

TableManager 协调所有 SQL 操作的执行，管理表元数据的加载与缓存。

#### Table（表结构）

```go
type Table struct {
    TBM     *TableManager
    Uid     int64
    Name    string
    NextUid int64        // 链表指针，指向下一张表
    Fields  []*Field     // 字段列表
}
```

表的元数据以二进制格式持久化：`[TableName][NextTableUid][FieldUid1][FieldUid2]...`

表之间通过 `NextUid` 形成链表结构，Booter 文件（`.bt`）存储第一张表的 UID。

#### Field（字段定义）

```go
type Field struct {
    Uid       int64
    table     *Table
    FieldName string
    FieldType string      // 支持类型：int32, int64, string
    index     int64        // 索引 UID（0 表示无索引）
    bt        *im.BPlusTree // B+ 树索引实例
}
```

字段的二进制格式：`[FieldName][FieldType][IndexUid(8B)]`

**类型支持**：

| 类型 | Go 类型 | 索引键转换 |
|------|---------|-----------|
| `int32` | `int32` | 直接转为 `int64` |
| `int64` | `int64` | 直接使用 |
| `string` | `string` | 通过 `Str2Uid` 哈希为 `int64` |

#### Booter（引导文件）

> 源码：`backend/tbm/Booter.go`

Booter 管理 `.bt` 引导文件，存储第一张表的 UID（8 字节）。更新操作采用原子写入策略：先写入临时文件，删除旧文件，再重命名临时文件，确保崩溃安全。

### 5.4 执行器

> 源码：`backend/server/Executor.go`

Executor 负责 SQL 的路由与执行：

1. 调用 Parser 解析 SQL 语句
2. 根据语句类型路由到对应的 TableManager 方法
3. 对于非事务控制语句（SELECT、INSERT 等），自动包装临时事务（auto-transaction）
4. 返回执行结果或错误信息

---

## 6. 网络通信

> 源码：`transport/`, `client/`, `backend/server/`

### 6.1 传输层设计

SimpleDB 使用基于 TCP 的自定义协议进行客户端/服务器通信，默认端口 `9998`。

```
通信协议栈：
┌──────────────────────┐
│   Packager           │  ← 封装/解封 Package 对象
├──────────────────────┤
│   Encoder            │  ← 编码/解码数据和错误
├──────────────────────┤
│   Transporter        │  ← Hex 编码 + 行分隔传输
├──────────────────────┤
│   TCP Connection     │  ← net.Conn
└──────────────────────┘
```

#### Encoder（编码层）

> 源码：`transport/Encoder.go`

```
编码格式：
┌──────────┬──────────────────────────┐
│ Flag(1B) │        Data (nB)         │
└──────────┴──────────────────────────┘

Flag = 0x00 → 成功，Data 为返回数据
Flag = 0x01 → 错误，Data 为错误消息
```

#### Transporter（传输层）

> 源码：`transport/Transported.go`

使用 Hex 编码 + 换行符分隔的文本协议：

- **发送**：`hex.EncodeToString(data) + "\n"`
- **接收**：读取一行，`hex.DecodeString(line)`

#### Package（数据包）

> 源码：`transport/Package.go`

```go
type Package struct {
    Data []byte   // 数据内容
    Err  error    // 错误信息（若有）
}
```

### 6.2 服务端

> 源码：`backend/server/Server.go`

```go
type Server struct {
    port int
    tbm  *tbm.TableManager
}
```

服务端启动后监听 TCP 端口，为每个客户端连接启动独立的 goroutine 处理请求。每个连接的处理流程：

1. 创建 Transporter → Encoder → Packager
2. 循环接收 SQL → 通过 Executor 执行 → 返回结果

### 6.3 客户端

> 源码：`client/Client.go`, `client/Shell.go`, `client/RoundTripper.go`

客户端提供交互式 Shell 界面：

- **Shell**：读取用户输入的 SQL 语句
- **Client**：维护与服务端的连接
- **RoundTripper**：封装一次请求-响应往返

---

## 7. 项目架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Client (客户端)                              │
│                     client/Shell.go                                 │
│                     client/Client.go                                │
│                     client/RoundTripper.go                          │
└────────────────────────────┬────────────────────────────────────────┘
                             │ TCP (port 9998)
                             │ Hex 编码 + 行分隔协议
┌────────────────────────────▼────────────────────────────────────────┐
│                      Transport (传输层)                              │
│              transport/Packager.go  Encoder.go  Transported.go      │
└────────────────────────────┬────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────────┐
│                       Server (服务层)                                │
│              backend/server/Server.go  Executor.go                  │
│              SQL 路由 · 自动事务包装                                   │
└────────────────────────────┬────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────────┐
│                   Table Manager (表管理层)                           │
│          backend/tbm/TableManager.go  Table.go  Field.go            │
│          表元数据 · 字段类型 · SQL 执行协调                             │
│                                                                     │
│          ┌──────────────────────────────────┐                       │
│          │   Index Manager (索引管理)        │                       │
│          │   backend/im/BPlusTree.go         │                       │
│          │   B+ 树 · 单字段索引               │                       │
│          └──────────────────────────────────┘                       │
│          ┌──────────────────────────────────┐                       │
│          │   Parser (SQL 解析器)             │                       │
│          │   backend/parser/Parser.go        │                       │
│          │   词法分析 · 语法分析               │                       │
│          └──────────────────────────────────┘                       │
└────────────────────────────┬────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────────┐
│                  Version Manager (版本管理层)                        │
│         backend/vm/VersionManager.go  Visibility.go                 │
│         MVCC · 可见性判断 · 版本跳跃检测                               │
│                                                                     │
│         ┌──────────────────────────────────┐                        │
│         │   LockTable (锁管理)              │                        │
│         │   backend/vm/LockTable.go         │                        │
│         │   2PL · 等待图 · 死锁检测 (DFS)     │                        │
│         └──────────────────────────────────┘                        │
└────────────────────────────┬────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────────┐
│                   Data Manager (数据管理层)                          │
│           backend/dm/DataManager.go  DataItem.go  Recover.go        │
│           分页存储 · DataItem 读写 · WAL 崩溃恢复                      │
│                                                                     │
│  ┌────────────────┐ ┌────────────────┐ ┌────────────────────────┐   │
│  │  Page Cache    │ │  Page Index    │ │  Logger (日志管理)       │   │
│  │  dmPage/       │ │  dmPageIndex/  │ │  logger/DBLogger.go    │   │
│  │  8KB 页 · 校验  │ │  40 区间索引    │ │  校验和 · Redo/Undo    │   │
│  └────────────────┘ └────────────────┘ └────────────────────────┘   │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  AbstractCache (引用计数缓存)                                 │    │
│  │  backend/common/AbstractCache.go                             │    │
│  └─────────────────────────────────────────────────────────────┘    │
└────────────────────────────┬────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────────┐
│                Transaction Manager (事务管理层)                      │
│           backend/tm/TransactionManagerImpl.go                      │
│           XID 文件 · 事务状态持久化 · SuperXid                        │
└─────────────────────────────────────────────────────────────────────┘
```
