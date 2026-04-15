# Shenyu MCP 协议实现深度技术分析

## 1. 概述

Shenyu MCP (Model Context Protocol) 是一个完整的 AI 微服务交互协议实现，基于 Spring WebFlux 响应式编程框架构建。该实现提供了两个传输协议支持：
- **SSE (Server-Sent Events)**: 标准 MCP 协议
- **Streamable HTTP**: 优化的 HTTP 流式传输协议

## 2. 整体架构设计

### 2.1 架构层次结构

```
┌─────────────────────────────────────────────────────────────┐
│                      MCP Client (AI/SDK)                    │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    McpServerPlugin                           │
│              (协议路由、CORS、Session初始化)                 │
└─────────────────────────────┬───────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐    ┌─────────────────────────┐
│ ShenyuSseServerTransport│    │ShenyuStreamableHttp      │
│  Provider               │    │ServerTransportProvider  │
│ (SSE 协议处理)          │    │ (Streamable HTTP 处理)   │
└─────────────────────────┘    └─────────────────────────┘
              │                               │
              └───────────────┬───────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              ShenyuMcpServerManager                          │
│           (共享服务器管理、工具注册、组合传输)                │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  McpAsyncServer (MCP SDK)                   │
│             (协议核心、Session管理、工具执行)                  │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│               ShenyuToolCallback                             │
│              (工具调用、请求转换、响应处理)                    │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│            Shenyu Plugin Chain / Upstream Services          │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 核心设计模式

1. **组合模式 (Composite Pattern)**: `CompositeTransportProvider` 组合多个传输协议
2. **工厂方法模式 (Factory Method)**: 传输提供者的懒加载创建
3. **装饰器模式 (Decorator Pattern)**: `ShenyuMcpResponseDecorator` 和 `NonCommittingMcpResponseDecorator`
4. **单例模式**: `ShenyuMcpExchangeHolder` 静态持有
5. **建造者模式 (Builder)**: `ShenyuSseServerTransportProvider.Builder` 等

## 3. 核心模块详解

### 3.1 插件入口模块 - McpServerPlugin

**位置**: [McpServerPlugin.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/McpServerPlugin.java)

#### 3.1.1 核心职责
- 协议路由 (SSE vs Streamable HTTP)
- CORS 跨域处理
- Session 上下文建立
- 懒加载初始化

#### 3.1.2 关键数据结构
```java
// 协议路径标识
private static final String MESSAGE_ENDPOINT = "/message";
private static final String STREAMABLE_HTTP_PATH = "/streamablehttp";
private static final String SSE_PATH = "/sse";

// Session 提取优先级
private static final String SESSION_ID_PARAM = "sessionId";
private static final String[] SESSION_ID_HEADERS = {
    "X-Session-Id", "Mcp-Session-Id"
};
```

#### 3.1.3 执行流程
```java
doExecute() 
  ├─ 检查是否可路由
  │   └─ 不可路由 → 懒加载初始化
  │       ├─ getOrCreateMcpServerTransport()
  │       └─ getOrCreateStreamableHttpTransport()
  ├─ 创建 ServerRequest
  └─ routeByProtocol()
      ├─ OPTIONS → handleCorsPreflight()
      ├─ Streamable HTTP → handleStreamableHttpRequest()
      └─ SSE → handleSseRequest()
```

#### 3.1.4 CORS 处理策略
```java
// 针对不同协议的允许方法
private static final String CORS_ALLOW_METHODS = "GET, POST, OPTIONS";           // SSE
private static final String CORS_STREAMABLE_ALLOW_METHODS = "POST, OPTIONS";    // Streamable HTTP

// 动态解析允许的 Headers
private String resolveAllowHeaders(ServerWebExchange exchange) {
    Set<String> allowedHeaders = new LinkedHashSet<>();
    String allowHeaders = Objects.nonNull(configuredCorsAllowHeaders) 
        ? configuredCorsAllowHeaders 
        : CORS_FALLBACK_ALLOW_HEADERS;
    // ...
}
```

### 3.2 核心管理器 - ShenyuMcpServerManager

**位置**: [ShenyuMcpServerManager.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/manager/ShenyuMcpServerManager.java)

#### 3.2.1 核心职责
- 共享服务器实例管理
- 路径规范化
- 工具注册/注销
- 组合传输提供者

#### 3.2.2 关键数据结构
```java
// 共享服务器映射 (路径 → McpAsyncServer)
private final Map<String, McpAsyncServer> sharedServerMap 
    = new ConcurrentHashMap<>();

// 路由映射 (路径 → HandlerFunction)
private final Map<String, HandlerFunction<?>> routeMap 
    = new ConcurrentHashMap<>();

// 组合传输映射 (路径 → CompositeTransportProvider)
private final Map<String, CompositeTransportProvider> compositeTransportMap 
    = new ConcurrentHashMap<>();
```

#### 3.2.3 7步路径规范化流程
```java
normalizeServerPath(path) {
    1. 去除空格
    2. URI 解析 (移除 scheme)
    3. 移除查询参数和片段
    4. 去除重复斜杠
    5. 移除 /** 模式
    6. 移除协议后缀 (/message, /sse, /streamablehttp)
    7. 规范化结尾斜杠
}
```

#### 3.2.4 组合传输提供者 - CompositeTransportProvider
这是一个内部类，实现了 `McpServerTransportProvider` 接口：

```java
private static class CompositeTransportProvider 
        implements McpServerTransportProvider {
    
    private final Map<String, Object> transports 
        = new ConcurrentHashMap<>();
    
    private volatile McpServerSession.Factory sessionFactory;
    
    // 支持的协议
    public Set<String> getSupportedProtocols() {
        return new HashSet<>(transports.keySet());
    }
    
    // 广播通知到所有传输
    @Override
    public Mono<Void> notifyClients(String method, Object params) {
        return Flux.fromIterable(transports.entrySet())
            .flatMap(entry -> {
                if (transport instanceof McpServerTransportProvider) {
                    return ((McpServerTransportProvider) transport)
                        .notifyClients(method, params)
                        .onErrorComplete();
                }
                return Mono.empty();
            })
            .then();
    }
}
```

#### 3.2.5 工具注册流程
```java
addTool(serverPath, name, description, requestTemplate, inputSchema)
  ├─ 规范化路径
  ├─ 先移除已存在的同名工具
  ├─ 创建 ShenyuToolDefinition
  ├─ 创建 ShenyuToolCallback
  └─ 调用 sharedServer.addTool()
      └─ McpToolUtils.toAsyncToolSpecifications()
```

### 3.3 传输层实现

#### 3.3.1 SSE 传输 - ShenyuSseServerTransportProvider

**位置**: [ShenyuSseServerTransportProvider.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/transport/ShenyuSseServerTransportProvider.java)

**核心特点**:
- 双端点架构: `/sse` (事件流) + `/message` (消息提交)
- SSE 事件类型: `endpoint` + `message`
- Session 生命周期管理

**SSE 连接建立流程**:
```
Client                    Server
  │                         │
  ├─ GET /sse ────────────→│
  │                         ├─ 创建 McpServerSession
  │                         ├─ 生成 sessionId
  │                         │
  │←─ event: endpoint ─────┤
  │    data: /message?sessionId=xxx
  │                         │
  │←─ (保持连接打开) ──────┤
  │                         │
  ├─ POST /message?sessionId=xxx
  │    body: JSON-RPC
  │                         ├─ 查找 Session
  │                         ├─ 处理消息
  │                         │
  │←─ 200 OK ──────────────┤
  │                         │
  │←─ event: message ──────┤
  │    data: JSON-RPC response
```

#### 3.3.2 Streamable HTTP 传输 - ShenyuStreamableHttpServerTransportProvider

**位置**: [ShenyuStreamableHttpServerTransportProvider.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/transport/ShenyuStreamableHttpServerTransportProvider.java)

**核心特点**:
- 单端点架构: `/streamablehttp` 处理所有操作
- Session 自动恢复机制
- 临时 Session 支持
- 初始化握手自动完成

**关键 Session 管理策略**:

1. **Initialize 请求**: 创建新 Session
2. **无 SessionId 请求**: 创建临时 Session，执行后清理
3. **无效 SessionId**: 创建新 Session，返回新的 SessionId
4. **丢失 Exchange 映射**: 重新绑定当前 Exchange

**Session 初始化流程 (内部)**:
```java
initializeSessionDirectly(session, sessionId)
  ├─ 创建模拟的 initialize 请求
  ├─ 调用 session.handle(initRequest)
  ├─ 捕获响应 (通过 transport)
  ├─ 发送 notifications/initialized 完成握手
  └─ 反射验证状态
```

### 3.4 工具回调 - ShenyuToolCallback

**位置**: [ShenyuToolCallback.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/callback/ShenyuToolCallback.java)

#### 3.4.1 核心职责
- 工具调用执行
- 请求参数转换
- 响应捕获和处理
- 防止无限循环

#### 3.4.2 工具调用流程
```java
call(input, toolContext)
  ├─ extractMcpExchange(toolContext)
  ├─ extractSessionId(mcpExchange)
  ├─ getOriginExchange(sessionId)  // 从 ShenyuMcpExchangeHolder
  ├─ getPluginChain(originExchange)
  └─ executeToolCall(...)
      ├─ buildDecoratedExchange()
      │   ├─ parseInput()
      │   ├─ buildRequestConfig()
      │   ├─ buildDecoratedRequest()
      │   ├─ createResponseDecorator()
      │   └─ handleRequestBody()
      ├─ chain.execute(decoratedExchange)
      └─ 等待 responseFuture (60秒超时)
```

#### 3.4.3 智能参数映射

**1. 路径/查询参数构建**:
```java
RequestConfigHelper.buildPath(urlTemplate, argsPosition, inputJson)
  ├─ 检查是否是完整 URL
  ├─ 处理路径参数 {{.paramName}}
  ├─ 处理查询参数
  └─ 合并已有查询参数
```

**2. Body 参数智能回退**:
```java
if (!argsToJsonBody && bodyJson.size() == 0 
        && isRequestBodyMethod(method) && inputJson.size() > 0) {
    // 智能回退: 使用完整的 inputJson 作为 Body
    bodyJson = inputJson.deepCopy();
}
```

**3. Header 模板变量解析**:
```java
// 格式: {{.variableName}}
if (headerValue.contains("{{.") && Objects.nonNull(inputJson)) {
    headerValue = resolveTemplateVariables(headerValue, inputJson);
}
```

**4. 参数格式化**:
```java
// 自动解析 JSON 字符串为对象/数组
JsonElement formattedValue = ParameterFormatter.tryParseJsonString(value);
```

#### 3.4.4 响应装饰器

**两种响应装饰器**:
1. **ShenyuMcpResponseDecorator**: 标准 SSE 协议
2. **NonCommittingMcpResponseDecorator**: Streamable HTTP 协议

关键区别在于 `NonCommittingMcpResponseDecorator` 不会立即提交响应，而是捕获响应后通过 HTTP 返回。

### 3.5 数据处理辅助模块

#### 3.5.1 请求配置 - RequestConfigHelper

**位置**: [RequestConfigHelper.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/request/RequestConfigHelper.java)

**配置结构示例**:
```json
{
  "requestTemplate": {
    "url": "/api/orders/{{.orderId}}",
    "method": "GET",
    "argsToJsonBody": false,
    "headers": [
      {"key": "Authorization", "value": "Bearer {{.token}}"}
    ]
  },
  "argsPosition": {
    "orderId": "path",
    "token": "header",
    "status": "query"
  },
  "responseTemplate": {...}
}
```

#### 3.5.2 Session 辅助 - McpSessionHelper

**位置**: [McpSessionHelper.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/session/McpSessionHelper.java)

**SDK 兼容性设计**:
- 使用反射访问 MCP SDK 内部字段
- 缓存 Field 引用提高性能
- 支持的 SDK 版本: 0.17.0
- 清晰的错误信息提示

```java
// 类初始化时解析反射字段
static {
    resolveReflectionFields();
}

// 使用缓存的字段
private static Field asyncExchangeFieldCache;
private static Field sessionFieldCache;
```

#### 3.5.3 Exchange 持有者 - ShenyuMcpExchangeHolder

**位置**: [ShenyuMcpExchangeHolder.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/holder/ShenyuMcpExchangeHolder.java)

```java
public final class ShenyuMcpExchangeHolder {
    private static final Map<String, ServerWebExchange> EXCHANGE_MAP 
        = new ConcurrentHashMap<>();
    
    public static void put(String sessionId, ServerWebExchange exchange)
    public static ServerWebExchange get(String sessionId)
    public static void remove(String sessionId)
}
```

### 3.6 数据处理模块

#### 3.6.1 插件数据处理器 - McpServerPluginDataHandler

**位置**: [McpServerPluginDataHandler.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/handler/McpServerPluginDataHandler.java)

**核心职责**:
- Selector 数据处理 → 创建 MCP 服务器
- Rule 数据处理 → 注册工具
- 缓存管理

```java
// 静态缓存
public static final Supplier<CommonHandleCache<String, ShenyuMcpServer>> CACHED_SERVER
public static final Supplier<CommonHandleCache<String, ShenyuMcpServerTool>> CACHED_TOOL

handlerSelector(selectorData)
  ├─ 提取 URI
  ├─ 规范化路径
  ├─ 缓存 ShenyuMcpServer
  └─ 初始化两种传输协议

handlerRule(ruleData)
  ├─ 解析 ShenyuMcpServerTool
  ├─ 缓存
  ├─ 生成 JSON Schema
  └─ 调用 shenyuMcpServerManager.addTool()
```

## 4. 完整数据流程

### 4.1 SSE 协议完整流程

```
时序图: SSE 协议流程

1. 连接建立
Client ──GET /sse──→ McpServerPlugin
                      │
                      ├─ routeByProtocol()
                      ├─ handleSseRequest()
                      ├─ ShenyuSseServerTransportProvider.createSseFlux()
                      │  ├─ 创建 McpServerSession
                      │  ├─ 发送 endpoint 事件
                      │  └─ 返回 Flux<ServerSentEvent>
                      │
Client ←─200 OK (SSE流)──

2. 消息发送
Client ──POST /message?sessionId=xxx──→ McpServerPlugin
                                        │
                                        ├─ handleSseRequest()
                                        ├─ handleMessageEndpoint()
                                        ├─ ShenyuSseServerTransportProvider.handleMessageEndpoint()
                                        │  ├─ 查找 Session
                                        │  ├─ 解析 JSON-RPC
                                        │  └─ session.handle(message)
                                        │      └─ McpAsyncServer 处理
                                        │
Client ←─200 OK──

3. 工具调用 (MCP 内部)
McpAsyncServer ──→ ShenyuToolCallback.call()
                  │
                  ├─ 从 ShenyuMcpExchangeHolder 获取 Exchange
                  ├─ 构建装饰后的 Exchange
                  ├─ 执行 Plugin Chain
                  ├─ 捕获响应
                  └─ 返回结果

4. 响应返回
Server (SSE) ──event: message──→ Client
              data: JSON-RPC
```

### 4.2 Streamable HTTP 协议完整流程

```
时序图: Streamable HTTP 协议流程

1. Initialize
Client ──POST /streamablehttp──→ McpServerPlugin
           body: initialize
                                 │
                                 ├─ handleStreamableHttpRequest()
                                 ├─ ShenyuStreamableHttpServerTransportProvider
                                 │  ├─ handleMessageEndpoint()
                                 │  ├─ handleInitializeRequest()
                                 │  │  ├─ 创建 Session
                                 │  │  ├─ 内部初始化 (模拟握手)
                                 │  │  └─ 返回 initialize 响应
                                 │
Client ←─200 OK──
           body: {"result": {"sessionId": "xxx", ...}}

2. 工具调用
Client ──POST /streamablehttp──→ McpServerPlugin
           header: Mcp-Session-Id: xxx
           body: tools/call
                                 │
                                 ├─ handleRegularRequestWithEnhancement()
                                 ├─ 查找/恢复 Session
                                 ├─ processWithExistingSession()
                                 │  ├─ session.handle(message)
                                 │  ├─ 执行工具 (同 SSE)
                                 │  └─ 等待 transport 响应
                                 │
Client ←─200 OK──
           body: JSON-RPC 响应
```

## 5. 关键技术特性

### 5.1 共享服务器架构

**创新点**: 多个传输协议共享同一个 `McpAsyncServer` 实例

**优势**:
- 工具只需注册一次，自动对所有协议可用
- 内存占用减少
- 状态一致性保证

**实现**:
```java
// ShenyuMcpServerManager.java
private McpAsyncServer getOrCreateSharedServer(String normalizedPath) {
    return sharedServerMap.computeIfAbsent(normalizedPath, path -> {
        CompositeTransportProvider compositeTransport 
            = getOrCreateCompositeTransport(path);
        
        return McpServer
            .async(compositeTransport)  // 传入组合传输
            .serverInfo(...)
            .capabilities(...)
            .tools(Lists.newArrayList())
            .build();
    });
}
```

### 5.2 懒加载初始化

**触发时机**: 第一次请求到达时

```java
// McpServerPlugin.java
if (!shenyuMcpServerManager.canRoute(uri)) {
    ShenyuMcpServer server = McpServerPluginDataHandler.CACHED_SERVER
        .get()
        .obtainHandle(selector.getId());
    
    if (Objects.nonNull(server)) {
        // 同时初始化两种协议
        shenyuMcpServerManager.getOrCreateMcpServerTransport(
            serverPath, messageEndpoint);
        shenyuMcpServerManager.getOrCreateStreamableHttpTransport(
            serverPath + STREAMABLE_HTTP_PATH);
    }
}
```

### 5.3 防无限循环机制

```java
// 1. Skip 机制
@Override
public boolean skip(ServerWebExchange exchange) {
    Boolean isMcpToolCall = exchange.getAttribute(MCP_TOOL_CALL_ATTR);
    if (Boolean.TRUE.equals(isMcpToolCall)) {
        return true;  // 跳过插件
    }
    return skipExcept(exchange, RpcTypeEnum.HTTP);
}

// 2. 设置标记
// ShenyuToolCallback.java
decoratedExchange.getAttributes().put(MCP_TOOL_CALL_ATTR, true);
```

### 5.4 Session 自动恢复 (Streamable HTTP)

三种恢复策略:
1. **无 SessionId**: 创建临时 Session
2. **无效 SessionId**: 创建新 Session，返回新 ID
3. **丢失 Exchange**: 重新绑定当前 Exchange

```java
handleRegularRequestWithEnhancement()
  ├─ 提取 requestedSessionId
  ├─ 无 → createTemporarySessionAndProcess()
  ├─ 无效 → createSessionAndRestoreId()
  └─ 有效 → 检查 Exchange
      ├─ 丢失 → 重新绑定
      └─ 存在 → 正常处理
```

### 5.5 SDK 兼容性设计

使用反射 + 缓存策略:

```java
// 类初始化时解析
static {
    resolveReflectionFields();
}

// 缓存字段引用
private static volatile Field asyncExchangeFieldCache;
private static volatile Field sessionFieldCache;

// 检查可用性
private static void checkReflectionAvailability() {
    if (!fieldsResolved) {
        throw new IllegalStateException(
            "SDK COMPATIBILITY ERROR: " +
            "Tested SDK version: " + SUPPORTED_SDK_VERSION);
    }
}
```

## 6. 性能优化考量

### 6.1 并发数据结构
- `ConcurrentHashMap` 替代 `HashMap`
- 无锁读取路径

### 6.2 反射优化
- 字段缓存 (只解析一次)
- 避免重复反射调用

### 6.3 路径匹配优化
```java
canRoute(uri) {
    // 1. 先尝试精确匹配 (O(1))
    if (routeMap.containsKey(uri)) {
        return true;
    }
    // 2. 再尝试 AntPathMatcher 模式匹配
    for (String pattern : routeMap.keySet()) {
        if (pathMatcher.match(pattern, uri)) {
            return true;
        }
    }
    return false;
}
```

### 6.4 响应式编程
- 全程使用 Reactor (Mono/Flux)
- 避免阻塞 Netty 线程
- 超时控制 (10秒工具添加，60秒工具执行)

## 7. 错误处理策略

### 7.1 分层错误处理
1. **传输层**: 协议解析错误 → 400 Bad Request
2. **Session 层**: Session 未找到 → 404 Not Found
3. **工具层**: 工具执行异常 → 500 Internal Error
4. **插件层**: 继续执行链或返回错误

### 7.2 优雅降级
- 工具添加失败不影响其他工具
- 一个传输失败不影响其他传输
- Session 初始化失败继续处理

## 8. 文件结构索引

### 核心插件
- [McpServerPlugin.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/McpServerPlugin.java) - 主插件入口

### 管理器
- [ShenyuMcpServerManager.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/manager/ShenyuMcpServerManager.java) - 核心管理器

### 传输层
- [ShenyuSseServerTransportProvider.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/transport/ShenyuSseServerTransportProvider.java) - SSE 传输
- [ShenyuStreamableHttpServerTransportProvider.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/transport/ShenyuStreamableHttpServerTransportProvider.java) - Streamable HTTP 传输

### 工具回调
- [ShenyuToolCallback.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/callback/ShenyuToolCallback.java) - 工具回调核心

### 数据处理
- [RequestConfigHelper.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/request/RequestConfigHelper.java) - 请求配置解析
- [McpServerPluginDataHandler.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/handler/McpServerPluginDataHandler.java) - 插件数据处理

### 辅助类
- [ShenyuToolDefinition.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/definition/ShenyuToolDefinition.java) - 工具定义
- [McpSessionHelper.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/session/McpSessionHelper.java) - Session 辅助
- [ShenyuMcpExchangeHolder.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/holder/ShenyuMcpExchangeHolder.java) - Exchange 持有

---

## 总结

Shenyu MCP 实现是一个设计精良、架构清晰的协议实现，具有以下突出特点:

1. **双协议支持**: SSE + Streamable HTTP，适应不同场景
2. **共享服务器**: 工具一次注册，多协议可用
3. **智能参数映射**: 灵活的参数位置配置 + 智能回退
4. **Session 高可用**: 自动恢复、临时 Session、重绑定
5. **SDK 兼容**: 反射 + 缓存，平滑应对 SDK 变更
6. **性能优化**: 并发数据结构、响应式编程、路径匹配优化

这份实现展示了企业级中间件的设计水准，兼顾了功能性、可扩展性、性能和可靠性。
