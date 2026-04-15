
# Apache Shenyu MCP (Model Context Protocol) 实现深度分析 (Master分支)

## 📋 文档信息
- **分析分支**: master
- **Shenyu版本**: 2.7.1-SNAPSHOT
- **分析日期**: 2026-04-15

---

## 1. 项目概览

### 1.1 项目定位
Apache Shenyu MCP 是 Apache Shenyu API 网关的一个核心模块，实现了 Model Context Protocol（模型上下文协议），使得 AI 模型可以通过标准化的 MCP 工具定义与 Shenyu 网关管理的微服务进行交互。

### 1.2 核心价值
- **协议标准化**: 遵循 MCP 协议规范，实现 AI 与微服务的标准化交互
- **双协议支持**: 同时支持 SSE (Server-Sent Events) 和 Streamable HTTP 两种传输协议
- **无缝集成**: 与 Shenyu 网关插件链完美集成，复用认证、限流、负载均衡等能力
- **动态工具管理**: 运行时动态添加/移除 MCP 工具
- **共享架构**: 多协议共享同一 MCP 服务器实例，工具自动跨协议可用

---

## 2. 技术栈详解

### 2.1 核心依赖

```xml
&lt;!-- Spring AI 核心 --&gt;
&lt;dependency&gt;
    &lt;groupId&gt;org.springframework.ai&lt;/groupId&gt;
    &lt;artifactId&gt;spring-ai-model&lt;/artifactId&gt;
&lt;/dependency&gt;

&lt;dependency&gt;
    &lt;groupId&gt;org.springframework.ai&lt;/groupId&gt;
    &lt;artifactId&gt;spring-ai-mcp&lt;/artifactId&gt;
&lt;/dependency&gt;

&lt;!-- MCP SDK (官方) --&gt;
&lt;dependency&gt;
    &lt;groupId&gt;io.modelcontextprotocol.sdk&lt;/groupId&gt;
    &lt;artifactId&gt;mcp-spring-webflux&lt;/artifactId&gt;
&lt;/dependency&gt;

&lt;dependency&gt;
    &lt;groupId&gt;io.modelcontextprotocol.sdk&lt;/groupId&gt;
    &lt;artifactId&gt;mcp-json-jackson2&lt;/artifactId&gt;
&lt;/dependency&gt;

&lt;!-- WebFlux 响应式框架 --&gt;
&lt;dependency&gt;
    &lt;groupId&gt;org.springframework.boot&lt;/groupId&gt;
    &lt;artifactId&gt;spring-boot-starter-webflux&lt;/artifactId&gt;
&lt;/dependency&gt;
```

### 2.2 SDK 兼容性说明

**pom.xml 中的重要注释** ([pom.xml](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/pom.xml#L54-L67)):
```xml
&lt;!--
    Spring AI and MCP SDK Dependencies

    SDK Compatibility Notes:
    - Current tested version: MCP SDK 0.17.0, Spring AI 1.1.2
    - McpSessionHelper uses reflection to access McpSyncServerExchange.exchange and
      McpAsyncServerExchange.session fields for session ID extraction
    - ShenyuStreamableHttpServerTransportProvider uses reflection to access
      McpServerSession.initialized and McpServerSession.state fields for session verification
    - When upgrading SDK versions, verify that these internal fields still exist and
      update McpSessionHelper.SUPPORTED_SDK_VERSION and any corresponding
      SDK compatibility documentation in ShenyuStreamableHttpServerTransportProvider
--&gt;
```

### 2.3 关键技术选型原因

| 技术 | 选型原因 |
|------|---------|
| Spring WebFlux | 响应式编程，高并发场景下的性能优势 |
| MCP SDK | 官方协议实现，确保协议兼容性 |
| Spring AI | 提供工具定义和回调的标准抽象 |
| Jackson2 | JSON 处理，与 MCP SDK 集成 |

---

## 3. 架构设计深度解析

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         AI Client / Model                              │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                      ┌──────────────┴──────────────┐
                      │                             │
         MCP (SSE)    │                             │  MCP (Streamable HTTP)
                      ▼                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     Apache Shenyu Gateway                              │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                    McpServerPlugin (入口)                         │  │
│  │  - 协议路由 (SSE / Streamable HTTP)                              │  │
│  │  - CORS 预处理                                                     │  │
│  │  - 会话上下文建立                                                 │  │
│  └───────────────────────────────────┬───────────────────────────────┘  │
│                                      │                                   │
│  ┌───────────────────────────────────▼───────────────────────────────┐  │
│  │              ShenyuMcpServerManager (核心管理器)                   │  │
│  │  ┌───────────────────────────────────────────────────────────────┐  │  │
│  │  │           CompositeTransportProvider                         │  │  │
│  │  │  ┌──────────────────────┐  ┌──────────────────────────┐  │  │  │
│  │  │  │ SSE Transport        │  │ Streamable HTTP Transport│  │  │  │
│  │  │  │ Provider             │  │ Provider                 │  │  │  │
│  │  │  └──────────────────────┘  └──────────────────────────┘  │  │  │
│  │  └───────────────────────────────────────────────────────────────┘  │  │
│  │  ┌───────────────────────────────────────────────────────────────┐  │  │
│  │  │           Shared McpAsyncServer (共享实例)                    │  │  │
│  │  │  - 工具管理 (addTool/removeTool)                             │  │  │
│  │  │  - 能力声明 (tools, logging)                                 │  │  │
│  │  └───────────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────┬───────────────────────────────┘  │
│                                      │                                   │
│  ┌───────────────────────────────────▼───────────────────────────────┐  │
│  │                  ShenyuToolCallback (工具回调)                    │  │
│  │  - 会话关联 (sessionId ↔ ServerWebExchange)                      │  │
│  │  - 请求装饰 (修改方法、路径、Header、Body)                        │  │
│  │  - 响应捕获 (ResponseDecorator)                                   │  │
│  └───────────────────────────────────┬───────────────────────────────┘  │
│                                      │                                   │
│  ┌───────────────────────────────────▼───────────────────────────────┐  │
│  │                 Shenyu Plugin Chain (插件链)                      │  │
│  │  (认证、限流、缓存、负载均衡、代理...)                             │  │
│  └───────────────────────────────────┬───────────────────────────────┘  │
└──────────────────────────────────────┼───────────────────────────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │   后端微服务           │
                          └─────────────────────────┘
```

### 3.2 核心设计模式

#### 3.2.1 组合模式 (Composite Pattern)
**位置**: [ShenyuMcpServerManager.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/manager/ShenyuMcpServerManager.java#L601-L754)

```java
private static class CompositeTransportProvider 
    implements io.modelcontextprotocol.spec.McpServerTransportProvider {
    
    private final Map&lt;String, Object&gt; transports = new ConcurrentHashMap&lt;&gt;();
    
    public void addTransport(String protocol, Object transportProvider) { ... }
    
    public ShenyuSseServerTransportProvider getSseTransport() { ... }
    
    public ShenyuStreamableHttpServerTransportProvider getStreamableHttpTransport() { ... }
    
    // 实现 McpServerTransportProvider 接口
    @Override
    public void setSessionFactory(McpServerSession.Factory sessionFactory) { ... }
    
    @Override
    public Mono&lt;Void&gt; notifyClients(String method, Object params) { ... }
    
    @Override
    public Mono&lt;Void&gt; closeGracefully() { ... }
}
```

**设计优势**:
- 透明支持多种传输协议
- 协议可以独立扩展
- 统一的会话工厂管理
- 广播通知自动转发到所有协议

#### 3.2.2 工厂方法模式 (Factory Method Pattern)
**位置**: [ShenyuMcpServerManager.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/manager/ShenyuMcpServerManager.java#L151-L167)

```java
private &lt;T&gt; T getOrCreateTransport(
    final String normalizedPath, 
    final String protocol, 
    final java.util.function.Supplier&lt;T&gt; transportFactory
) {
    CompositeTransportProvider compositeTransport = getOrCreateCompositeTransport(normalizedPath);
    
    T transport = switch (protocol) {
        case SSE_PROTOCOL -&gt; (T) compositeTransport.getSseTransport();
        case STREAMABLE_HTTP_PROTOCOL -&gt; (T) compositeTransport.getStreamableHttpTransport();
        default -&gt; null;
    };
    
    if (Objects.isNull(transport)) {
        transport = transportFactory.get();  // 工厂方法创建
        addTransportToSharedServer(normalizedPath, protocol, transport);
    }
    
    return transport;
}
```

#### 3.2.3 装饰器模式 (Decorator Pattern)
**位置**: 
- [ShenyuMcpResponseDecorator.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/response/ShenyuMcpResponseDecorator.java)
- [NonCommittingMcpResponseDecorator.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/response/NonCommittingMcpResponseDecorator.java)
- [BodyWriterExchange.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/request/BodyWriterExchange.java)

**用途**:
- 响应装饰器：捕获后端响应并通过 CompletableFuture 传回
- 请求装饰器：修改请求方法、路径、Header、Body
- BodyWriterExchange：注入自定义请求体

---

## 4. 核心模块深度剖析

### 4.1 McpServerPlugin - 入口插件

**文件位置**: [McpServerPlugin.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/McpServerPlugin.java)

#### 4.1.1 核心职责
1. **协议路由**: 根据 URI 路径路由到 SSE 或 Streamable HTTP 处理器
2. **CORS 预处理**: 处理 OPTIONS 预检请求
3. **会话建立**: 提取 sessionId 并建立会话上下文
4. **请求转发**: 转发到对应传输提供者

#### 4.1.2 关键流程 - doExecute()

```java
@Override
protected Mono&lt;Void&gt; doExecute(
    final ServerWebExchange exchange,
    final ShenyuPluginChain chain,
    final SelectorData selector,
    final RuleData rule
) {
    final ShenyuContext shenyuContext = exchange.getAttribute(Constants.CONTEXT);
    Objects.requireNonNull(shenyuContext, "ShenyuContext must not be null");

    final String uri = exchange.getRequest().getURI().getRawPath();

    // 关键改进：懒加载初始化
    if (!shenyuMcpServerManager.canRoute(uri)) {
        ShenyuMcpServer server = McpServerPluginDataHandler.CACHED_SERVER.get()
            .obtainHandle(selector.getId());
        if (Objects.nonNull(server)) {
            String serverPath = server.getPath();
            String messageEndpoint = server.getMessageEndpoint();
            // 同时初始化两种协议的传输
            shenyuMcpServerManager.getOrCreateMcpServerTransport(serverPath, messageEndpoint);
            shenyuMcpServerManager.getOrCreateStreamableHttpTransport(
                serverPath + STREAMABLE_HTTP_PATH
            );
        }
        // 再次检查是否可路由
        if (!shenyuMcpServerManager.canRoute(uri)) {
            return chain.execute(exchange);
        }
    }

    // 创建 ServerRequest
    final ServerRequest request = ServerRequest.create(exchange, messageReaders);

    // 路由到协议处理器
    return routeByProtocol(exchange, chain, request, selector, uri);
}
```

**设计亮点**:
- ✅ 懒加载初始化：首次请求时才创建传输提供者
- ✅ 双重协议初始化：同时创建 SSE 和 Streamable HTTP
- ✅ 容错处理：如果初始化失败，继续走插件链

#### 4.1.3 CORS 处理 - 重大改进

**新增方法**: [handleCorsPreflight()](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/McpServerPlugin.java#L315-L320)

```java
private Mono&lt;Void&gt; handleCorsPreflight(
    final ServerWebExchange exchange, 
    final String uri
) {
    exchange.getResponse().setStatusCode(HttpStatus.OK);
    setCorsHeaders(exchange, resolveAllowMethods(uri));
    exchange.getResponse().getHeaders().set("Access-Control-Max-Age", "3600");
    return exchange.getResponse().setComplete();
}
```

**增强的 CORS 配置**:

```java
private static final String CORS_ALLOW_METHODS = "GET, POST, OPTIONS";
private static final String CORS_STREAMABLE_ALLOW_METHODS = "POST, OPTIONS";
private static final String CORS_FALLBACK_ALLOW_HEADERS =
    "Content-Type, Mcp-Session-Id, Authorization, Last-Event-ID, " +
    "Mcp-Protocol-Version, X-Request, XRequest, xrequest";
```

**智能 CORS Header 解析**:

```java
private String resolveAllowHeaders(final ServerWebExchange exchange) {
    final Set&lt;String&gt; allowedHeaders = new LinkedHashSet&lt;&gt;();
    final String allowHeaders = Objects.nonNull(configuredCorsAllowHeaders) 
        &amp;&amp; !configuredCorsAllowHeaders.isBlank()
            ? configuredCorsAllowHeaders : CORS_FALLBACK_ALLOW_HEADERS;
    for (String header : allowHeaders.split(",")) {
        final String trimmed = header.trim();
        if (!trimmed.isEmpty()) {
            allowedHeaders.add(trimmed);
        }
    }
    return String.join(", ", allowedHeaders);
}

private void mergeVaryHeaders(final ServerWebExchange exchange) {
    final Set&lt;String&gt; varyValues = new LinkedHashSet&lt;&gt;();
    for (String varyHeader : exchange.getResponse().getHeaders()
            .getOrEmpty(HttpHeaders.VARY)) {
        for (String varyValue : varyHeader.split(",")) {
            final String trimmed = varyValue.trim();
            if (!trimmed.isEmpty()) {
                varyValues.add(trimmed);
            }
        }
    }
    varyValues.add("Origin");
    varyValues.add("Access-Control-Request-Headers");
    exchange.getResponse().getHeaders().setVary(List.copyOf(varyValues));
}
```

**改进点**:
1. ✅ 支持配置化的 CORS allowHeaders (通过 `shenyu.cross.allowedHeaders`)
2. ✅ 协议感知的 allowMethods (SSE 允许 GET/POST/OPTIONS，Streamable HTTP 只允许 POST/OPTIONS)
3. ✅ 智能 Origin 解析 (使用请求中的 Origin 而非通配符)
4. ✅ Vary Header 合并处理

#### 4.1.4 会话 ID 提取策略

```java
private String extractSessionId(final ServerWebExchange exchange) {
    // 优先级 1: 查询参数
    String sessionId = exchange.getRequest().getQueryParams().getFirst(SESSION_ID_PARAM);
    if (Objects.nonNull(sessionId)) {
        return sessionId;
    }

    // 优先级 2: Header - X-Session-Id
    // 优先级 3: Header - Mcp-Session-Id
    for (String headerName : SESSION_ID_HEADERS) {
        sessionId = exchange.getRequest().getHeaders().getFirst(headerName);
        if (Objects.nonNull(sessionId)) {
            return sessionId;
        }
    }

    // 优先级 4: Authorization Bearer Token
    final String authHeader = exchange.getRequest().getHeaders().getFirst(AUTHORIZATION_HEADER);
    if (Objects.nonNull(authHeader) &amp;&amp; authHeader.startsWith(BEARER_PREFIX)) {
        return authHeader.substring(BEARER_PREFIX.length());
    }

    return null;
}
```

### 4.2 ShenyuMcpServerManager - 核心管理器

**文件位置**: [ShenyuMcpServerManager.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/manager/ShenyuMcpServerManager.java)

#### 4.2.1 核心数据结构

```java
// 共享服务器映射: normalizedPath -&gt; McpAsyncServer
private final Map&lt;String, McpAsyncServer&gt; sharedServerMap = new ConcurrentHashMap&lt;&gt;();

// 路由映射: routePattern -&gt; HandlerFunction
private final Map&lt;String, HandlerFunction&lt;?&gt;&gt; routeMap = new ConcurrentHashMap&lt;&gt;();

// 组合传输映射: normalizedPath -&gt; CompositeTransportProvider
private final Map&lt;String, CompositeTransportProvider&gt; compositeTransportMap = new ConcurrentHashMap&lt;&gt;();
```

#### 4.2.2 路径规范化 - 关键改进

**位置**: [normalizeServerPath()](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/manager/ShenyuMcpServerManager.java#L478-L527)

```java
private String normalizeServerPath(final String path) {
    if (Objects.isNull(path)) {
        return null;
    }

    String normalizedPath = path.trim();
    if (normalizedPath.isEmpty()) {
        return "/";
    }

    // 步骤 1: 处理完整 URI (提取 path 部分)
    try {
        URI uri = URI.create(normalizedPath);
        if (Objects.nonNull(uri.getScheme())) {
            normalizedPath = uri.getRawPath();
        }
    } catch (IllegalArgumentException ignored) {
        // 不是完整 URI，保持原样
    }

    // 步骤 2: 基础规范化
    if (Objects.isNull(normalizedPath) || normalizedPath.isEmpty()) {
        normalizedPath = "/";
    }
    if (!normalizedPath.startsWith("/")) {
        normalizedPath = "/" + normalizedPath;
    }
    
    // 步骤 3: 移除查询参数和片段
    int queryStart = normalizedPath.indexOf('?');
    if (queryStart &gt;= 0) {
        normalizedPath = normalizedPath.substring(0, queryStart);
    }
    int fragmentStart = normalizedPath.indexOf('#');
    if (fragmentStart &gt;= 0) {
        normalizedPath = normalizedPath.substring(0, fragmentStart);
    }

    // 步骤 4: 移除重复斜杠
    normalizedPath = normalizedPath.replaceAll("/{2,}", "/");
    
    // 步骤 5: 移除通配符后缀
    if (normalizedPath.endsWith("/**")) {
        normalizedPath = normalizedPath.substring(0, normalizedPath.length() - "/**".length());
    }
    
    // 步骤 6: 移除协议特定后缀
    normalizedPath = removeSuffix(normalizedPath, "/message");
    normalizedPath = removeSuffix(normalizedPath, "/sse");
    normalizedPath = removeSuffix(normalizedPath, "/streamablehttp");

    // 步骤 7: 最终规范化
    if (normalizedPath.length() &gt; 1 &amp;&amp; normalizedPath.endsWith("/")) {
        normalizedPath = normalizedPath.substring(0, normalizedPath.length() - 1);
    }
    if (normalizedPath.isEmpty()) {
        return "/";
    }
    return normalizedPath;
}
```

**规范化示例**:

| 输入 | 输出 | 说明 |
|------|------|------|
| `http://example.com/mcp/sse?foo=bar` | `/mcp` | 提取 path，移除查询 |
| `/mcp//message//` | `/mcp` | 移除重复斜杠和后缀 |
| `/mcp/streamablehttp/**` | `/mcp` | 移除通配符和协议后缀 |
| `mcp/sse` | `/mcp` | 添加前导斜杠，移除后缀 |

#### 4.2.3 共享服务器创建

```java
private McpAsyncServer getOrCreateSharedServer(final String normalizedPath) {
    return sharedServerMap.computeIfAbsent(normalizedPath, path -&gt; {
        LOG.info("Creating shared MCP server for path: {}", path);

        // 获取或创建组合传输提供者
        CompositeTransportProvider compositeTransport = getOrCreateCompositeTransport(path);

        // 配置服务器能力
        var capabilities = McpSchema.ServerCapabilities.builder()
                .tools(true)           // 启用工具能力
                .logging()             // 启用日志能力
                .build();

        // 创建共享服务器 - 关键点：使用组合传输
        McpAsyncServer server = McpServer
                .async(compositeTransport)  // 使用 CompositeTransportProvider
                .serverInfo("MCP Shenyu Server (Multi-Protocol)", "1.0.0")
                .capabilities(capabilities)
                .tools(Lists.newArrayList())  // 初始空工具列表
                .build();

        LOG.info("Created shared MCP server for path: {} with multi-protocol support", path);
        return server;
    });
}
```

**架构优势**:
- ✅ 延迟创建：首次访问时才创建
- ✅ 线程安全：使用 `ConcurrentHashMap` 和 `computeIfAbsent`
- ✅ 共享实例：同一 normalizedPath 下的所有协议共享一个服务器
- ✅ 工具共享：添加的工具自动对所有协议可用

#### 4.2.4 工具管理

```java
public synchronized void addTool(
    final String serverPath, 
    final String name, 
    final String description,
    final String requestTemplate, 
    final String inputSchema
) {
    String normalizedPath = processPath(serverPath);

    // 先移除已存在的同名工具
    try {
        removeTool(serverPath, name);
    } catch (Exception ignored) {
        // ignore
    }

    // 创建工具定义
    ToolDefinition shenyuToolDefinition = ShenyuToolDefinition.builder()
            .name(name)
            .description(description)
            .requestConfig(requestTemplate)
            .inputSchema(inputSchema)
            .build();

    ShenyuToolCallback shenyuToolCallback = new ShenyuToolCallback(shenyuToolDefinition);

    // 添加到共享服务器
    McpAsyncServer sharedServer = sharedServerMap.get(normalizedPath);
    if (Objects.nonNull(sharedServer)) {
        try {
            for (AsyncToolSpecification asyncToolSpecification : 
                    McpToolUtils.toAsyncToolSpecifications(shenyuToolCallback)) {
                // 带超时的非阻塞添加
                sharedServer.addTool(asyncToolSpecification)
                        .timeout(Duration.ofSeconds(10))
                        .doOnSuccess(v -&gt; LOG.debug("Successfully added tool '{}'", name))
                        .doOnError(e -&gt; LOG.error("Failed to add tool '{}'", name, e))
                        .block();  // 阻塞等待完成
            }

            Set&lt;String&gt; protocols = getSupportedProtocols(normalizedPath);
            LOG.info("Added tool '{}' (available across protocols: {})", name, protocols);
        } catch (Exception e) {
            LOG.error("Failed to add tool '{}'", name, e);
            // 不抛出异常，避免影响其他工具
        }
    }
}
```

### 4.3 ShenyuToolCallback - 工具回调核心

**文件位置**: [ShenyuToolCallback.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/callback/ShenyuToolCallback.java)

#### 4.3.1 核心执行流程

```java
@Override
public String call(@NonNull final String input, final ToolContext toolContext) {
    // 步骤 1: 提取 MCP 会话
    final McpSyncServerExchange mcpExchange = extractMcpExchange(toolContext);
    final String sessionId = extractSessionId(mcpExchange);

    // 步骤 2: 验证工具定义
    final ShenyuToolDefinition shenyuTool = validateToolDefinition();
    final String configStr = extractRequestConfig(shenyuTool);

    // 步骤 3: 获取预存的 Exchange 和 PluginChain
    final ServerWebExchange originExchange = getOriginExchange(sessionId);
    final ShenyuPluginChain chain = getPluginChain(originExchange);

    // 步骤 4: 执行工具调用
    return executeToolCall(originExchange, chain, sessionId, configStr, input);
}
```

#### 4.3.2 请求装饰 - 核心实现

```java
private ServerWebExchange buildDecoratedExchange(
    final ServerWebExchange originExchange,
    final CompletableFuture&lt;String&gt; responseFuture,
    final String sessionId,
    final String configStr,
    final String input
) {
    // 解析输入和配置
    final JsonObject inputJson = parseInput(input);
    final RequestConfig requestConfig = buildRequestConfig(configStr, inputJson);

    // 构建装饰后的请求
    final ServerHttpRequest decoratedRequest = buildDecoratedRequest(
        originExchange, sessionId, requestConfig);

    // 构建响应装饰器（协议感知）
    final ServerHttpResponseDecorator responseDecorator = createResponseDecorator(
        originExchange, sessionId, responseFuture, configStr);

    // 创建基础装饰 Exchange
    final ServerWebExchange decoratedExchange = originExchange.mutate()
            .request(decoratedRequest)
            .response(responseDecorator)
            .build();

    // 处理请求体
    final ServerWebExchange finalExchange = handleRequestBody(decoratedExchange, requestConfig);

    // 配置 Shenyu 上下文
    configureShenyuContext(finalExchange, sessionId, requestConfig.getPath(), configStr);

    return finalExchange;
}
```

#### 4.3.3 参数映射 - 重大改进

**智能 Body 回退机制**:

```java
private RequestConfig buildRequestConfig(
    final String configStr, 
    final JsonObject inputJson
) {
    final RequestConfigHelper configHelper = new RequestConfigHelper(configStr);
    final JsonObject requestTemplate = configHelper.getRequestTemplate();
    final JsonObject argsPosition = configHelper.getArgsPosition();
    final String urlTemplate = configHelper.getUrlTemplate();
    final String method = configHelper.getMethod();
    final boolean argsToJsonBody = configHelper.isArgsToJsonBody();

    // 构建路径和查询参数
    final String path = RequestConfigHelper.buildPath(urlTemplate, argsPosition, inputJson);

    // 构建 Body
    JsonObject bodyJson = buildFormattedBodyJson(argsToJsonBody, argsPosition, inputJson);

    // 关键改进：智能回退
    // 如果 argsToJsonBody=false，且没有 body 映射参数，
    // 但方法需要请求体，且 inputJson 有内容，则使用整个 inputJson 作为 body
    if (!argsToJsonBody &amp;&amp; bodyJson.size() == 0 
            &amp;&amp; isRequestBodyMethod(method) &amp;&amp; inputJson.size() &gt; 0) {
        LOG.warn("Using fallback body mapping: " +
                "argsToJsonBody=false and no body-mapped args, " +
                "but method {} expects a request body. " +
                "Using full inputJson as request body.", method);
        bodyJson = inputJson.deepCopy();
    }

    return new RequestConfig(method, path, bodyJson, requestTemplate, 
            argsToJsonBody, inputJson);
}
```

**Header 模板变量解析**:

```java
private static final Pattern TEMPLATE_VARIABLE_PATTERN = Pattern.compile("\\{\\{\\.(.*?)\\}\\}");

private void addCustomHeaders(
    final ServerHttpRequest.Builder requestBuilder,
    final RequestConfig requestConfig
) {
    // ...
    JsonObject inputJson = requestConfig.getInputJson();
    for (JsonElement headerElem : headersArray) {
        // ...
        String headerKey = headerObj.get("key").getAsString();
        String headerValue = headerObj.get("value").getAsString();

        // 支持 {{.variableName}} 模板语法
        if (headerValue.contains("{{.") &amp;&amp; Objects.nonNull(inputJson)) {
            headerValue = resolveTemplateVariables(headerValue, inputJson);
        }

        requestBuilder.header(headerKey, headerValue);
    }
}

private String resolveTemplateVariables(
    final String templateValue, 
    final JsonObject inputJson
) {
    String result = templateValue;
    Matcher matcher = TEMPLATE_VARIABLE_PATTERN.matcher(templateValue);

    while (matcher.find()) {
        String variableName = matcher.group(1);
        if (inputJson.has(variableName)) {
            JsonElement element = inputJson.get(variableName);
            if (element.isJsonPrimitive()) {
                String value = element.getAsString();
                result = result.replace("{{." + variableName + "}}", value);
            }
        }
    }

    return result;
}
```

#### 4.3.4 SDK 兼容性错误处理

```java
private static final String SDK_COMPATIBILITY_ERROR_PREFIX = "SDK COMPATIBILITY ERROR";

private String extractSessionId(final McpSyncServerExchange mcpExchange) {
    try {
        final String sessionId = McpSessionHelper.getSessionId(mcpExchange);
        if (StringUtils.hasText(sessionId)) {
            return sessionId;
        }
        throw new IllegalStateException("Session ID is empty");
    } catch (RuntimeException e) {
        if (!isSdkCompatibilityError(e)) {
            throw e;
        }

        // SDK 兼容性错误的特殊处理
        throw new IllegalStateException(
                "Failed to extract session ID from MCP exchange. " +
                "This may indicate an SDK compatibility issue. " +
                "Tested SDK version: " + McpSessionHelper.getSupportedSdkVersion() + ". " +
                "Original error: " + e.getMessage(), e);
    }
}

private boolean isSdkCompatibilityError(final RuntimeException exception) {
    return exception instanceof IllegalStateException
            &amp;&amp; StringUtils.hasText(exception.getMessage())
            &amp;&amp; exception.getMessage().startsWith(SDK_COMPATIBILITY_ERROR_PREFIX);
}
```

---

## 5. 数据流程深度解析

### 5.1 完整的工具调用时序

```
┌─────────────┐
│ AI Client   │
└──────┬──────┘
       │ 1. POST /mcp/streamablehttp
       │    { jsonrpc: "2.0", method: "tools/call", ... }
       ▼
┌────────────────────────────────────────────────────────────┐
│ McpServerPlugin.doExecute()                                │
│  - 检测 Streamable HTTP 协议                               │
│  - 懒加载初始化传输提供者                                    │
│  - 提取 sessionId                                          │
│  - 保存 exchange 到 ShenyuMcpExchangeHolder                │
└──────┬─────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ ShenyuStreamableHttpServerTransportProvider                │
│  - 解析 JSON-RPC 请求                                      │
│  - 创建/获取 McpServerSession                              │
│  - 路由到 McpAsyncServer                                    │
└──────┬─────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ McpAsyncServer (MCP SDK)                                   │
│  - 验证 JSON-RPC 格式                                       │
│  - 查找工具定义                                              │
│  - 调用 ShenyuToolCallback.call()                          │
└──────┬─────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ ShenyuToolCallback.call()                                  │
│  ┌──────────────────────────────────────────────────────┐ │
│  │ 1. extractMcpExchange(toolContext)                  │ │
│  │    → 获取 McpSyncServerExchange                      │ │
│  ├──────────────────────────────────────────────────────┤ │
│  │ 2. extractSessionId(mcpExchange)                    │ │
│  │    → 通过反射获取 sessionId                          │ │
│  ├──────────────────────────────────────────────────────┤ │
│  │ 3. getOriginExchange(sessionId)                    │ │
│  │    → 从 ShenyuMcpExchangeHolder 获取                │ │
│  ├──────────────────────────────────────────────────────┤ │
│  │ 4. buildDecoratedExchange()                         │ │
│  │    ├─ parseInput(input)                             │ │
│  │    ├─ buildRequestConfig()                          │ │
│  │    ├─ buildDecoratedRequest()                      │ │
│  │    │   ├─ 修改 HTTP 方法                            │ │
│  │    │   ├─ 修改 URI 路径                              │ │
│  │    │   ├─ 解析 Header 模板变量                       │ │
│  │    │   └─ 设置 Content-Type                         │ │
│  │    ├─ createResponseDecorator()                    │ │
│  │    │   └─ 协议感知选择装饰器类型                     │ │
│  │    ├─ handleRequestBody()                          │ │
│  │    │   └─ BodyWriterExchange 注入请求体             │ │
│  │    └─ configureShenyuContext()                     │ │
│  │       ├─ 设置 path/realUrl                          │ │
│  │       ├─ 设置 MCP_TOOL_CALL=true (防循环)           │ │
│  │       └─ 设置 MCP_SESSION_ID                        │ │
│  └──────────────────────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────┐ │
│  │ 5. executeToolCall()                                │ │
│  │    ├─ 创建 CompletableFuture&lt;String&gt;               │ │
│  │    ├─ chain.execute(decoratedExchange)             │ │
│  │    │   └─ 异步订阅，不阻塞                          │ │
│  │    ├─ future.get(60, SECONDS)                      │ │
│  │    └─ 返回结果                                      │ │
│  └──────────────────────────────────────────────────────┘ │
└──────┬─────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ Shenyu Plugin Chain (异步执行)                            │
│  ┌──────────────────────────────────────────────────────┐ │
│  │ 跳过 MCP 插件 (检测到 MCP_TOOL_CALL=true)          │ │
│  ├──────────────────────────────────────────────────────┤ │
│  │ 认证插件 (Basic Auth/JWT/OAuth2)                    │ │
│  ├──────────────────────────────────────────────────────┤ │
│  │ 限流插件 (RateLimiter/Sentinel)                     │ │
│  ├──────────────────────────────────────────────────────┤ │
│  │ 缓存插件                                              │ │
│  ├──────────────────────────────────────────────────────┤ │
│  │ 负载均衡插件                                          │ │
│  ├──────────────────────────────────────────────────────┤ │
│  │ 代理插件 (Divide)                                     │ │
│  └──────────────────────────────────────────────────────┘ │
└──────┬─────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ 后端微服务                                                 │
│  - 处理业务逻辑                                            │
│  - 返回响应                                                │
└──────┬─────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ NonCommittingMcpResponseDecorator (Streamable HTTP)      │
│  - writeWith() 拦截响应                                    │
│  - 收集响应体                                              │
│  - responseFuture.complete(responseBody)                  │
└──────┬─────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ ShenyuToolCallback.executeToolCall()                      │
│  - future.get() 解除阻塞                                   │
│  - 返回响应字符串                                          │
└──────┬─────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ McpAsyncServer (MCP SDK)                                   │
│  - 包装 JSON-RPC 响应                                      │
└──────┬─────────────────────────────────────────────────────┘
       │
       ▼
┌────────────────────────────────────────────────────────────┐
│ ShenyuStreamableHttpServerTransportProvider                │
│  - 配置响应头 (Mcp-Session-Id, Content-Type)             │
│  - 写入 JSON-RPC 响应                                      │
└──────┬─────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────┐
│ AI Client   │
│  ← 响应     │
└─────────────┘
```

### 5.2 关键数据结构流转

#### 5.2.1 会话关联数据

```
┌─────────────────────────────────────────────────────────┐
│ ShenyuMcpExchangeHolder (静态持有器)                    │
├─────────────────────────────────────────────────────────┤
│ Map&lt;String, ServerWebExchange&gt; EXCHANGE_HOLDER        │
│                                                          │
│ "session_123" → ServerWebExchange (origin)             │
│ "session_456" → ServerWebExchange (origin)             │
│ "temp_789"  → ServerWebExchange (临时, 自动清理)        │
└─────────────────────────────────────────────────────────┘
```

#### 5.2.2 工具定义数据

```
ShenyuToolDefinition
├── name: "findOrderById"
├── description: "Find order by ID"
├── inputSchema: JSON Schema (参数定义)
└── requestConfig: JSON 字符串
    ├── requestTemplate
    │   ├── url: "/order/findById"
    │   ├── method: "GET"
    │   ├── headers: [...]
    │   ├── queryParams: [{key: "id", value: "${id}"}]
    │   └── timeout: 30000
    └── argsPosition
        └── id: "query"
```

---

## 6. 关键特性深度解析

### 6.1 多协议共享架构

**核心思想**: 同一服务器路径下的所有协议共享同一个 `McpAsyncServer` 实例

**实现细节**:

```
normalizedPath = "/mcp" (通过 normalizeServerPath() 计算)

sharedServerMap:
  "/mcp" → McpAsyncServer (共享实例)
              ↑
              │
              ├── CompositeTransportProvider
              │   ├── SSE Transport
              │   └── Streamable HTTP Transport
              │
              └── 工具列表 (自动跨协议共享)
                  ├── findOrderById
                  ├── saveOrder
                  └── ...
```

**优势**:
1. **资源高效**: 只创建一个 `McpAsyncServer`，节省内存
2. **工具同步**: 添加工具一次，所有协议立即可用
3. **能力统一**: 服务器能力声明只需要配置一次
4. **会话隔离**: 虽然服务器共享，但会话是按协议隔离的

### 6.2 懒加载初始化

**触发时机**: 首次请求到达时

**流程**:
```
1. 请求到达: POST /mcp/streamablehttp
   ↓
2. McpServerPlugin.doExecute() 检查:
   shenyuMcpServerManager.canRoute("/mcp/streamablehttp")
   → 返回 false (还未初始化)
   ↓
3. 从缓存获取 ShenyuMcpServer 配置
   ↓
4. 调用:
   shenyuMcpServerManager.getOrCreateMcpServerTransport("/mcp", "/message")
   → 创建 SSE Transport
   ↓
5. 调用:
   shenyuMcpServerManager.getOrCreateStreamableHttpTransport("/mcp/streamablehttp")
   → 创建 Streamable HTTP Transport
   ↓
6. 再次检查 canRoute() → 返回 true
   ↓
7. 处理请求
```

**优势**:
- 启动快: 不需要在网关启动时初始化所有 MCP 服务器
- 按需创建: 只创建实际使用的服务器
- 配置灵活: 可以动态添加新的 MCP 服务器

### 6.3 防循环调用机制

**问题**: MCP 工具调用会发起 HTTP 请求到网关，如果不拦截，会再次进入 MCP 插件，造成无限循环

**解决方案**:

```java
// McpServerPlugin.skip()
@Override
public boolean skip(final ServerWebExchange exchange) {
    // 检查标记
    final Boolean isMcpToolCall = exchange.getAttribute(MCP_TOOL_CALL_ATTR);
    if (Boolean.TRUE.equals(isMcpToolCall)) {
        LOG.debug("Skipping MCP plugin for tool call to prevent infinite loop");
        return true;  // 跳过 MCP 插件
    }
    return skipExcept(exchange, RpcTypeEnum.HTTP);
}

// ShenyuToolCallback.configureShenyuContext()
private void configureShenyuContext(...) {
    // ...
    // 设置标记
    decoratedExchange.getAttributes().put(MCP_TOOL_CALL_ATTR, true);
    // ...
}
```

**流程**:
```
1. 首次请求: POST /mcp/streamablehttp
   ↓
2. McpServerPlugin.doExecute() (不跳过, MCP_TOOL_CALL=null)
   ↓
3. ShenyuToolCallback 装饰 exchange
   - 设置 MCP_TOOL_CALL=true
   ↓
4. 执行插件链
   ↓
5. 再次经过 McpServerPlugin
   ↓
6. McpServerPlugin.skip() 检测到 MCP_TOOL_CALL=true
   → 返回 true, 跳过 MCP 插件
   ↓
7. 继续执行其他插件 (认证、限流、代理...)
```

### 6.4 协议感知的响应装饰器

**问题**: SSE 和 Streamable HTTP 需要不同的响应处理策略

**解决方案**:

```java
private ServerHttpResponseDecorator createResponseDecorator(
    final ServerWebExchange originExchange,
    final String sessionId,
    final CompletableFuture&lt;String&gt; responseFuture,
    final String configStr
) {
    final RequestConfigHelper configHelper = new RequestConfigHelper(configStr);
    final JsonObject responseTemplate = configHelper.getResponseTemplate();

    if (isStreamableHttpProtocol(originExchange)) {
        // Streamable HTTP: 使用 NonCommittingMcpResponseDecorator
        // 不提交响应，让传输层自己处理
        return new NonCommittingMcpResponseDecorator(
            originExchange.getResponse(), sessionId, responseFuture, responseTemplate);
    } else {
        // SSE: 使用 ShenyuMcpResponseDecorator
        // 标准装饰器
        return new ShenyuMcpResponseDecorator(
            originExchange.getResponse(), sessionId, responseFuture, responseTemplate);
    }
}
```

---

## 7. SDK 兼容性设计

### 7.1 背景
MCP SDK 仍在快速发展，内部 API 可能变化。Shenyu 使用反射访问 SDK 内部字段，需要特别处理兼容性。

### 7.2 反射使用点

| 类 | 字段 | 用途 |
|----|------|------|
| `McpSyncServerExchange` | `exchange` | 获取底层 exchange |
| `McpAsyncServerExchange` | `session` | 获取 session |
| `McpServerSession` | `initialized`, `state` | 验证会话状态 |

### 7.3 兼容性策略

**位置**: [McpSessionHelper.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/session/McpSessionHelper.java)

```java
public static String getSupportedSdkVersion() {
    return "0.17.0";  // 当前测试版本
}

public static String getSessionId(McpSyncServerExchange exchange) 
    throws NoSuchFieldException, IllegalAccessException {
    try {
        // 尝试通过反射获取
        Field sessionField = McpSyncServerExchange.class.getDeclaredField("session");
        sessionField.setAccessible(true);
        Object session = sessionField.get(exchange);
        
        // ... 获取 sessionId
    } catch (Exception e) {
        // 抛出带兼容性标记的异常
        throw new IllegalStateException(
            SDK_COMPATIBILITY_ERROR_PREFIX + 
            " - Failed to access session field. " +
            "Tested SDK version: " + getSupportedSdkVersion(),
            e
        );
    }
}
```

### 7.4 升级检查清单

当升级 MCP SDK 版本时，需要检查:

1. ✅ `McpSyncServerExchange.exchange` 字段是否存在
2. ✅ `McpAsyncServerExchange.session` 字段是否存在
3. ✅ `McpServerSession.initialized` 字段是否存在
4. ✅ `McpServerSession.state` 字段是否存在
5. ✅ 更新 `McpSessionHelper.SUPPORTED_SDK_VERSION`
6. ✅ 更新 pom.xml 中的兼容性注释
7. ✅ 运行集成测试验证

---

## 8. 性能优化考量

### 8.1 并发设计

| 组件 | 数据结构 | 线程安全 |
|------|---------|---------|
| sharedServerMap | ConcurrentHashMap | ✅ |
| routeMap | ConcurrentHashMap | ✅ |
| compositeTransportMap | ConcurrentHashMap | ✅ |
| transports (CompositeTransportProvider) | ConcurrentHashMap | ✅ |
| protocolSessions (CompositeTransportProvider) | Collections.synchronizedSet(HashSet) | ✅ |
| EXCHANGE_HOLDER (ShenyuMcpExchangeHolder) | ConcurrentHashMap | ✅ |

### 8.2 内存管理

**临时会话自动清理**:

```java
// 检查是否是临时会话
final boolean isTemporarySession = sessionId.startsWith("temp_");

// 执行完成后清理
.doFinally(signalType -&gt; {
    if (isTemporarySession) {
        LOG.debug("Cleaning up temporary session: {}", sessionId);
        ShenyuMcpExchangeHolder.remove(sessionId);
    }
})
```

**优雅关闭**:

```java
// CompositeTransportProvider.closeGracefully()
@Override
public Mono&lt;Void&gt; closeGracefully() {
    return Flux.fromIterable(transports.entrySet())
        .flatMap(entry -&gt; {
            if (transport instanceof McpServerTransportProvider) {
                return ((McpServerTransportProvider) transport)
                    .closeGracefully()
                    .onErrorComplete();  // 一个失败不影响其他
            }
            return Mono.empty();
        })
        .then()
        .doOnSuccess(aVoid -&gt; {
            transports.clear();
            protocolSessions.clear();
        });
}
```

### 8.3 超时控制

| 操作 | 超时设置 | 位置 |
|------|---------|------|
| 工具添加 | 10 秒 | ShenyuMcpServerManager.addTool() |
| 工具执行 | 60 秒 | ShenyuToolCallback.DEFAULT_TIMEOUT_SECONDS |

---

## 9. 错误处理策略

### 9.1 分层错误处理

```
1. ShenyuToolCallback.call()
   ├─ 捕获所有异常
   ├─ 记录错误日志
   └─ 包装为 RuntimeException 抛出
      ↓
2. McpAsyncServer (MCP SDK)
   ├─ 捕获工具回调异常
   └─ 转换为 JSON-RPC error 响应
      ↓
3. ShenyuStreamableHttpServerTransportProvider / ShenyuSseServerTransportProvider
   ├─ 处理传输层错误
   └─ 发送错误响应
```

### 9.2 容错策略

| 场景 | 策略 |
|------|------|
| 工具添加失败 | 记录日志，不抛出异常，不影响其他工具 |
| 工具移除失败 (工具不存在) | 检测并忽略 |
| 一个传输关闭失败 | `onErrorComplete()`，继续关闭其他传输 |
| 一个传输通知失败 | `onErrorComplete()`，继续通知其他传输 |
| SDK 兼容性错误 | 抛出带上下文信息的异常 |

---

## 10. 文件结构索引

### 10.1 核心文件

| 模块 | 文件 | 功能 |
|------|------|------|
| 插件入口 | [McpServerPlugin.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/McpServerPlugin.java) | 主插件类，协议路由，CORS处理 |
| 服务器管理 | [ShenyuMcpServerManager.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/manager/ShenyuMcpServerManager.java) | 共享服务器管理，传输组合，工具管理 |
| 工具回调 | [ShenyuToolCallback.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/callback/ShenyuToolCallback.java) | 工具执行，请求装饰，响应捕获 |
| 数据处理 | [McpServerPluginDataHandler.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/handler/McpServerPluginDataHandler.java) | 选择器/规则处理，工具注册 |
| 工具定义 | [ShenyuToolDefinition.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/definition/ShenyuToolDefinition.java) | Shenyu 特定的工具定义 |
| 会话持有 | [ShenyuMcpExchangeHolder.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/holder/ShenyuMcpExchangeHolder.java) | sessionId ↔ ServerWebExchange 映射 |
| 会话辅助 | [McpSessionHelper.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/session/McpSessionHelper.java) | SDK 反射，会话 ID 提取 |

### 10.2 传输层文件

| 文件 | 功能 |
|------|------|
| [ShenyuSseServerTransportProvider.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/transport/ShenyuSseServerTransportProvider.java) | SSE 协议传输 |
| [ShenyuStreamableHttpServerTransportProvider.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/transport/ShenyuStreamableHttpServerTransportProvider.java) | Streamable HTTP 协议传输 |
| [MessageHandlingResult.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/transport/MessageHandlingResult.java) | 消息处理结果 |
| [SseEventFormatter.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/transport/SseEventFormatter.java) | SSE 事件格式化 |

### 10.3 请求/响应处理文件

| 文件 | 功能 |
|------|------|
| [RequestConfig.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/request/RequestConfig.java) | 请求配置模型 |
| [RequestConfigHelper.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/request/RequestConfigHelper.java) | 请求配置解析 |
| [ParameterFormatter.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/request/ParameterFormatter.java) | 参数格式化 |
| [BodyWriterExchange.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/request/BodyWriterExchange.java) | 请求体注入 |
| [ShenyuMcpResponseDecorator.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/response/ShenyuMcpResponseDecorator.java) | SSE 响应装饰器 |
| [NonCommittingMcpResponseDecorator.java](file:///workspace/shenyu-plugin/shenyu-plugin-mcp-server/src/main/java/org/apache/shenyu/plugin/mcp/server/response/NonCommittingMcpResponseDecorator.java) | Streamable HTTP 响应装饰器 |

### 10.4 客户端文件

| 文件 | 功能 |
|------|------|
| [McpServiceEventListener.java](file:///workspace/shenyu-client/shenyu-client-mcp/shenyu-client-mcp-register/src/main/java/org/apache/shenyu/client/mcp/McpServiceEventListener.java) | 服务注册监听器 |
| [ShenyuMcpTool.java](file:///workspace/shenyu-client/shenyu-client-mcp/shenyu-client-mcp-common/src/main/java/org/apache/shenyu/client/mcp/common/annotation/ShenyuMcpTool.java) | MCP 工具注解 |
| [ShenyuMcpToolParam.java](file:///workspace/shenyu-client/shenyu-client-mcp/shenyu-client-mcp-common/src/main/java/org/apache/shenyu/client/mcp/common/annotation/ShenyuMcpToolParam.java) | MCP 工具参数注解 |

---

## 11. 总结

### 11.1 Master 分支的关键改进

相比之前的分支，master 分支有以下重要改进:

1. ✅ **增强的 CORS 处理**
   - 支持配置化 allowHeaders
   - 协议感知的 allowMethods
   - 智能 Origin 解析
   - Vary Header 合并

2. ✅ **智能参数映射**
   - Body 回退机制
   - Header 模板变量解析 (`{{.variableName}}`)
   - 更健壮的类型处理

3. ✅ **完善的路径规范化**
   - 7 步规范化流程
   - 支持完整 URI 输入
   - 更好的容错处理

4. ✅ **SDK 兼容性设计**
   - 明确的版本标记
   - 详细的兼容性注释
   - 友好的错误提示

5. ✅ **更好的错误处理**
   - 分层错误处理
   - 容错策略
   - 详细的日志记录

### 11.2 架构亮点

1. **多协议共享架构**: 资源高效，工具同步
2. **懒加载初始化**: 启动快，按需创建
3. **装饰器模式**: 请求/响应灵活处理
4. **组合模式**: 透明支持多协议
5. **会话关联**: sessionId ↔ ServerWebExchange 映射
6. **防循环调用**: 标记检测，安全可靠

### 11.3 技术栈版本

| 组件 | 版本 |
|------|------|
| Shenyu | 2.7.1-SNAPSHOT |
| Spring AI | 1.1.2 (测试版本) |
| MCP SDK | 0.17.0 (测试版本) |
| Spring Boot | 3.x (WebFlux) |
| Java | 17+ |

---

**文档结束**

*分析日期: 2026-04-15*
*分析分支: master*

