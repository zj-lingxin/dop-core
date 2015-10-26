DOP Service
===

## Feature

1. 实现了PV/UV等访问信息的采集
1. 实现了特殊业务（注册/绑店/申请 等）信息的采集
1. 实现了运营业务分析查询

# Core Design

1. 架构简洁，由于数据量不大，没有使用大数据方案，直接使用MySQL存储
1. 运行高效，考虑到PV采集可能存在的高并发，系统整体采用`异步非阻塞`的代码编写
1. 数据实时，对业务信息的采集使用Binlog监控，在业务数据变更时可以近实时地同步变更并使用WebSocket及时反馈
1. 结构简单，一个自启动Jar，无第三方依赖，业务模块间通过EventBus交互避免污染

## Evolution

1. 简化编码
* 实现类似RoR的路由规则表
* 实现通用DAO，支持ORMapping
* 实现服务脚手架，提供常用的CRUD服务
* 更简单的RX机制
1. 降低业务查询复杂度
* 将访问表做业务拆分
* 使用Redis做辅助处理

## How to Use

1. 使用`mvn package`打包，`target`中的`core-x-fat.jar`即为自启动包
1. 配置`resources`中的`config.json`
1. 启动
> 格式
>    java -jar core-xx-fat.jar \
>         -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory 
>         <-instances number> \
>         <-ha> \
>         <-hagroup group name> \
>         <-conf file> \
>         <-cp other classpath>\
> 示例
>    java -jar core-x-fat.jar \
>          -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory \
>          -conf C:\config.json
>          -cp .\config\

## How to Debug

1. Create `application`
1. parameters :
> Main Class : io.vertx.core.Starter
> Program arguments : run com.asto.dop.core.Startup -conf <some path>\config.json

## Test

所有测试代码位于`test`下，其中`funcion`包表示是功能点测试，`business`包表示业务点测试

目前测试覆盖率为：classes 65% lines 66%，所有核心逻辑均已覆盖

## Performance

随着数据量的增涨，可能出现的性能瓶颈：

1. 页面访问并发过大导致HTTP服务无响应
1. 页面访问并发过大导致采集处理过慢
1. 数据量过大导致查询分析过慢

解决方案（从易到难）

1. 为MySQL表增加组合索引
1. 使用多节点负载
1. 使用Redis缓存数据或将计数器迁入Redis
1. 分库分表
1. 使用大数据架构

## HA 示例（按如下命令启动多个）

    java -jar core-x-fat.jar \
         -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory \
         -ha \
         -hagroup dopcore \
         -conf <some path>\config.json


## 暂不能实现

一个cookie或device使用多个账号

## 其它

使用IP地址库：https://www.ipip.net/


## 进度

## 服务端
1. ~~数据采集 *（蒋震宇）*~~
* ~~浏览器访问数据采集 *（蒋震宇）*~~
* ~~APP访问数据采集 *（蒋震宇）*~~
* ~~停留时间数据采集 *（蒋震宇）*~~
* ~~API数据采集 *（蒋震宇）*~~
1. 查询分析
* ~~首页-实时指标 *（蒋震宇）*~~
* ~~首页-最近30天运营概况 *（蒋震宇）*~~
* ~~首页-流量分析 *（姚磊）*~~
* 首页-地域分布 *（姚磊）*
* 实时播报-实时概况 *（姚磊）*
* 实时播报-实时来源 *（姚磊）*
* ~~实时播报-实时访客 *（蒋震宇）*~~
* 经营分析-客户转化概况
* 经营分析-客户转化构成
* ~~经营分析-来源转化构成 *（蒋震宇）*~~
1. ~~源标识管理~~
* ~~源标识的CRUD *（蒋震宇）*~~

## 前端
* 交互查询开发 *（陈嘉楠，孙静）*

## 接口集成
* ~~浏览器访问数据接口 *（秦俊）*~~
* ~~停留时间数据接口 *（秦俊）*~~
* APP访问数据接口 *（孙鑫）*
* ~~API数据接口 *（李鹏程）*~~
* Binlog查询对接

## 测试
* 全流程测试 *（测试组）*
* 数据稽查 *（蒋震宇，姚磊）*
* 压力测试 *（测试组）*


