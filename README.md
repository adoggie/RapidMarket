
### Manager 
> 中心管理服务器，负责接收各业务服务的日志，派发状态查询，收集服务运行状态和报警消息。
> 

### KLogger
>行情Kline的接收服务，收到MX转发的kline，写入mongodb数据库

### KSeeker
> 行情Kline的读取查询服务，提供webapi 从mongodb中查询kline记录

### Registry
> 系统服务注册系统， 行情服务和报单服务运行时，登陆Registry服务器，获得运行配置信息。


### MX 消息路由系统

> 一级转发： xsub -> xpub 
> 行情服务通过mx 转发代理 将kline转发到 idc环境

<code>
"system_broker_addr": "tcp://127.0.0.1:15555",
  "position_broker_addr": "tcp://127.0.0.1:15555",
  "market_public_broker_addr": "tcp://127.0.0.1:15555",
  "market_local_broker_addr": "tcp://127.0.0.1:15558",
</code>

> 运行：  

一级消息路由
> python -m elabs.utils.mx-broker run_spbb tcp://*:15555 tcp://*:15556

二级中继路由

> python -m elabs.utils.mx-broker run_spcb tcp://127.0.0.1:15556 tcp://*:15557

订阅消息 (kline)
> python -m elabs.utils.mx-client do_sub tcp://127.0.0.1:15556

报单服务订阅消息(tick,orderbook,kline)
> python -m elabs.utils.mx-client do_sub tcp://127.0.0.1:15558