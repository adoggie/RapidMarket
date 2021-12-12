
### RapidMarket   
数字货币快速行情服务系统

#### 1. svcMarket 
行情接入服务，提供行情kline，tick，orderbook等信息给报单系统和其他应用服务系统。

运行
> python -m elabs.app.svcMarket run --id=market01 

#### 2. svcTrader
报单服务，接收 行情服务提供的 tick，orderbook 和来自策略系统的仓位信号进行委托交易报单。

运行
> pythono -m elabs.app.svcTrade run --id=trade01