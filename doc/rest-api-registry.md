
## 1. market-int
行情服务注册接口

```text
URL: /api/market/init
Method: POST
Parameters: ( x-form ) 
  - service_id 
  - service_type
  - timestamp 
  - pub_addr     行情发布地址 ，供 发单服务使用
  
Return: (json)
  - status : 0 /1 
  - result :  object / array 
  - errcode : 
  - errmsg : 

```
