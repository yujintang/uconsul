### consul Node.js 版本
> 本sdk使用consul nodes 全节点注册,用于解决单agent导致的服务发现问题。

### Install
```shell
npm install uconsul
```

### 环境变量配置
```
DML_CONSUL_URL=
DML_CONSUL_NAMESPACE=
DML_CONSUL_TOKEN=
DML_CONSUL_CRON_EXPRESSION=
```

### Usage
```js
const consul = require('uconsul')

const main = async ()=>{
    // 初始化, 必须使用 await 
    await consul.init({
        DmlConsulUrl: "http://consul.xx.com",
        Namespace: "ns",
        Token: "token"
    })

    // 获取配置信息
    const kvInfo = await consul.KvInfo({
        Key: "/a/b/v1",
        JsonParse: true
    })
    if(kvInfo.RetCode != 0){
        throw new Error(kvInfo.Message)
    }
    console.log(kvInfo.Data.Value)

    // 注册
    const serviceAdd = await consul.ServiceAdd({
        Name: "test",
        Address: "192.168.1.2",
        Port: 1234,
        Check:{
            TCP: "192.168.1.2:1234",
            Interval: "1s",
            DeregisterCriticalServiceAfter: "1m"
        }

    })
    if(serviceAdd.RetCode != 0){
        throw new Error(serviceAdd.Message) 
    }
    console.log(serviceAdd.Data)
}

main()
```