```bash
docker run -d \
  --name hperfor-epa-plugin \
  -v ${PWD}/data/config:/data/config \
  -v ${PWD}/data/logs:/data/logs \
  nuc10.i.uassist.cn:5000/hperfor-epa-plugin:latest
```

## hperfor 本地代理

本地的1883已经被thingsboard 占用, 修改为11883端口

宿主机端口	容器端口	协议	用途	适用场景
1883	1883	TCP	标准 MQTT	物联网设备、内部应用（明文）
8883	8883	TCP (SSL/TLS)	安全 MQTT	公网设备、对安全要求高的应用（加密）
8083	8083	WS	WebSocket MQTT	浏览器应用（明文）
8084	8084	WSS	安全 WebSocket MQTT	浏览器应用（加密）
18083	18083	HTTP	Dashboard 管理控制台	管理员通过浏览器管理和监控 EMQX



```bash
docker pull emqx/emqx-enterprise:6.2.0

mkdir -p /home/jh/hperfor-epa-plugin/emqx-data  # 数据库地址

sudo chown -R 1000:1000 /home/jh/hperfor-epa-plugin/emqx-data

docker run -d --name emqx-enterprise -p 11883:1883 -p 8083:8083 -p 8084:8084 -p 8883:8883 -p 18083:18083 -v /home/jh/hperfor-epa-plugin/emqx-data:/opt/emqx/data/mnesia emqx/emqx-enterprise:6.2.0
```
