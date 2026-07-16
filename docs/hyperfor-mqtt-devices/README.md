# 海普发(hperfor) EPA 开关设备适配 Thingsboard 插件

## 背景

海普发设备的通信协议与ThingsBoard 差异较大：

- ThingsBoard不支持心跳包；海普发设备大约每20秒会有心跳包，收到心跳包应立即回应同样的消息，收不到消息就会重启。
- 海普发消息格式 from\epa\{{hperfor-clientid}} to\epa\{{hperfor-clientid}} 与 Thingsboard 的RPC不兼容

需要开发插件进行适配。

## 海普发设备消息格式

```JSON
{"bid": xxx
"mid": "消息ID",
....}
```

ThingsBoard发送RPC的格式为

```JSON
{"method": "method", "params": {}}
```

## 插件的原理

服务端采用开源的 MQTT 服务 —— EMQX-enterprise(http://192.168.4.244:18083/) 
EMQX 将 1883 端口映射到 11883端口; 海普发设备连接到这个端口；本程序作为桥接程序，也连到设备，并侦听所有设备的消息，将所有的消息转发到 ThingsBoard 的 MQTT 服务端，ThingsBoard 再将消息转发给海普发设备。

## EMQX的安装和配置

本地的1883已经被thingsboard 占用, 修改为11883端口

宿主机端口      容器端口        协议    用途    适用场景
1883    1883    TCP     标准 MQTT       物联网设备、内部应用（明文）
8883    8883    TCP (SSL/TLS)   安全 MQTT       公网设备、对安全要求高的应用（加密）
8083    8083    WS      WebSocket MQTT  浏览器应用（明文）
8084    8084    WSS     安全 WebSocket MQTT     浏览器应用（加密）
18083   18083   HTTP    Dashboard 管理控制台    管理员通过浏览器管理和监控 EMQX

```bash
docker pull emqx/emqx-enterprise:6.2.0

mkdir -p ~/hperfor-epa-plugin/emqx-data  # 数据库地址

sudo chown -R 1000:1000 ~/hperfor-epa-plugin/emqx-data

docker run -d --name emqx-enterprise -p 11883:1883 -p 8083:8083 -p 8084:8084 -p 8883:8883 -p 18083:18083 -v /home/jh/hperfor-epa-plugin/emqx-data:/opt/emqx/data/mnesia emqx/emqx-enterprise:6.2.0
```

## 海普发设备由公网环境转换到内网环境

海普发设备出厂默认与公网的MQTT服务端连接，参数如下：

### 海普发服务端参数

可以使用 MQTT.fx 等客户端连接，连接参数如下:

``` MQTT Broker Profile Settings
Broker Address: www.hperfor.com
Broker Port: 1883
ClientID: test

User Credentials:

UserName: ePa_CB
Password: 123456
```

### 海普发平台订阅主题

订阅接收消息的主题:
from/epa/{hperfor-ClientID}

订阅发送消息的主题(可选):
to/epa/{hperfor-ClientID}

发布消息：
to/epa/{hperfor-ClientID}

### 测试

MQTTX 按照以上参数配置, 添加订阅主题后,会定期收到 from/epa/xxxx

```JSON
{
    "bid": 101
}
```

## 独立ePa设备

独立设备， ethernet - 控制器

## epa_gateway 网关设备

网关设备，例如ethernet - 485网关下带控制器的情况


### 在内部服务器MQTT上添加设备

http://192.168.4.244:18083

![alt text](docs\{337AC956-3A3C-41F1-9710-514C085B837C}.png)

用户名为 {{hperfor-ClientID}}
密码为 bestlink

### 修改服务器为本地服务器

注意，测试表明，服务器如果直接使用内网的ip地址，会出现异常的情况，因此最好将服务器的地址通过域名方式映射。

使用 Publish 发送消息到 to/epa/{hperfor-ClientID}，例如：

#### 域名方式

```JSON
{
    "bid":321,"mid":"6F9619FF-8B86-D011-B42D-00C04FC964FF",
    "clientid":"JE1X600645",
    "host":"expo.i.uassist.cn",
    "dns": 1
    "port":"11883",
    "username":"JE1X600645",
    "password":"bestlink",
    "subscribe":"to/epa/JE1X600645",
    "publish":"from/epa/JE1X600645"
}
```

#### 本地IP方式

```JSON
{"bid":321,"mid":"6F9619FF-8B86-D011-B42D-00C04FC964FF",
"clientid":"JE1X600647",
"host":"192.168.4.244",
"dns":0,
"port":"11883",
"username":"JE1X600647",
"password":"bestlink",
"subscribe":"to/epa/JE1X600647",
"publish":"from/epa/JE1X600647"
}
```

### 修改后的测试

在MQTT.fx的添加服务器调整到
Broker Address: 192.168.4.244
Broker Port: 11883
Client ID: bestlink

User Name: bestlink
Password: bestlink

添加 Subscribe:
from/epa/JE1X600827
to/epa/JE1X600827

应该可以观察到周期性的 bid = 101 的消息

## 开发要求

插件使用 Python 开发，与ThingsBoard通信使用原生的python SDK: tb-mqtt-client

与 海普发 设备通信需要自定义 MQTT 客户端，使用 paho-mqtt 库实现。

### ThingsBoard 服务端参数

服务端: 192.168.4.244
mqtt port: 11883

设备列表需要可配置，例如:

```JSON
[{"name": "3F-Screen-Left",
"hperfor-ClientID": "JE1X600479",
"tb-DeviceToken": "GwuT3v0dmoPdoFo6raYF"},
{"name": "3F-Screen-Right",
"hperfor-ClientID": "JE1X600480"},
"tb-DeviceToken": "Bxpw42p5vGCvft8cjRAx"},]
```

## 海普发的主要功能和协议

### 心跳功能（101接口）

设备大约每20秒会有心跳包，收到该消息，回复同样的消息即可。

```JSON
{
    "bid": 101
}
```

### 设备状态发生变化时,会主动上报状态信息（201接口）

```JSON
{
    "SysTime": "2023-05-04 09:09:53",
    "bid": 201,
    "mid": "1C0593F0-80F1-E309-A8CE-7C3A1423DBC3",
    "Children": [{
            "ClientID": "JE1X600479",
            "Voltage": [0, 0, 0, 0],
            "Current": [0, 0, 0, 0],
            "Power": [0, 0, 0, 0],
            "Energy": [0, 0, 0, 0],
            "Temperature": [4288, 0, 0, 0],
            "ElectricStatus": 0,
            "SwitchStatus": 1,
            "Mode": 0
        }]
}
```

### 请求单个ePa设备数据（208接口）

#### 发布消息

```JSON
{
    "bid": 208,
    "mid": "6F9619FF-8B86-D011-B42D-00C04FC964FF",
    "Children": [
        {
            "ClientID": "JE1X600479"
        }
    ]
}
```

#### 收到消息

```JSON
{
    "SysTime": "2023-05-04 09:08:33",
    "bid": 208,
    "mid": "6F9619FF-8B86-D011-B42D-00C04FC964FF",
    "Children": [{
            "ClientID": "JE1X600479",
            "Voltage": [0, 0, 0, 0],
            "Current": [0, 0, 0, 0],
            "Power": [0, 0, 0, 0],
            "Energy": [0, 0, 0, 0],
            "Temperature": [4288, 0, 0, 0],
            "ElectricStatus": 0,
            "SwitchStatus": 0,
            "Mode": 0
        }]
}
```

### 开关开合闸（202接口）

MoterOperation： value=1->关  value=2->开 

#### 发布消息

```JSON
{
    "bid": 202,
    "mid": "6F9619FF-8B86-D011-B42D-00C04FC964FF",
    "Children": [
        {
            "ClientID": "JE1X600479",
            "MoterOperation": 2
        }
    ]
}
```

#### 收到的正确消息

```JSON
{
    "SysTime":    "2022-02-14 08:34:55",
    "bid":    202,
    "mid":    "6F9619FF-8B86-D011-B42D-00C04FC964FF",
    "Children":    [{
            "ClientID":    "JR1X400278",
            "Voltage":    [0, 0, 0, 0],
            "Current":    [0, 0, 0, 0],
            "Power":    [0, 0, 0, 0],
            "Energy":    [0, 0, 0, 0],
            "Temperature":    [3122, 0, 0, 0],
            "ElectricStatus":    0,
            "SwitchStatus":    0,
            "Mode":    0,
            "UsartComm":    0
        }, {
            "ClientID":    "JR1X400279",
            "Voltage":    [0, 0, 0, 0],
            "Current":    [0, 0, 0, 0],
            "Power":    [0, 0, 0, 0],
            "Energy":    [0, 0, 0, 0],
            "Temperature":    [3250, 0, 0, 0],
            "ElectricStatus":    0,
            "SwitchStatus":    0,
            "Mode":    0,
            "UsartComm":    0
        }]
}
```

#### 收到的错误的消息

message 的 ID 代表
12 - 开关处于合闸状态(下发开命令时,开关处于合闸状态,无法执行开命令)
13 - 开关处于分闸状态(下发关命令时,开关处于分闸状态,无法执行关命令)

```JSON
{
    "bid": 202,
    "mid": "6F9619FF-8B86-D011-B42D-00C04FC964FF",
    "result": false,
    "message": 13
}
```

## Thingsboard 主要功能

### 状态更新

收到海普发101接口消息,根据 ClientID 发送 telemetry 数据到 Thingsboard

```JSON
{
    "heartbeat": 当前时间
}
```

收到202/208 消息, 将消息根据 ClientID，分发到 thingsboard 对应的 deviceToken 设备。

### RPC命令

Thingsboard 接收平台的RPC命令，根据 ClientID 分发到海普发设备。

#### 开关开合闸

```JSON
{
    "method": "switch",
    "params": {
        "status": "on"/"off"
    }
}
```

执行 `switch` 时会读取对应设备配置中的 `delay_on` / `delay_off`：

- `status=on` 使用 `delay_on`
- `status=off` 使用 `delay_off`
- 未配置时默认均为 `0`，即立即执行

#### 请求单个ePa设备数据（208接口）

```JSON
{
    "method": "get_data",
    "params": {
    }
}
```

## 当前实现说明

当前仓库已经补齐了一个可运行的 Python 桥接程序，入口文件为 `main.py`，主要能力包括：

- 读取本地 `config.json` 配置，并支持 README 中示例字段名
- 使用 `paho-mqtt` 连接海普发 MQTT 服务端
- 为每个 ThingsBoard 设备 token 建立独立的 `tb-mqtt-client` 连接
- 订阅 `from/epa/{hperfor-ClientID}` 主题并处理 101/201/202/208 消息
- 收到 101 心跳后原样回应，并向对应 ThingsBoard 设备上报 `heartbeat`
- 收到 201/202/208 消息后，按 `ClientID` 分发到对应 ThingsBoard 设备并上报 telemetry
- 接收 ThingsBoard 服务端 RPC，并映射为海普发 202/208 指令下发到 `to/epa/{hperfor-ClientID}`

## 配置方式

先复制示例配置文件：

```powershell
Copy-Item config.example.json config.json
```

然后按实际环境修改 `config.json`。

也可以通过环境变量指定配置文件路径：

```powershell
$env:HPERFOR_PLUGIN_CONFIG = "config.json"
```

## 运行方式

安装依赖：

```shell
uv sync
```

启动程序：

```shell
uv run python main.py
```

如果需要查看更多日志：

```powershell
$env:LOG_LEVEL = "DEBUG"
uv run python main.py
```

## Docker 运行

项目现在可以直接构建为 Docker 镜像运行。

构建和推送镜像：

```shell
docker build -t hperfor-epa-plugin .

docker tag hperfor-epa-plugin nuc10.i.uassist.cn:5000/hperfor-epa-plugin:latest

docker push nuc10.i.uassist.cn:5000/hperfor-epa-plugin:latest
```

下载和运行镜像

```shell

docker stop hperfor-epa-plugin
docker rm hperfor-epa-plugin
docker pull nuc10.i.uassist.cn:5000/hperfor-epa-plugin:latest

cd ~/hperfor-epa-plugin

docker run -d \
  --name hperfor-epa-plugin \
  -v ${PWD}/data/config:/data/config \
  -v ${PWD}/data/logs:/data/logs \
  nuc10.i.uassist.cn:5000/hperfor-epa-plugin:latest

docker logs -f hperfor-epa-plugin

```

容器内默认约定：

- 配置文件路径：`/data/config/config.json`
- 日志文件路径：`/data/logs/hperfor-epa-plugin.log`
- 日志按天切分：每天午夜生成一个新的日志文件，默认保留 30 天历史日志

推荐先在宿主机准备目录，并把 [config.example.json](/C:/Files/Documents/Project/playground-bestlink-expo/hperfor-epa-plugin/config.example.json) 复制成实际配置文件：

```shell
New-Item -ItemType Directory -Force -Path .\data\config, .\data\logs
Copy-Item .\config.example.json .\data\config\config.json
```

运行容器：

```shell
docker run -d `
  --name hperfor-epa-plugin `
  -v ${PWD}\data\config:/data/config `
  -v ${PWD}\data\logs:/data/logs `
  hperfor-epa-plugin
```

如果你想自定义配置文件或日志文件位置，也可以覆盖环境变量：

```shell
docker run -d `
  --name hperfor-epa-plugin `
  -e HPERFOR_PLUGIN_CONFIG=/data/config/config.json `
  -e LOG_FILE=/data/logs/custom.log `
  -e LOG_BACKUP_COUNT=30 `
  -e LOG_LEVEL=DEBUG `
  -v ${PWD}\data\config:/data/config `
  -v ${PWD}\data\logs:/data/logs `
  hperfor-epa-plugin
```
