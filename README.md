# 基于 Thingsboard 的 Gateway 接入自定义的 connector

## 自定义海普发MQTT设备的连接器网关

### 安装 eqmx 服务器

该服务起到接收内网的网关设备的MQTT消息的作用(海普发设备不支持ThingsBoard的MQTT协议,需要MQTT转发接入TB)

本实例将eqmx 服务器安装在与服务端同一台服务下, 端口设置为 11883, 开通用户 bestlink/bestlink; 以及 GWGE100345/bestlink (根据海普发的网关设备确定)

### 安装和配置 GATEWAY

文件目录 [海普发MQTT设备的连接器网关配置](expo-hyperfor-gateway)

- 在 网关的 connectors 中添加 CUSTOM connectors；

Name: Hyperfor MQTT Connector

Class: HyperforMqttConnector

- 将 [Python 文件](expo-hyperfor-gateway\extensions\custom\hyperfor_mqtt_connector.py)的拷贝到网关的 extensions\custom 中

- 在网关的 Connectors 的 configuration 中更新 [config](expo-hyperfor-gateway\config\hyperforMqttConnector.json) 更新内容, 点击保存

## 自定义的外部设备REQUEST连接器网关

公司内部的门禁、空调已接入第三方系统，有管理功能, 该连接器连接第三方的门禁和空调设备。这些设备使用 API连接。

文件目录 [外部设备REQUEST连接器网关](expo-bestlink-iot-gateway)

- 在 网关的 connectors 中添加 CUSTOM connectors；

Name: Bestlink IOT Gateway Connector

Class: BestlinkIOTGatewayConnector

- 将 [Python 文件](expo-bestlink-iot-gateway\extensions\custom\bestlink_iot_request_connector.py) 拷贝到网关的 extensions\custom 中

- 在网关的 Connectors 的 configuration 中 使用 [config](expo-bestlink-iot-gateway\config\bestlinkIotGatewayConnector.json) 更新内容, 点击保存
