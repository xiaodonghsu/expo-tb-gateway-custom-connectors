# 海普发MQTT设备的连接器网关

## tb_gateway.json

在 网关的 connectors 中添加 CUSTOM connectors；

Name: Hyperfor MQTT Connector

Class: HyperforMqttConnector

在 tb_gateway.json 中会自动增加以下配置

```JSON
{"connectors": [
    {
      "type": "custom",
      "name": "Hyperfor MQTT Connector",
      "configuration": "hyperforMqttConnector.json",
      "class": "HyperforMqttConnector"
    },
    {
      "type": "custom",
      "name": "Bestlink IOT Request Connector",
      "configuration": "bestlinkIotGateway.json",
      "class": "bestlinkIOTGateway"
    },
  ]
}
```

## 在网关的 Connectors 的 configuration 中更新 config

## 将 Python 文件的拷贝到 extensions 中