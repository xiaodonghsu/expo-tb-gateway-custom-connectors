网关 Connector RPC:

```LOG
tb-gateway  | 2026-07-14 00:06:26.706 - |INFO| - [hyperfor_mqtt_connector.py] - hyperfor_mqtt_connector - server_side_rpc_handler - 291 - Received RPC: {'method': 'custom_hello-command', 'params': {'command': 'hello-command', 'params': {'para': 0}, 'connectorId': '6a8912b8-a514-4775-ae38-04e1cee30332'}, 'id': '19'}
```

网关生成设备的 RPC

```LOG
tb-gateway  | 2026-07-14 00:05:38.324 - |INFO| - [hyperfor_mqtt_connector.py] - hyperfor_mqtt_connector - server_side_rpc_handler - 291 - Received RPC: {'device': 'F3-Light-North-test', 'data': {'id': 1, 'method': 'switch', 'params': {'status': 'off'}}, 'id': 1}
```