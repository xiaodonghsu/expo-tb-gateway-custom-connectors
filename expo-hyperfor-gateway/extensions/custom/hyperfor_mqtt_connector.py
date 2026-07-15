import json
import queue
import time
from threading import Event

import paho.mqtt.client as mqtt

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.tb_utility.tb_logger import init_logger

import uuid
import threading
from datetime import datetime, timezone

class HyperforMqttConnector(Connector):
    """
    接收包含 Children 数组的 MQTT 消息，
    将每个 Children 元素映射为一个 ThingsBoard 子设备。
    """

    def __init__(self, gateway, config, connector_type):
        self.__gateway = gateway
        self.__config = config
        self._connector_type = connector_type

        self.__id = config.get("id")
        self.__name = config.get("name", "Hyperfor MQTT Connector")

        self.__stopped = Event()
        self.__connected = False

        self.__log = init_logger(
            gateway,
            self.__name,
            config.get("logLevel", "INFO"),
            enable_remote_logging=config.get("enableRemoteLogging", False),
            is_connector_logger=True
        )

        broker = config.get("broker", {})

        self.__broker_host = broker.get("host", "localhost")
        self.__broker_port = int(broker.get("port", 1883))
        self.__broker_username = broker.get("username")
        self.__broker_password = broker.get("password")
        self.__client_id = broker.get(
            "clientId",
            "tb-gateway-children-connector"
        )

        self.__topic_filter = config.get("topicFilter", "from/epa/+")
        self.__device_profile = config.get("deviceProfile", "default")
        self.__devices = config.get("devices", {})
        self.__rpc_method_maps = config.get("rpcMethodMaps", {})
        self.__rpcs = config.get("rpcs", {})
        command_queue_config = config.get("rpcCommandQueue", {})
        self.__command_min_interval = max(0.0, float(command_queue_config.get("minIntervalSeconds", 1.0)))
        self.__command_response_timeout = max(0.0, float(command_queue_config.get("responseTimeoutSeconds", 5.0)))
        self.__wait_for_command_response = bool(command_queue_config.get("waitForResponse", True))
        self.__command_queues = {}
        self.__command_workers = {}
        self.__command_timers = []
        self.__command_lock = threading.Lock()
        self.__pending_command_responses = {}
        self.__mqtt_client = None

    def open(self):
        """Gateway 启动 Connector 时调用。"""

        self.__stopped.clear()

        self.__mqtt_client = mqtt.Client(
            client_id=self.__client_id
        )

        if self.__broker_username:
            self.__mqtt_client.username_pw_set(
                self.__broker_username,
                self.__broker_password
            )

        self.__mqtt_client.on_connect = self.__on_connect
        self.__mqtt_client.on_disconnect = self.__on_disconnect
        self.__mqtt_client.on_message = self.__on_message

        self.__log.info(
            "Connecting to MQTT broker %s:%s",
            self.__broker_host,
            self.__broker_port
        )

        self.__mqtt_client.connect(
            self.__broker_host,
            self.__broker_port,
            keepalive=60
        )

        self.__mqtt_client.loop_start()

    def close(self):
        """Gateway 停止 Connector 时调用。"""

        self.__stopped.set()
        self.__connected = False

        with self.__command_lock:
            command_queues = list(self.__command_queues.values())
            command_workers = list(self.__command_workers.values())
            command_timers = list(self.__command_timers)
            pending_responses = list(self.__pending_command_responses.values())
            self.__pending_command_responses.clear()

        for timer in command_timers:
            timer.cancel()
        for response_event in pending_responses:
            response_event.set()
        for command_queue in command_queues:
            command_queue.put(None)
        for worker in command_workers:
            worker.join(timeout=2)

        if self.__mqtt_client is not None:
            try:
                self.__mqtt_client.loop_stop()
                self.__mqtt_client.disconnect()
            except Exception:
                self.__log.exception(
                    "Failed to close MQTT client"
                )

        self.__log.stop()

    def __on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            self.__connected = True

            client.subscribe(self.__topic_filter)

            self.__log.info(
                "Connected to MQTT broker, subscribed to %s",
                self.__topic_filter
            )
        else:
            self.__connected = False

            self.__log.error(
                "MQTT connection failed, reason code: %s",
                reason_code
            )

    def __on_disconnect(
        self,
        client,
        userdata,
        disconnect_flags=None,
        reason_code=None,
        properties=None
    ):
        self.__connected = False

        if not self.__stopped.is_set():
            self.__log.warning(
                "Disconnected from MQTT broker: %s",
                reason_code
            )

    def __on_message(self, client, userdata, message):
        try:
            payload = json.loads(
                message.payload.decode("utf-8")
            )

            self.__process_payload(message.topic, payload)

        except UnicodeDecodeError:
            self.__log.exception(
                "MQTT payload is not valid UTF-8, topic=%s",
                message.topic
            )

        except json.JSONDecodeError:
            self.__log.exception(
                "MQTT payload is not valid JSON, topic=%s",
                message.topic
            )

        except Exception:
            self.__log.exception(
                "Failed to process MQTT message, topic=%s",
                message.topic
            )

    def __process_payload(self, topic: str, payload: dict):
        self.__log.info("Received topic=%s, payload=%s", topic, payload)

        # 控制命令的响应会带回相同 mid，先放行对应网关的命令队列。
        self.__acknowledge_command_response(topic, payload)

        bid = payload.get("bid")

        if not bid:
            self.__log.warning("Payload does not contain 'bid', ignored.")
            return

        if bid == 101:
            self.__log.info("Received bid=101, heartbeat payload, publish to 'to' topic")
            topic_split = topic.split("/")
            self.__mqtt_client.publish(
                "/".join(["to"] + topic_split[1:]),
                json.dumps(payload)
            )
            # 为每台设备更新 heartbeat 消息
            heartbeat = {"HeartBeat": self.utc_now_iso()}
            for device in self.__devices:
                client_id = device["hperfor_client_id"]
                client_name = device["name"]
                converted_data = self.__convert_telemetry_data(
                    client_name=client_name,
                    client_telemetry=heartbeat
                )
                self.__gateway.send_to_storage(
                        self.__name,
                        self.__id,
                        converted_data
                    )
            return

        if bid in {201, 202, 207, 208}:
            self.__log.info("Received bid=%s, process payload", bid)

            # 对 201 消息需要回复
            if bid == 201:
                self.__log.info("Replying to bid=201 message")
                reply_payload = {
                    "bid": 201,
                    "result": True,
                    "mid": payload.get("mid", self.build_mid()),
                    "message": 0
                }
                topic_split = topic.split("/")
                self.__mqtt_client.publish(
                    "/".join(["to"] + topic_split[1:]),
                    json.dumps(reply_payload)
                )

            children = payload.get("Children")

            if not isinstance(children, list):
                self.__log.warning("Children is not an array")
                return

            client_attributes = {
                "gatewayID": topic.split("/")[-1]
            }

            for child in children:
                if not isinstance(child, dict):
                    continue

                client_id = child.get("ClientID")

                if not client_id:
                    self.__log.warning(
                        "Child without ClientID ignored: %s",
                        child
                    )
                    continue

                client_name = ""

                for device in self.__devices:
                    if client_id == device["hperfor_client_id"]:
                        client_name = device["name"]
                        break

                if client_name == "":
                    self.__log.warning(
                        "Child with unknown ClientID ignored: %s",
                        child
                    )
                    continue

                converted_data = self.__convert_telemetry_data(
                    client_name=client_name,
                    client_telemetry=child,
                    client_attributes=client_attributes
                )

                self.__gateway.send_to_storage(
                    self.__name,
                    self.__id,
                    converted_data
                )
            
            return

        if bid in {209, 210, 213}:
            self.__log.info("Received bid=%s, process payload", bid)

            children = payload.get("Children")

            if not isinstance(children, list):
                self.__log.warning("Children is not an array")
                return

            client_attributes = {
                "gatewayID": topic.split("/")[-1]
            }

            for child in children:
                if not isinstance(child, dict):
                    continue

                client_id = child.get("ClientID")

                if not client_id:
                    self.__log.warning(
                        "Child without ClientID ignored: %s",
                        child
                    )
                    continue

                client_name = ""

                for device in self.__devices:
                    if client_id == device["hperfor_client_id"]:
                        client_name = device["name"]
                        break

                if client_name == "":
                    self.__log.warning(
                        "Child with unknown ClientID ignored: %s",
                        child
                    )
                    continue

                converted_data = self.__convert_telemetry_data(
                    client_name=client_name,
                    client_attributes=child
                )

                self.__gateway.send_to_storage(
                    self.__name,
                    self.__id,
                    converted_data
                )
            
            return

        self.__log.warning("Unknown bid=%s", bid)

    def __convert_telemetry_data(
        self,
        client_name: str,
        client_telemetry: dict = None,
        client_attributes: dict = None,
    ) -> ConvertedData:

        converted_data = ConvertedData(
            device_name=client_name,
            device_type=self.__device_profile
        )

        # 可以把设备上报时间作为字符串属性保存；
        # 若要作为遥测时间戳，应先正确转换为毫秒时间戳。
        if client_telemetry is not None:
            converted_data.add_to_telemetry(
                TelemetryEntry(client_telemetry)
            )

        attributes = {}
        if client_attributes is not None:
            attributes = {
                key: value
                for key, value in client_attributes.items()
                if value is not None
            }
        if client_telemetry is not None:
            if "ClientID" in client_telemetry:
                attributes["ClientID"] = client_telemetry.get("ClientID")

        for key, value in attributes.items():
            converted_data.add_to_attributes(key, value)

        return converted_data

    def on_attributes_update(self, content):
        """
        ThingsBoard 共享属性更新时调用。

        后续可以在这里将属性更新转换成 MQTT 指令，
        再发布给原始设备。
        """
        self.__log.info(
            "Attribute update received: %s",
            content
        )

    def server_side_rpc_handler(self, content):
        self.__log.info("Received RPC: %s", content)

        # 标准格式:
        # {'device': 'F3-Light-North-test', 'data': {'id': 6, 'method': 'hello', 'params': {'status': 'off'}}, 'id': 6}

        device_name = content.get("device")
        rpc_data = content.get("data", {})

        request_id = rpc_data.get("id", content.get("id"))
        method = rpc_data.get("method")
        params = rpc_data.get("params")

        if not device_name:
            self.__log.error("RPC does not contain device name")
            return

        if request_id is None:
            self.__log.error("RPC does not contain request id")
            return

        # 处理RPC
        mqtt_topic, mqtt_content, delay_seconds = self.__match_rpc(device_name, method, params)

        # 执行 MQTT 发布
        if mqtt_topic and mqtt_content is not None:
            self.__log.info(
                "Matched RPC to MQTT topic=%s, content=%s, delay=%s",
                mqtt_topic,
                mqtt_content,
                delay_seconds
            )

            gateway_id = mqtt_topic.rsplit("/", 1)[-1]
            self.__enqueue_command(gateway_id, mqtt_topic, mqtt_content, delay_seconds)
 
            # 回复 RPC 成功
            self.__gateway.send_rpc_reply(
                device=device_name,
                req_id=request_id,
                content={"success": True, "status": "queued"}
            )
            return

        self.__gateway.send_rpc_reply(
            device=device_name,
            req_id=request_id,
            content={
                "success": False,
                "error": "RPC is not implemented",
                "method": method,
                "params": params
            }
        )

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.__name

    def get_type(self):
        return self._connector_type

    def get_config(self):
        return self.__config

    def is_connected(self):
        return self.__connected

    def is_stopped(self):
        return self.__stopped.is_set()

    def build_mid(self) -> str:
        return str(uuid.uuid4()).upper()

    def __match_rpc(self, device_name: str, method: str, params: dict) -> tuple:
        """
        根据 RPC 方法和参数，匹配 config 的对应的 MQTT 主题和内容。
        """
        # 检查命令映射：一个命令的多种形式, 例如“screen_shutdown”，对应 "switch {status: off}"
        # 命令形式: {'device': 'F3-Light-North', 'data': {'id': 17, 'method': 'screen_shutdown', 'params': ''}, 'id': 17}
        # 将新的命令中的method， params，导入进来 params 可能是 '' 空串
        if self.__rpc_method_maps:
            for method_map in self.__rpc_method_maps:
                if method in method_map.get("method", None):
                    rpc_data = method_map.get("rpc_data", {})
                    method = rpc_data.get("method", method)
                    if params:
                        # 合并两个字典
                        params.update(rpc_data.get("params", {}))
                    else:
                        params = rpc_data.get("params", {})
                    self.__log.info("New mapped method: %s, params: %s", method, params)

        # 获取 hperfor_client_id
        hperfor_client_id = ""
        hperfor_gateway_id = ""
        delay = {"on": 0, "off": 0}
        for device in self.__devices:
            if device_name == device.get("name"):
                hperfor_client_id = device.get("hperfor_client_id")
                hperfor_gateway_id = device.get("hperfor_gateway_id")
                delay["on"] = device.get("delay", {"on": 0, "off": 0}).get("on", 0)
                delay["off"] = device.get("delay", {"on": 0, "off": 0}).get("off", 0)
                break

        if not hperfor_client_id:
            self.__log.warning("No matching device for name: %s", device_name)
            return None, None, 0

        # 匹配 RPC 配置
        for rpc in self.__rpcs:
            if rpc.get("rpc_data", {}).get("method", None) == method:
                if rpc.get("rpc_data", {}).get("params", None) == params:
                    mqtt_data = rpc.get("mqtt_data", {"topic": None, "content": None})
                    # 转换mqtt_data为字符串
                    mqtt_str = json.dumps(mqtt_data)
                    # 查找字符串中是否包含 {hperfor_client_id} 和 {hperfor_gateway_id} 以及 {build_mid()}
                    if "{hperfor_client_id}" in mqtt_str:
                        mqtt_str = mqtt_str.replace("{hperfor_client_id}", hperfor_client_id)
                    if "{hperfor_gateway_id}" in mqtt_str:
                        mqtt_str = mqtt_str.replace("{hperfor_gateway_id}", hperfor_gateway_id)
                    if "{build_mid()}" in mqtt_str:
                        mqtt_str = mqtt_str.replace("{build_mid()}", self.build_mid())
                    # mqtt_data 转换为 json
                    mqtt_data = json.loads(mqtt_str)
                    mqtt_topic = mqtt_data.get("topic")
                    mqtt_content = mqtt_data.get("content")
                    delay_seconds = delay.get(rpc.get("delay", None), 0)
                    return mqtt_topic, mqtt_content, delay_seconds

        self.__log.warning("No matching RPC for method: %s, params: %s", method, params)
        return None, None, 0

    def utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def __enqueue_command(self, gateway_id, topic, content, delay_seconds):
        """将 RPC 放入物理网关专属的 FIFO 队列。"""
        with self.__command_lock:
            command_queue = self.__command_queues.get(gateway_id)
            if command_queue is None:
                command_queue = queue.Queue()
                worker = threading.Thread(
                    target=self.__command_worker,
                    args=(gateway_id, command_queue),
                    name="hyperfor-rpc-%s" % gateway_id,
                    daemon=True
                )
                self.__command_queues[gateway_id] = command_queue
                self.__command_workers[gateway_id] = worker
                worker.start()

        command = (topic, content)
        delay_seconds = max(0.0, float(delay_seconds))
        if delay_seconds:
            timer_holder = {}

            def enqueue_scheduled_command():
                timer = timer_holder["timer"]
                with self.__command_lock:
                    if timer in self.__command_timers:
                        self.__command_timers.remove(timer)
                if not self.__stopped.is_set():
                    command_queue.put(command)

            timer = threading.Timer(delay_seconds, enqueue_scheduled_command)
            timer_holder["timer"] = timer
            timer.daemon = True
            with self.__command_lock:
                self.__command_timers.append(timer)
            timer.start()
            self.__log.info("RPC scheduled for gateway=%s in %s seconds", gateway_id, delay_seconds)
        else:
            command_queue.put(command)
            self.__log.info("RPC queued for gateway=%s, queue_size=%s", gateway_id, command_queue.qsize())

    def __command_worker(self, gateway_id, command_queue):
        """顺序下发同一网关的命令，并等待 mid 回包或超时。"""
        last_publish_time = 0.0

        while not self.__stopped.is_set():
            try:
                command = command_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if command is None:
                command_queue.task_done()
                break

            topic, content = command
            response_key = None
            try:
                interval_wait = last_publish_time + self.__command_min_interval - time.monotonic()
                if interval_wait > 0 and self.__stopped.wait(interval_wait):
                    continue

                mid = content.get("mid") if isinstance(content, dict) else None
                response_event = None
                response_key = (gateway_id, str(mid)) if mid else None
                if self.__wait_for_command_response and response_key:
                    response_event = Event()
                    with self.__command_lock:
                        self.__pending_command_responses[response_key] = response_event

                publish_result = self.__mqtt_client.publish(topic, json.dumps(content))
                last_publish_time = time.monotonic()
                if publish_result.rc != mqtt.MQTT_ERR_SUCCESS:
                    self.__log.error("Failed to publish RPC gateway=%s, mid=%s, rc=%s", gateway_id, mid, publish_result.rc)
                else:
                    self.__log.info("Published queued RPC gateway=%s, mid=%s", gateway_id, mid)

                if response_event is not None and publish_result.rc == mqtt.MQTT_ERR_SUCCESS:
                    if not response_event.wait(self.__command_response_timeout):
                        self.__log.warning("RPC response timeout gateway=%s, mid=%s; continuing queue", gateway_id, mid)
            except Exception:
                self.__log.exception("Failed to execute queued RPC for gateway=%s", gateway_id)
            finally:
                if response_key:
                    with self.__command_lock:
                        self.__pending_command_responses.pop(response_key, None)
                command_queue.task_done()

    def __acknowledge_command_response(self, topic, payload):
        mid = payload.get("mid") if isinstance(payload, dict) else None
        if not mid:
            return

        gateway_id = topic.rsplit("/", 1)[-1]
        with self.__command_lock:
            response_event = self.__pending_command_responses.get((gateway_id, str(mid)))

        if response_event is not None:
            self.__log.info("Received RPC response gateway=%s, mid=%s", gateway_id, mid)
            response_event.set()
