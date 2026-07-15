import copy
import json
import time
from threading import Event, Lock, Thread
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

try:
    import requests
except ImportError:
    from thingsboard_gateway.tb_utility.tb_utility import TBUtility

    TBUtility.install_package("requests")
    import requests

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.tb_utility.tb_logger import init_logger

from datetime import datetime, timezone

class BestlinkIOTGatewayConnector(Thread, Connector):
    """Custom request connector for Bestlink HTTP API devices."""

    DEFAULT_DEVICE_TYPE = "default"
    DEFAULT_HTTP_TIMEOUT = 10.0
    LOOP_SLEEP_SECONDS = 0.2

    def __init__(self, gateway, config: Dict[str, Any], connector_type: str):
        super().__init__(daemon=True)

        self._gateway = gateway
        self._config = config
        self._connector_type = connector_type
        self._id = config.get("id")
        self._name = config.get("name", "Bestlink IOT Gateway")

        self._stopped = Event()
        self._stopped.set()
        self._connected = False

        self._session = requests.Session()
        self._http_lock = Lock()

        self.__device_profile = config.get("deviceProfile", "default")
        self._devices: Dict[str, Dict[str, Any]] = {}
        self._poll_jobs: List[Dict[str, Any]] = []

        self._log = init_logger(
            self._gateway,
            self._name,
            level=config.get("logLevel", "INFO"),
            enable_remote_logging=config.get("enableRemoteLogging", False),
            is_connector_logger=True,
        )

        self._load_configuration()
        self._log.info(
            "Connector %s initialized: %d devices, %d polling jobs",
            self._name,
            len(self._devices),
            len(self._poll_jobs),
        )

    # ---------------------------------------------------------------------
    # Required Connector interface
    # ---------------------------------------------------------------------

    def open(self) -> None:
        if self.is_alive():
            self._log.warning("Connector %s is already running", self._name)
            return

        self._stopped.clear()
        self.start()

    def close(self) -> None:
        if self._stopped.is_set():
            return

        self._log.info("Stopping connector %s", self._name)
        self._stopped.set()
        self._connected = False

        for device_name in self._devices:
            try:
                self._gateway.del_device(device_name)
            except Exception:
                self._log.exception("Failed to disconnect device %s", device_name)

        try:
            self._session.close()
        finally:
            self._log.stop()

    def get_id(self):
        return self._id

    def get_name(self) -> str:
        return self._name

    def get_type(self) -> str:
        return self._connector_type

    def get_config(self) -> Dict[str, Any]:
        return self._config

    def is_connected(self) -> bool:
        return self._connected

    def is_stopped(self) -> bool:
        return self._stopped.is_set()

    def on_attributes_update(self, content: Dict[str, Any]) -> None:
        # No attribute-to-HTTP mappings are present in the supplied config.
        self._log.debug("Attribute update is not configured: %s", content)

    # ---------------------------------------------------------------------
    # Main worker
    # ---------------------------------------------------------------------

    def run(self) -> None:
        self._log.info("start running...")
        try:
            self._register_devices()
            self._connected = True

            # Run all polling jobs immediately after startup.
            now = time.monotonic()
            for job in self._poll_jobs:
                job["next_run"] = now

            while not self._stopped.is_set():
                now = time.monotonic()
                ran_job = False

                for job in self._poll_jobs:
                    if now < job["next_run"]:
                        continue

                    ran_job = True
                    self._run_poll_job(job)

                    # Schedule from the current time so slow APIs do not create
                    # a tight "catch-up" loop.
                    job["next_run"] = time.monotonic() + job["period"]

                if not ran_job:
                    self._stopped.wait(self.LOOP_SLEEP_SECONDS)

        except Exception:
            self._connected = False
            self._log.exception("Fatal error in connector %s", self._name)
        finally:
            self._connected = False

    # ---------------------------------------------------------------------
    # Configuration and device registration
    # ---------------------------------------------------------------------

    def _load_configuration(self) -> None:
        devices = self._config.get("devices", [])
        if not isinstance(devices, list):
            raise ValueError('"devices" must be a JSON array')

        for device_config in devices:
            device_name = device_config.get("name")
            if not device_name:
                self._log.error("Ignoring device without name: %s", device_config)
                continue

            if device_name in self._devices:
                self._log.error("Ignoring duplicate device name: %s", device_name)
                continue

            self._devices[device_name] = device_config

            for destination, section_name in (
                ("telemetry", "tb_telemetry"),
                ("attributes", "tb_attributes"),
            ):
                for request_config in device_config.get(section_name, []):
                    period = self._normalise_period(request_config.get("period", 60))
                    self._poll_jobs.append(
                        {
                            "device_name": device_name,
                            "device_config": device_config,
                            "request_config": request_config,
                            "destination": destination,
                            "period": period,
                            "next_run": 0.0,
                        }
                    )

    def _register_devices(self) -> None:
        for device_name, device_config in self._devices.items():
            device_type = device_config.get(
                "type",
                device_config.get("deviceType", self.DEFAULT_DEVICE_TYPE),
            )

            added = self._gateway.add_device(
                device_name,
                {"connector": self},
                device_type=device_type,
            )
            if not added:
                self._log.warning(
                    "Gateway returned False while registering %s; "
                    "the device may already be registered",
                    device_name,
                )

            static_attributes = device_config.get("static_attributes") or {}
            if static_attributes:
                self._send_attributes(device_name, device_type, static_attributes)

            static_telemetry = device_config.get("static_telemetry") or {}
            if static_telemetry:
                self._send_telemetry(device_name, device_type, static_telemetry)

            # A token belongs to a directly connected ThingsBoard device.
            # Child devices proxied by a Gateway are identified by device name,
            # therefore tb_device_token is intentionally not used.
            if device_config.get("tb_device_token"):
                self._log.debug(
                    "tb_device_token for %s is ignored in Gateway child-device mode",
                    device_name,
                )

    @staticmethod
    def _normalise_period(value: Any) -> float:
        try:
            period = float(value)
        except (TypeError, ValueError):
            period = 60.0

        return max(period, 0.1)

    # ---------------------------------------------------------------------
    # Polling and HTTP
    # ---------------------------------------------------------------------

    def _run_poll_job(self, job: Dict[str, Any]) -> None:
        device_name = job["device_name"]
        device_config = job["device_config"]
        request_config = job["request_config"]
        destination = job["destination"]

        try:
            response_data = self._perform_http_request(request_config.get("api", {}))
            values = self._convert_response(
                response_data,
                request_config.get("pivot"),
                device_config,
            )

            if not values:
                self._log.debug(
                    "No %s values produced for device %s",
                    destination,
                    device_name,
                )

            # 添加 HeartBeat 参数，保证设备活跃
            if not values:
                values = {"HeartBeat": self.utc_now_iso()}
            else:
                values["HeartBeat"] = self.utc_now_iso()

            device_type = device_config.get(
                "type",
                device_config.get("deviceType", self.DEFAULT_DEVICE_TYPE),
            )

            if destination == "attributes":
                self._send_attributes(device_name, device_type, values)
            else:
                self._send_telemetry(device_name, device_type, values)

            self._log.debug(
                "Uploaded %s for %s: %s",
                destination,
                device_name,
                values,
            )

        except requests.RequestException as exc:
            self._log.error(
                "HTTP polling failed for %s: %s",
                device_name,
                exc,
            )
        except Exception:
            self._log.exception("Polling job failed for %s", device_name)

    def _perform_http_request(self, api_config: Mapping[str, Any]) -> Any:
        url = api_config.get("url")
        if not url:
            raise ValueError("API url is required")

        method = str(api_config.get("method", "GET")).upper()
        headers = api_config.get("headers") or {}
        timeout = float(api_config.get("timeout", self.DEFAULT_HTTP_TIMEOUT))
        verify = api_config.get("verify", True)

        kwargs: Dict[str, Any] = {
            "method": method,
            "url": url,
            "headers": headers,
            "timeout": timeout,
            "verify": verify,
        }

        if api_config.get("params") is not None:
            kwargs["params"] = api_config["params"]

        if api_config.get("data") is not None:
            content_type = str(headers.get("Content-Type", "")).lower()
            if "application/json" in content_type:
                kwargs["json"] = api_config["data"]
            else:
                kwargs["data"] = api_config["data"]

        with self._http_lock:
            response = self._session.request(**kwargs)

        response.raise_for_status()

        if response.status_code == 204 or not response.content:
            return {}

        try:
            return response.json()
        except ValueError:
            return {"text": response.text}

    # ---------------------------------------------------------------------
    # Response conversion
    # ---------------------------------------------------------------------

    def _convert_response(
        self,
        response_data: Any,
        pivot_config: Optional[Mapping[str, Any]],
        device_config: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """
        Convert a third-party API response to ThingsBoard values.

        For a response such as:

        {
            "data": [
                {
                    "deviceName": "3F-西过道空调",
                    "attrCode": "thermostatTemperatureSetpoint",
                    "attrValue": "24"
                },
                {
                    "deviceName": "3F-西过道空调",
                    "attrCode": "fanSpeed"
                },
                {
                    "deviceName": "3F-西过道空调",
                    "attrCode": "on",
                    "attrValue": "0"
                }
            ]
        }

        the generated values are:

        {
            "deviceName": "3F-西过道空调",
            "thermostatTemperatureSetpoint": "24",
            "on": "0"
        }

        Empty or missing attrValue fields are ignored. String values are
        preserved by default; set "preserve_string": false in pivot config
        to convert numeric and boolean strings to native Python values.
        """
        if not pivot_config:
            if isinstance(response_data, dict):
                return response_data
            return {"value": response_data}

        rows = self._get_path(
            response_data,
            pivot_config.get("path", ""),
        )

        key_field = pivot_config.get("key_field")
        value_field = pivot_config.get("value_field")
        include_keys = set(pivot_config.get("include_keys") or [])

        skip_empty = pivot_config.get("skip_empty", True)
        preserve_string = pivot_config.get("preserve_string", True)

        result: Dict[str, Any] = {}

        if isinstance(rows, list) and key_field and value_field:
            for row in rows:
                if not isinstance(row, dict):
                    continue

                key = row.get(key_field)
                if key is None:
                    continue

                key = str(key)
                if include_keys and key not in include_keys:
                    continue

                # Some API records contain parseValue but do not contain
                # attrValue. Only the configured value field is uploaded.
                if value_field not in row:
                    continue

                value = row.get(value_field)
                if skip_empty and self._is_empty_value(value):
                    continue

                result[key] = (
                    value
                    if preserve_string
                    else self._coerce_scalar(value)
                )

        elif isinstance(rows, dict):
            for key, value in rows.items():
                if include_keys and key not in include_keys:
                    continue

                if skip_empty and self._is_empty_value(value):
                    continue

                result[str(key)] = (
                    value
                    if preserve_string
                    else self._coerce_scalar(value)
                )

        elif rows is not None:
            if not skip_empty or not self._is_empty_value(rows):
                result["value"] = (
                    rows
                    if preserve_string
                    else self._coerce_scalar(rows)
                )

        # Resolve configured fields from:
        # 1. the response root;
        # 2. the first dictionary item under the pivot path;
        # 3. device static_attributes.
        first_row = None
        if isinstance(rows, list):
            first_row = next(
                (row for row in rows if isinstance(row, dict)),
                None,
            )

        static_attributes = device_config.get("static_attributes") or {}

        for output_key, source_path in (
            pivot_config.get("fields") or {}
        ).items():
            value = self._get_path(response_data, source_path)

            if value is None and first_row is not None:
                value = self._get_path(first_row, source_path)

            if value is None:
                value = self._get_path(static_attributes, source_path)

            if skip_empty and self._is_empty_value(value):
                continue

            if value is not None:
                result[output_key] = (
                    value
                    if preserve_string
                    else self._coerce_scalar(value)
                )

        return result

    @staticmethod
    def _get_path(data: Any, path: Any) -> Any:
        if path in (None, "", "$"):
            return data

        current = data
        cleaned = str(path).strip()
        if cleaned.startswith("$."):
            cleaned = cleaned[2:]

        for part in cleaned.split("."):
            if not part:
                continue

            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    current = current[int(part)]
                except (ValueError, IndexError):
                    return None
            else:
                return None

            if current is None:
                return None

        return current


    @staticmethod
    def _is_empty_value(value: Any) -> bool:
        """
        Return True for values that should not be uploaded.

        Empty values:
        - None
        - empty or whitespace-only strings
        - the strings "null" and "none", ignoring case

        Numeric zero and the string "0" are valid values.
        """
        if value is None:
            return True

        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return True
            if stripped.lower() in ("null", "none"):
                return True

        return False

    @staticmethod
    def _coerce_scalar(value: Any) -> Any:
        if not isinstance(value, str):
            return value

        text = value.strip()
        lowered = text.lower()

        if lowered == "true":
            return True
        if lowered == "false":
            return False
        if lowered in ("null", "none"):
            return None

        try:
            if "." not in text and "e" not in lowered:
                return int(text)
            return float(text)
        except ValueError:
            return value

    # ---------------------------------------------------------------------
    # ThingsBoard uplink
    # ---------------------------------------------------------------------

    def _send_telemetry(
        self,
        device_name: str,
        device_type: str,
        values: Mapping[str, Any],
    ) -> None:
        if not values:
            return

        converted = ConvertedData(device_name, device_type)
        converted.add_to_telemetry(TelemetryEntry(dict(values)))
        self._gateway.send_to_storage(self._name, self._id, converted)

    def _send_attributes(
        self,
        device_name: str,
        device_type: str,
        values: Mapping[str, Any],
    ) -> None:
        if not values:
            return

        converted = ConvertedData(device_name, device_type)
        converted.add_to_attributes(dict(values))
        self._gateway.send_to_storage(self._name, self._id, converted)

    # ---------------------------------------------------------------------
    # RPC
    # ---------------------------------------------------------------------

    def server_side_rpc_handler(self, content: Dict[str, Any]) -> None:
        self._log.info("Received RPC: %s", content)

        device_name, request_id, method, params = self._parse_rpc_content(content)

        if not device_name:
            result = {
                "success": False,
                "error": "RPC does not contain a device name",
            }
            self._log.error("%s: %s", result["error"], content)
            self._send_rpc_reply(device_name, request_id, result)
            return

        device_config = self._devices.get(device_name)
        if device_config is None:
            result = {
                "success": False,
                "error": "Device is not managed by this connector",
                "device": device_name,
            }
            self._send_rpc_reply(device_name, request_id, result)
            return

        rpc_config = self._find_rpc_config(device_config, method)
        if rpc_config is None:
            result = {
                "success": False,
                "error": "RPC method is not configured",
                "method": method,
            }
            self._send_rpc_reply(device_name, request_id, result)
            return

        try:
            delay = float(rpc_config.get("delay", 0))
            if delay > 0 and self._stopped.wait(delay):
                return

            api_config = self._render_rpc_api(
                rpc_config.get("api", {}),
                device_config,
                params,
            )
            response_data = self._perform_http_request(api_config)

            telemetry = rpc_config.get("telemetry") or {}
            if telemetry:
                device_type = device_config.get(
                    "type",
                    device_config.get("deviceType", self.DEFAULT_DEVICE_TYPE),
                )
                self._send_telemetry(device_name, device_type, telemetry)

            result = {
                "success": True,
                "device": device_name,
                "method": method,
                "response": response_data,
            }

        except requests.RequestException as exc:
            self._log.error(
                "RPC HTTP request failed, device=%s method=%s: %s",
                device_name,
                method,
                exc,
            )
            result = {
                "success": False,
                "device": device_name,
                "method": method,
                "error": str(exc),
            }
        except Exception as exc:
            self._log.exception(
                "RPC execution failed, device=%s method=%s",
                device_name,
                method,
            )
            result = {
                "success": False,
                "device": device_name,
                "method": method,
                "error": str(exc),
            }

        self._send_rpc_reply(device_name, request_id, result)

    def _parse_rpc_content(
        self,
        content: Mapping[str, Any],
    ) -> Tuple[Optional[str], Any, Optional[str], Any]:
        """
        Supports both common payloads:

        Device RPC:
            {
              "device": "F3-AirCondition-North",
              "data": {"id": 1, "method": "on", "params": {}}
            }

        Some connector-RPC/Gateway variants:
            {
              "id": 1,
              "method": "on",
              "params": {
                "deviceName": "F3-AirCondition-North",
                ...
              }
            }
        """
        rpc_data = content.get("data")
        if not isinstance(rpc_data, dict):
            rpc_data = content

        params = rpc_data.get("params")
        device_name = content.get("device") or rpc_data.get("device")

        if not device_name and isinstance(params, dict):
            device_name = (
                params.get("device")
                or params.get("deviceName")
                or params.get("name")
            )

            # Support a nested command envelope:
            # params = {"command": "on", "params": {...}, "deviceName": "..."}
            nested_method = params.get("command")
            nested_params = params.get("params")
        else:
            nested_method = None
            nested_params = None

        method = rpc_data.get("method") or nested_method
        if nested_method:
            method = nested_method
        if nested_params is not None:
            params = nested_params

        request_id = rpc_data.get("id", content.get("id"))
        return device_name, request_id, method, params

    @staticmethod
    def _find_rpc_config(
        device_config: Mapping[str, Any],
        requested_method: Optional[str],
    ) -> Optional[Mapping[str, Any]]:
        for rpc_config in device_config.get("tb_rpc", []):
            methods = rpc_config.get("method", [])
            if isinstance(methods, str):
                methods = [methods]

            if requested_method in methods:
                return rpc_config

        return None

    def _render_rpc_api(
        self,
        api_config: Mapping[str, Any],
        device_config: Mapping[str, Any],
        params: Any,
    ) -> Dict[str, Any]:
        context = {
            "deviceName": device_config.get("name"),
            "deviceCode": (device_config.get("static_attributes") or {}).get(
                "deviceCode"
            ),
            "params": params,
        }
        return self._render_template(copy.deepcopy(dict(api_config)), context)

    def _render_template(self, value: Any, context: Mapping[str, Any]) -> Any:
        """
        Replaces exact placeholders such as:
            "${deviceCode}"
            "${params.value}"

        An exact placeholder preserves the original value type. Placeholders
        embedded in a larger string are converted to text.
        """
        if isinstance(value, dict):
            return {
                key: self._render_template(item, context)
                for key, item in value.items()
            }

        if isinstance(value, list):
            return [self._render_template(item, context) for item in value]

        if not isinstance(value, str):
            return value

        if value.startswith("${") and value.endswith("}") and value.count("${") == 1:
            resolved = self._get_path(context, value[2:-1])
            return value if resolved is None else resolved

        rendered = value
        for key in self._find_placeholders(value):
            resolved = self._get_path(context, key)
            if resolved is not None:
                rendered = rendered.replace("${" + key + "}", str(resolved))
        return rendered

    @staticmethod
    def _find_placeholders(value: str) -> Iterable[str]:
        start = 0
        while True:
            left = value.find("${", start)
            if left < 0:
                return
            right = value.find("}", left + 2)
            if right < 0:
                return
            yield value[left + 2:right]
            start = right + 1

    def _send_rpc_reply(
        self,
        device_name: Optional[str],
        request_id: Any,
        result: Mapping[str, Any],
    ) -> None:
        if request_id is None:
            self._log.warning("Cannot send RPC reply without request id: %s", result)
            return

        # For a malformed connector-level RPC there may be no child-device
        # name. The gateway device name is used as a best-effort fallback.
        reply_device = device_name or self._name

        try:
            self._gateway.send_rpc_reply(
                device=reply_device,
                req_id=request_id,
                content=dict(result),
                wait_for_publish=True,
                quality_of_service=1,
                to_connector_rpc=device_name is None,
            )
        except TypeError:
            # Compatibility with Gateway versions whose send_rpc_reply does
            # not yet accept to_connector_rpc.
            self._gateway.send_rpc_reply(
                device=reply_device,
                req_id=request_id,
                content=dict(result),
                wait_for_publish=True,
                quality_of_service=1,
            )

    def utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()
