#
# Cassini
#
# Copyright (C) 2023 Vladimir Vukicevic
# License: MIT
#

import sys
import socket
import time
import json
import asyncio
import logging
import random
from enum import Enum

import websockets

SATURN_UDP_PORT = 3000
SDCP_V3_WS_PORTS = [3030, 80]


class CurrentStatus(Enum):
    READY = 0
    BUSY = 1


class PrintInfoStatus(Enum):
    EXPOSURE = 2
    RETRACTING = 3
    LOWERING = 4
    COMPLETE = 16


class FileStatus(Enum):
    NONE = 0
    DONE = 2
    ERROR = 3


class ProtocolFlavor(Enum):
    LEGACY_MQTT = "legacy-mqtt"
    SDCP_V3_WEBSOCKET = "sdcp-v3-websocket"


class Command(Enum):
    CMD_0 = 0
    CMD_1 = 1
    DISCONNECT = 64
    START_PRINTING = 128
    PAUSE_PRINTING = 129
    STOP_PRINTING = 130
    CONTINUE_PRINTING = 131
    UPLOAD_FILE = 256
    SET_MYSTERY_TIME_PERIOD = 512


def random_hexstr():
    return "%032x" % random.getrandbits(128)


class SaturnPrinter:
    def __init__(self, addr, desc, timeout=5):
        self.addr = addr
        self.timeout = timeout
        self.file_transfer_future = None
        self.last_status = None
        self.protocol_flavor = ProtocolFlavor.LEGACY_MQTT
        self.protocol_version = ""
        if desc is not None:
            self.set_desc(desc)
        else:
            self.desc = None

    def find_printers(timeout=1, broadcast=None):
        if broadcast is None:
            broadcast = "<broadcast>"
        printers = []
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        with sock:
            sock.settimeout(timeout)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, timeout)
            sock.sendto(b"M99999", (broadcast, SATURN_UDP_PORT))

            now = time.time()
            while True:
                if time.time() - now > timeout:
                    break
                try:
                    data, addr = sock.recvfrom(8192)
                except socket.timeout:
                    continue
                else:
                    try:
                        pdata = json.loads(data.decode("utf-8"))
                        printers.append(SaturnPrinter(addr, pdata))
                    except json.JSONDecodeError:
                        logging.warning(f"Ignoring non-JSON discovery response from {addr[0]}")
        return printers

    def find_printer(addr, timeout=5):
        printers = SaturnPrinter.find_printers(timeout=timeout, broadcast=addr)
        if len(printers) == 0 or printers[0].addr[0] != addr:
            return None
        return printers[0]

    def refresh(self, timeout=5):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        with sock:
            sock.settimeout(timeout)
            sock.sendto(b"M99999", (self.addr[0], SATURN_UDP_PORT))
            try:
                data, _ = sock.recvfrom(8192)
            except socket.timeout:
                return False
            else:
                pdata = json.loads(data.decode("utf-8"))
                self.set_desc(pdata)
                return True

    def set_desc(self, desc):
        self.desc = desc
        data = desc.get("Data", {})

        if "Attributes" in data:
            attrs = data.get("Attributes", {})
            status = data.get("Status", {})
            self.protocol_flavor = ProtocolFlavor.LEGACY_MQTT
            self.last_status = status if isinstance(status, dict) else None
        else:
            attrs = data
            self.last_status = None
            protocol_version = str(attrs.get("ProtocolVersion", ""))
            if protocol_version.startswith("V3"):
                self.protocol_flavor = ProtocolFlavor.SDCP_V3_WEBSOCKET
            else:
                self.protocol_flavor = ProtocolFlavor.LEGACY_MQTT

        self.id = attrs.get("MainboardID", "")
        self.name = attrs.get("Name", "Unknown")
        self.machine_name = attrs.get("MachineName", self.name)
        self.protocol_version = attrs.get("ProtocolVersion", "")

        if self.last_status is not None:
            self.current_status = self.last_status.get("CurrentStatus", 0)
        else:
            self.current_status = 0
        self.busy = self.current_status > 0

    async def connect(self, mqtt, http):
        if self.protocol_flavor != ProtocolFlavor.LEGACY_MQTT:
            logging.error("Legacy MQTT connect flow is not used for SDCP V3 printers")
            return False

        self.mqtt = mqtt
        self.http = http

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        with sock:
            sock.sendto(b"M66666 " + str(mqtt.port).encode("utf-8"), self.addr)

        client_id = await asyncio.wait_for(mqtt.client_connection, timeout=self.timeout)
        if client_id != self.id:
            logging.error(f"Client ID mismatch: {client_id} != {self.id}")
            return False

        topic = await asyncio.wait_for(self.mqtt.client_subscribed, timeout=self.timeout)
        logging.debug(f"Client subscribed to {topic}")

        await self.send_command_and_wait(Command.CMD_0)
        await self.send_command_and_wait(Command.CMD_1)
        await self.send_command_and_wait(Command.SET_MYSTERY_TIME_PERIOD, {"TimePeriod": 5000})
        return True

    async def disconnect(self):
        await self.send_command_and_wait(Command.DISCONNECT)

    async def _send_command_v3_and_wait(self, cmdid, data=None, abort_on_bad_ack=True):
        request_id = random_hexstr()
        payload = {
            "Id": self.desc.get("Id", ""),
            "Data": {
                "Cmd": cmdid.value,
                "Data": data if data is not None else {},
                "From": 1,
                "MainboardID": self.id,
                "RequestID": request_id,
                "TimeStamp": int(time.time()),
            },
            "Topic": f"sdcp/request/{self.id}",
        }

        last_error = None
        for port in SDCP_V3_WS_PORTS:
            uri = f"ws://{self.addr[0]}:{port}/websocket"
            timeout_at = time.time() + self.timeout
            try:
                async with websockets.connect(uri, open_timeout=self.timeout, close_timeout=self.timeout) as ws:
                    await ws.send(json.dumps(payload))

                    while True:
                        remaining = timeout_at - time.time()
                        if remaining <= 0:
                            raise TimeoutError(f"Timed out waiting for response to command {cmdid.value}")

                        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="ignore")

                        msg = json.loads(raw)
                        msg_data = msg.get("Data", {})
                        topic = msg.get("Topic", "")

                        status_payload = msg.get("Status")
                        if not isinstance(status_payload, dict):
                            status_payload = msg_data.get("Status")
                        if isinstance(status_payload, dict):
                            self.last_status = status_payload
                            self.current_status = self._normalize_scalar(status_payload.get("CurrentStatus", self.current_status))
                            self.busy = self.current_status > 0

                        if "/response/" not in topic:
                            continue
                        if not topic.endswith("/" + self.id):
                            continue
                        if msg_data.get("RequestID") != request_id:
                            continue

                        result = msg_data.get("Data", {})
                        ack = result.get("Ack", 0)
                        if abort_on_bad_ack and ack != 0:
                            raise RuntimeError(f"Command {cmdid.value} returned Ack={ack} payload={result}")
                        return result
            except Exception as ex:
                last_error = ex

        raise RuntimeError(f"Unable to reach SDCP V3 websocket for {self.addr[0]}: {last_error}")

    async def refresh_v3_status(self):
        if self.protocol_flavor != ProtocolFlavor.SDCP_V3_WEBSOCKET:
            return False
        request_id = random_hexstr()
        payload = {
            "Id": self.desc.get("Id", ""),
            "Data": {
                "Cmd": Command.CMD_0.value,
                "Data": {},
                "From": 1,
                "MainboardID": self.id,
                "RequestID": request_id,
                "TimeStamp": int(time.time()),
            },
            "Topic": f"sdcp/request/{self.id}",
        }

        last_error = None
        for port in SDCP_V3_WS_PORTS:
            uri = f"ws://{self.addr[0]}:{port}/websocket"
            try:
                async with websockets.connect(uri, open_timeout=self.timeout, close_timeout=self.timeout) as ws:
                    await ws.send(json.dumps(payload))
                    timeout_at = time.time() + self.timeout
                    got_ack = False

                    while True:
                        remaining = timeout_at - time.time()
                        if remaining <= 0:
                            break
                        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="ignore")

                        msg = json.loads(raw)
                        msg_data = msg.get("Data", {})
                        topic = msg.get("Topic", "")

                        status_payload = msg.get("Status")
                        if not isinstance(status_payload, dict):
                            status_payload = msg_data.get("Status")
                        if isinstance(status_payload, dict):
                            self.last_status = status_payload
                            self.current_status = self._normalize_scalar(status_payload.get("CurrentStatus", self.current_status))
                            self.busy = self.current_status > 0
                            return True

                        if "/response/" in topic and topic.endswith("/" + self.id) and msg_data.get("RequestID") == request_id:
                            got_ack = True

                    if got_ack:
                        return self.last_status is not None
            except Exception as ex:
                last_error = ex

        if last_error is not None:
            logging.warning(f"Failed to refresh V3 status for {self.addr[0]}: {last_error}")
        return False

    async def upload_file(self, filename, start_printing=False):
        if self.protocol_flavor != ProtocolFlavor.LEGACY_MQTT:
            raise RuntimeError("Upload flow is currently implemented only for legacy MQTT printers")
        try:
            await self.upload_file_inner(filename, start_printing)
        except Exception as ex:
            logging.error(f"Exception during upload: {ex}")
            if self.file_transfer_future is not None:
                self.file_transfer_future.set_result((-1, -1, filename))
                self.file_transfer_future = asyncio.get_running_loop().create_future()

    async def upload_file_inner(self, filename, start_printing=False):
        self.file_transfer_future = asyncio.get_running_loop().create_future()

        basename = filename.split("\\")[-1].split("/")[-1]
        ext = basename.split(".")[-1].lower()
        if ext != "ctb" and ext != "goo":
            logging.warning(f"Unknown file extension: {ext}")

        httpname = random_hexstr() + "." + ext
        fileinfo = self.http.register_file_route("/" + httpname, filename)

        cmd_data = {
            "Check": 0,
            "CleanCache": 1,
            "Compress": 0,
            "FileSize": fileinfo["size"],
            "Filename": basename,
            "MD5": fileinfo["md5"],
            "URL": f"http://${{ipaddr}}:{self.http.port}/{httpname}",
        }

        await self.send_command_and_wait(Command.UPLOAD_FILE, cmd_data)

        while True:
            reply = await asyncio.wait_for(self.mqtt.next_published_message(), timeout=self.timeout * 2)
            data = json.loads(reply["payload"])
            if reply["topic"] == "/sdcp/status/" + self.id:
                self.incoming_status(data["Data"]["Status"])

                status = data["Data"]["Status"]
                file_info = status["FileTransferInfo"]
                current_offset = file_info["DownloadOffset"]
                total_size = file_info["FileTotalSize"]
                file_name = file_info["Filename"]

                if status["CurrentStatus"] == CurrentStatus.READY:
                    if file_info["Status"] == FileStatus.DONE:
                        self.file_transfer_future.set_result((total_size, total_size, file_name))
                    elif file_info["Status"] == FileStatus.ERROR:
                        logging.error("Transfer error")
                        self.file_transfer_future.set_result((-1, total_size, file_name))
                    else:
                        logging.error(f"Unknown file transfer status code: {file_info['Status']}")
                        self.file_transfer_future.set_result((-1, total_size, file_name))
                    break

                self.file_transfer_future.set_result((current_offset, total_size, file_name))
                self.file_transfer_future = asyncio.get_running_loop().create_future()

        self.file_transfer_future = None

    async def send_command_and_wait(self, cmdid, data=None, abort_on_bad_ack=True):
        if self.protocol_flavor == ProtocolFlavor.SDCP_V3_WEBSOCKET:
            return await self._send_command_v3_and_wait(cmdid, data, abort_on_bad_ack=abort_on_bad_ack)

        req = self.send_command(cmdid, data)
        logging.debug(f"Sent command {cmdid} as request {req}")
        while True:
            reply = await asyncio.wait_for(self.mqtt.next_published_message(), timeout=self.timeout)
            data = json.loads(reply["payload"])
            if reply["topic"] == "/sdcp/response/" + self.id:
                if data["Data"]["RequestID"] == req:
                    logging.debug(f"Got response to {req}")
                    result = data["Data"]["Data"]
                    if abort_on_bad_ack and result["Ack"] != 0:
                        raise RuntimeError(f"Command {cmdid.value} returned bad ack: {result}")
                    return result
            elif reply["topic"] == "/sdcp/status/" + self.id:
                self.incoming_status(data["Data"]["Status"])

    async def print_file(self, filename):
        cmd_data = {
            "Filename": filename,
            "StartLayer": 0,
        }
        result = await self.send_command_and_wait(Command.START_PRINTING, cmd_data, abort_on_bad_ack=False)
        return result.get("Ack", 0) == 0

    async def pause_print(self):
        return await self.send_command_and_wait(Command.PAUSE_PRINTING, {}, abort_on_bad_ack=False)

    async def resume_print(self):
        return await self.send_command_and_wait(Command.CONTINUE_PRINTING, {}, abort_on_bad_ack=False)

    async def stop_print(self):
        return await self.send_command_and_wait(Command.STOP_PRINTING, {}, abort_on_bad_ack=False)

    def incoming_status(self, status):
        logging.debug(f"STATUS: {status}")
        if isinstance(status, dict):
            self.last_status = status
            self.current_status = self._normalize_scalar(status.get("CurrentStatus", self.current_status))
            self.busy = self.current_status > 0

    def _normalize_scalar(self, value):
        if isinstance(value, list) and len(value) == 1:
            return value[0]
        return value

    def describe(self):
        return f"{self.name} ({self.machine_name})"

    def status(self):
        status_obj = self.last_status or {}
        printinfo = status_obj.get("PrintInfo", {})
        return {
            "status": self._normalize_scalar(status_obj.get("CurrentStatus", 0)),
            "filename": printinfo.get("Filename", ""),
            "currentLayer": self._normalize_scalar(printinfo.get("CurrentLayer", 0)),
            "totalLayers": self._normalize_scalar(printinfo.get("TotalLayer", 0)),
            "printStatus": self._normalize_scalar(printinfo.get("Status", 0)),
            "protocol": self.protocol_version,
            "transport": self.protocol_flavor.value,
        }

    def send_command(self, cmdid, data=None):
        if self.protocol_flavor != ProtocolFlavor.LEGACY_MQTT:
            raise RuntimeError("MQTT publish path is not used for SDCP V3 printers")
        hexstr = random_hexstr()
        timestamp = int(time.time() * 1000)
        cmd_data = {
            "Data": {
                "Cmd": cmdid.value,
                "Data": data,
                "From": 0,
                "MainboardID": self.id,
                "RequestID": hexstr,
                "TimeStamp": timestamp,
            },
            "Id": self.desc["Id"],
        }
        self.mqtt.publish("/sdcp/request/" + self.id, json.dumps(cmd_data))
        return hexstr

    def connect_mqtt(self, mqtt_host, mqtt_port):
        from scapy.all import IP, UDP, send

        logging.getLogger("scapy.runtime").setLevel(logging.WARNING)
        ip = IP(dst=self.addr[0], src=mqtt_host)
        any_src_port = random.randint(1024, 65535)
        udp = UDP(sport=any_src_port, dport=SATURN_UDP_PORT)
        payload = f"M66666 {mqtt_port}"
        packet = ip / udp / payload
        send(packet)
