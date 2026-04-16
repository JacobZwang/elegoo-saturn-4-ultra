#!env python3
# -*- coding: utf-8 -*-
#
# Cassini
#
# Copyright (C) 2023 Vladimir Vukicevic
# License: MIT
#
import os
import pprint
import socket
import sys
import time
import asyncio
import logging
import argparse
from simple_mqtt_server import SimpleMQTTServer
from simple_http_server import SimpleHTTPServer
from saturn_printer import SaturnPrinter, PrintInfoStatus, CurrentStatus, FileStatus, ProtocolFlavor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

try:
    from alive_progress import alive_bar
except ImportError:
    logging.info("Run 'pip3 install alive-progress' for better progress bars")
    class alive_bar(object):
        def __init__(self, total, title, **kwargs):
            self.total = total
            self.title = title
        def __call__(self, x):
            print(f"{int(x*self.total)}/{self.total} {self.title}\r", end="")
        def __enter__(self):
            return self
        def __exit__(self, *args):
            print("\n")

async def create_mqtt_server():
    mqtt = SimpleMQTTServer('0.0.0.0', 0)
    await mqtt.start()
    mqtt_server_task = asyncio.create_task(mqtt.serve_forever())
    return mqtt, mqtt.port, mqtt_server_task

async def create_http_server():
    http = SimpleHTTPServer('0.0.0.0', 0)
    await http.start()
    http_server_task = asyncio.create_task(http.serve_forever())
    return http, http.port, http_server_task

def _safe_enum_name(enum_type, value):
    try:
        return enum_type(value).name
    except Exception:
        return str(value)


async def refresh_live_status(printer):
    if printer.protocol_flavor == ProtocolFlavor.SDCP_V3_WEBSOCKET:
        return await printer.refresh_v3_status()
    return printer.refresh()


async def do_status(printers):
    for i, p in enumerate(printers):
        await refresh_live_status(p)
        status = p.status()
        current_status = status['status']
        print_status = status['printStatus']
        print(f"{p.addr[0]}:")
        print(f"    {p.describe()}")
        print(f"    Transport: {status['transport']} Protocol: {status['protocol']}")
        print(f"    Machine Status: {_safe_enum_name(CurrentStatus, current_status)}")
        print(f"    Print Status: {_safe_enum_name(PrintInfoStatus, print_status)}")
        print(f"    Layers: {status['currentLayer']}/{status['totalLayers']}")
        print(f"    File: {status['filename']}")

def do_status_full(printers):
    for i, p in enumerate(printers):
        pprint.pprint(p.desc)


async def do_watch(printer, interval=5, broadcast=None):
    await refresh_live_status(printer)
    status = printer.status()
    total_layers = max(status['totalLayers'], 1)
    title = status['filename'] if status['filename'] else printer.name
    with alive_bar(total=total_layers, manual=True, elapsed=False, title=title) as bar:
        while True:
            if printer.protocol_flavor == ProtocolFlavor.SDCP_V3_WEBSOCKET:
                await refresh_live_status(printer)
            else:
                printers = SaturnPrinter.find_printers(broadcast=broadcast)
                if len(printers) > 0:
                    printer = printers[0]

            status = printer.status()
            total_layers = max(status['totalLayers'], 1)
            pct = min(1.0, status['currentLayer'] / total_layers)
            bar(pct)

            if pct >= 1.0:
                break

            await asyncio.sleep(interval)

async def create_servers():
    mqtt, mqtt_port, mqtt_task = await create_mqtt_server()
    http, http_port, http_task = await create_http_server()

    return mqtt, http

async def do_print(printer, filename):
    if printer.protocol_flavor == ProtocolFlavor.SDCP_V3_WEBSOCKET:
        result = await printer.print_file(filename)
        if result:
            logging.info("Print command accepted")
            return
        logging.error("Print command was rejected by printer")
        sys.exit(1)

    mqtt, http = await create_servers()
    connected = await printer.connect(mqtt, http)
    if not connected:
        logging.error("Failed to connect to printer")
        sys.exit(1)

    result = await printer.print_file(filename)
    if result:
        logging.info("Print started")
    else:
        logging.error("Failed to start print")
        sys.exit(1)

async def do_upload(printer, filename, start_printing=False):
    if printer.protocol_flavor == ProtocolFlavor.SDCP_V3_WEBSOCKET:
        logging.error("Upload is not yet implemented for SDCP V3 printers in this tool")
        sys.exit(1)

    if not os.path.exists(filename):
        logging.error(f"{filename} does not exist")
        sys.exit(1)

    mqtt, http = await create_servers()
    connected = await printer.connect(mqtt, http)
    if not connected:
        logging.error("Failed to connect to printer")
        sys.exit(1)
    
    #await printer.upload_file(filename, start_printing=start_printing)
    upload_task = asyncio.create_task(printer.upload_file(filename, start_printing=start_printing))
    # grab the first one, because we want the file size
    basename = filename.split('\\')[-1].split('/')[-1]
    file_size = os.path.getsize(filename)
    with alive_bar(total=file_size, manual=True, elapsed=False, title=basename) as bar:
        while True:
            if printer.file_transfer_future is None:
                await asyncio.sleep(0.1)
                continue
            progress = await printer.file_transfer_future
            if progress[0] < 0:
                logging.error("File upload failed!")
                sys.exit(1)
            bar(progress[0] / progress[1])
            if progress[0] >= progress[1]:
                break
    await upload_task

async def do_pause_at_layer(printer, layer, interval=5):
    if layer < 0:
        logging.error("Layer must be >= 0")
        sys.exit(1)

    logging.info(f"Waiting for layer >= {layer}")

    while True:
        ok = await refresh_live_status(printer)
        if ok is False:
            logging.warning("No status response from printer while waiting")
            await asyncio.sleep(interval)
            continue

        status = printer.status()
        current_layer = status['currentLayer']
        total_layers = status['totalLayers']
        filename = status['filename']
        logging.info(f"Current layer: {current_layer}/{total_layers} file: {filename}")

        if current_layer >= layer:
            break

        await asyncio.sleep(interval)

    result = await printer.pause_print()
    ack = result.get('Ack', 0) if isinstance(result, dict) else 0
    if ack == 0:
        logging.info(f"Pause command acknowledged at layer {printer.status()['currentLayer']}")
    else:
        logging.error(f"Pause command rejected with Ack={ack}: {result}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(prog='cassini', description='ELEGOO Saturn printer control utility')
    parser.add_argument('-p', '--printer', help='ID of printer to target')
    parser.add_argument('--broadcast', help='Explicit broadcast IP address')
    parser.add_argument('--debug', help='Enable debug logging', action='store_true')

    subparsers = parser.add_subparsers(title="commands", dest="command", required=True)

    parser_status = subparsers.add_parser('status', help='Discover and display status of all printers')
    parser_status_full = subparsers.add_parser('status-full', help='Discover and display full status of all printers')

    parser_watch = subparsers.add_parser('watch', help='Continuously update the status of the selected printer')
    parser_watch.add_argument('--interval', type=int, help='Status update interval (seconds)', default=5)

    parser_pause_at_layer = subparsers.add_parser('pause-at-layer', help='Pause an active print when it reaches a target layer')
    parser_pause_at_layer.add_argument('layer', type=int, help='Target layer number to pause at (pause when current layer >= target)')
    parser_pause_at_layer.add_argument('--interval', type=int, help='Status polling interval in seconds while waiting', default=5)

    parser_upload = subparsers.add_parser('upload', help='Upload a file to the printer') 
    parser_upload.add_argument('--start-printing', help='Start printing after upload is complete', action='store_true')
    parser_upload.add_argument('filename', help='File to upload')

    parser_print = subparsers.add_parser('print', help='Start printing a file already present on the printer') 
    parser_print.add_argument('filename', help='File to print')

    parser_connect_mqtt = subparsers.add_parser('connect-mqtt', help='Connect printer to particular MQTT server')
    parser_connect_mqtt.add_argument('address', help='MQTT host and port, e.g. "192.168.1.33:1883" or "mqtt.local:1883"')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    printers = []
    printer = None
    broadcast = args.broadcast
    if args.printer:
        printer = SaturnPrinter.find_printer(args.printer)
        if printer is None:
            logging.error(f"No response from printer {args.printer}")
            sys.exit(1)
        printers = [printer]
    else:
        printers = SaturnPrinter.find_printers(broadcast=broadcast)
        if len(printers) == 0:
            logging.error("No printers found on network")
            sys.exit(1)
        printer = printers[0]

    if args.command == "status":
        asyncio.run(do_status(printers))
        sys.exit(0)

    if args.command == "status-full":
        do_status_full(printers)
        sys.exit(0)

    if args.command == "connect-mqtt":
        mqtt_host, mqtt_port = args.address.split(':')
        try:
            mqtt_host = socket.gethostbyname(mqtt_host)
        except socket.gaierror:
            pass
        for p in printers:
            p.connect_mqtt(mqtt_host, mqtt_port)

    if args.command == "watch":
        asyncio.run(do_watch(printer, interval=args.interval, broadcast=broadcast))
        sys.exit(0)

    if args.command == "pause-at-layer":
        asyncio.run(do_pause_at_layer(printer, args.layer, interval=args.interval))
        sys.exit(0)

    logging.info(f'Printer: {printer.describe()} ({printer.addr[0]})')
    if printer.busy and args.command in ["upload", "print"]:
        logging.error(f'Printer is busy (status: {printer.current_status})')
        sys.exit(1)

    if args.command == "upload":
        asyncio.run(do_upload(printer, args.filename, start_printing=args.start_printing))
    elif args.command == "print":
        asyncio.run(do_print(printer, args.filename))

main()
