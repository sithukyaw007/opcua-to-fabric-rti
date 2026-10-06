#!/usr/bin/env python3
"""
opcua_to_eventstream.py
=======================

Sample bridge that streams OPC UA machine data into Microsoft Fabric Real-Time
Intelligence through an Eventstream *custom endpoint* source.

    Machines (OPC UA servers) --OPC UA--> this bridge (inside the shop-floor network)
                                              |
                                              +--outbound TLS (443)--> Fabric Eventstream --> Eventhouse

* Runs on a small PC / VM inside the machine network (e.g. the shop-floor VLAN).
* All connections are OUTBOUND: OPC UA to the machines, AMQP over WebSockets (443) to Fabric.
* One event is sent per OPC UA value change (JSON), tagged with machineId and runId so
  telemetry can be joined with experiment (DOE) runs and specimens in the silver layer.
* Reconnects automatically to OPC UA servers; spools events to a local file when Fabric
  cannot be reached and replays them later.

SAMPLE CODE - provided as-is under the MIT License for learning and prototyping. It is not an
official Microsoft product or sample, is not supported, and is not production-hardened.
Review security, reliability and monitoring before use.

Usage
-----
    pip install -r requirements.txt
    python src/opcua_to_eventstream.py --dry-run --duration 30   # test OPC UA only (prints events)
    python src/opcua_to_eventstream.py                           # stream to Fabric
    python src/opcua_to_eventstream.py --browse                  # list variables / NodeIds on the server
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import socket
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:  # optional: load settings from a .env file
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

from asyncua import Client, ua
from azure.eventhub import EventData, TransportType
from azure.eventhub.aio import EventHubProducerClient

log = logging.getLogger("opcua-bridge")

# Default tags = OPC PLC simulator nodes (https://github.com/Azure-Samples/iot-edge-opc-plc).
# The namespace index (ns=3) can differ per server - use --browse to confirm NodeIds.
DEFAULT_NODES = {
    "ns=3;s=SpikeData": "Temperature",
    "ns=3;s=PositiveTrendData": "Pressure",
    "ns=3;s=FastUInt1": "SpindleSpeed",
    "ns=3;s=AlternatingBoolean": "HeaterOn",
}
SERVER_STATE_NODE = "i=2259"  # Server_ServerStatus_State - used as a keep-alive read


# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------
@dataclass
class MachineConfig:
    machine_id: str
    endpoint: str
    nodes: dict[str, str]                  # NodeId -> friendly tag name
    username: Optional[str] = None
    password: Optional[str] = None
    security: Optional[str] = None         # "Policy,Mode,client_cert,client_key[,server_cert]"
    publishing_interval_ms: int = 1000
    sampling_interval_ms: Optional[float] = None  # default: same as the publishing interval
    queue_size: int = 1                    # values kept per tag between publishes (1 = latest only)


@dataclass
class BridgeConfig:
    machines: list[MachineConfig]
    run_id: Optional[str] = None
    connection_string: Optional[str] = None  # SAS: Eventstream > custom endpoint > Event Hub > SAS Key Authentication
    namespace: Optional[str] = None          # Entra ID: "<name>.servicebus.windows.net"
    eventhub: Optional[str] = None           # Entra ID: event hub name
    use_websockets: bool = True              # True = AMQP over WebSockets (443); False = AMQP (5671)
    send_retries: int = 3                    # SDK retries per send before events are spooled
    batch_window_s: float = 1.0
    max_batch_events: int = 500
    queue_size: int = 100_000
    spool_file: Path = field(default_factory=lambda: Path("unsent_events.jsonl"))
    spool_max_mb: int = 500


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in ("1", "true", "yes", "y")


def _opt_env(name: Optional[str]) -> Optional[str]:
    return os.getenv(name) if name else None


def _opt_float(value: Any) -> Optional[float]:
    return float(value) if value not in (None, "") else None


def load_config(env_file: Optional[str] = None) -> BridgeConfig:
    """Build the configuration from environment variables (and an optional .env file).

    Several machines: set MACHINES_FILE to a JSON file (see config/machines.example.json).
    One machine:      set OPCUA_URL, MACHINE_ID and NODE_MAP_JSON (or use the simulator defaults).
    """
    if load_dotenv is not None:
        load_dotenv(env_file) if env_file else load_dotenv()

    machines_file = os.getenv("MACHINES_FILE")
    if machines_file:
        raw = json.loads(Path(machines_file).read_text(encoding="utf-8"))
        machines = [
            MachineConfig(
                machine_id=m["machineId"],
                endpoint=m["endpoint"],
                nodes=m["nodes"],
                username=_opt_env(m.get("usernameEnv")),   # secrets come from env vars, not the file
                password=_opt_env(m.get("passwordEnv")),
                security=m.get("security"),
                publishing_interval_ms=int(m.get("publishingIntervalMs", 1000)),
                sampling_interval_ms=_opt_float(m.get("samplingIntervalMs")),
                queue_size=int(m.get("queueSize", 1)),
            )
            for m in raw["machines"]
        ]
    else:
        machines = [
            MachineConfig(
                machine_id=os.getenv("MACHINE_ID", "machine-01"),
                endpoint=os.getenv("OPCUA_URL", "opc.tcp://localhost:50000"),
                nodes=json.loads(os.getenv("NODE_MAP_JSON") or "null") or dict(DEFAULT_NODES),
                username=os.getenv("OPCUA_USER") or None,
                password=os.getenv("OPCUA_PASSWORD") or None,
                security=os.getenv("OPCUA_SECURITY") or None,
                publishing_interval_ms=int(os.getenv("PUBLISHING_INTERVAL_MS", "1000")),
                sampling_interval_ms=_opt_float(os.getenv("SAMPLING_INTERVAL_MS")),
                queue_size=int(os.getenv("MONITORED_QUEUE_SIZE", "1")),
            )
        ]

    return BridgeConfig(
        machines=machines,
        run_id=os.getenv("RUN_ID") or None,
        connection_string=os.getenv("EVENTSTREAM_CONNECTION_STRING") or None,
        namespace=os.getenv("EVENTSTREAM_NAMESPACE") or None,
        eventhub=os.getenv("EVENTSTREAM_HUB") or None,
        use_websockets=_env_bool("USE_WEBSOCKETS", True),
        send_retries=int(os.getenv("SEND_RETRIES", "3")),
        batch_window_s=float(os.getenv("BATCH_WINDOW_S", "1.0")),
        max_batch_events=int(os.getenv("MAX_BATCH_EVENTS", "500")),
        queue_size=int(os.getenv("QUEUE_SIZE", "100000")),
        spool_file=Path(os.getenv("SPOOL_FILE", "unsent_events.jsonl")),
        spool_max_mb=int(os.getenv("SPOOL_MAX_MB", "500")),
    )


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _iso(ts: Optional[datetime]) -> Optional[str]:
    """asyncua returns naive UTC datetimes - make them explicit ISO 8601 UTC strings."""
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.isoformat()


def build_event(machine: MachineConfig, run_id: Optional[str], node_id: str,
                value: Any, data_value: Any) -> dict:
    """Shape of one event sent to Fabric (one row in the bronze table)."""
    status = getattr(data_value, "StatusCode", None)
    source_ts = getattr(data_value, "SourceTimestamp", None) or getattr(data_value, "ServerTimestamp", None)
    return {
        "machineId": machine.machine_id,
        "runId": run_id,
        "gatewayId": socket.gethostname(),
        "nodeId": node_id,
        "tag": machine.nodes.get(node_id, node_id),
        "value": value,
        "statusGood": status.is_good() if status is not None else True,
        "sourceTimestamp": _iso(source_ts) or datetime.now(timezone.utc).isoformat(),
        "ingestTimestamp": datetime.now(timezone.utc).isoformat(),
    }


def _new_client(machine: MachineConfig) -> Client:
    client = Client(url=machine.endpoint, timeout=10)
    if machine.username:
        client.set_user(machine.username)
        client.set_password(machine.password or "")
    return client


# --------------------------------------------------------------------------------------
# OPC UA side
# --------------------------------------------------------------------------------------
class DataChangeHandler:
    """Receives OPC UA data-change notifications and puts events on the local queue."""

    def __init__(self, machine: MachineConfig, run_id: Optional[str],
                 queue: asyncio.Queue, stats: dict):
        self.machine, self.run_id, self.queue, self.stats = machine, run_id, queue, stats

    def datachange_notification(self, node, val, data):
        node_id = node.nodeid.to_string()
        event = build_event(self.machine, self.run_id, node_id, val, data.monitored_item.Value)
        try:
            self.queue.put_nowait(event)
            self.stats["received"] += 1
        except asyncio.QueueFull:
            self.stats["dropped"] += 1
            log.warning("Local queue full - dropping value for %s/%s", self.machine.machine_id, node_id)

    def status_change_notification(self, status):
        log.warning("OPC UA subscription status changed on %s: %s", self.machine.machine_id, status)


async def opcua_loop(machine: MachineConfig, cfg: BridgeConfig, queue: asyncio.Queue,
                     stop: asyncio.Event, stats: dict) -> None:
    """Connect, subscribe and keep the session alive; reconnect with back-off on failure."""
    backoff = 2
    while not stop.is_set():
        client = _new_client(machine)
        try:
            if machine.security:
                await client.set_security_string(machine.security)
            async with client:
                log.info("Connected to %s (%s)", machine.machine_id, machine.endpoint)
                handler = DataChangeHandler(machine, cfg.run_id, queue, stats)
                subscription = await client.create_subscription(machine.publishing_interval_ms, handler)
                node_ids = list(machine.nodes)
                results = await subscription.subscribe_data_change(
                    [client.get_node(n) for n in node_ids],
                    queuesize=machine.queue_size,
                    sampling_interval=machine.sampling_interval_ms or float(machine.publishing_interval_ms),
                )
                results = results if isinstance(results, list) else [results]
                for node_id, result in zip(node_ids, results):
                    if not isinstance(result, int):  # a StatusCode is returned for a failed item
                        log.warning("Could not subscribe to %s on %s: %s", node_id, machine.machine_id,
                                    getattr(result, "name", result))
                backoff = 2
                state_node = client.get_node(SERVER_STATE_NODE)
                while not stop.is_set():
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=5)
                    except asyncio.TimeoutError:
                        await state_node.read_value()  # raises if the session has dropped
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # network drop, server restart, security error ...
            log.warning("OPC UA problem on %s (%s) - retrying in %ss", machine.machine_id, exc, backoff)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            backoff = min(backoff * 2, 60)


async def browse_variables(machine: MachineConfig, max_depth: int = 4, max_nodes: int = 300,
                           root: Optional[str] = None) -> list[dict]:
    """List variables (NodeId, browse path, current value) to help build the tag list.

    Starts at the Objects folder, or at `root` (a NodeId such as "ns=3;s=OpcPlc").
    Nodes that can't be browsed or read (for example because of access rights or the
    security mode) are skipped, so one restricted node doesn't stop the whole browse.
    """
    client = _new_client(machine)
    if machine.security:
        await client.set_security_string(machine.security)
    results: list[dict] = []
    visited: set[str] = set()
    wanted = ua.NodeClass.Object | ua.NodeClass.Variable

    async def walk(node, depth: int, path: str) -> None:
        if depth > max_depth or len(results) >= max_nodes:
            return
        try:
            children = await node.get_children_descriptions(nodeclassmask=wanted)
        except Exception as exc:
            log.debug("Cannot browse %s (%s)", path, exc)
            return
        for ref in children:
            if len(results) >= max_nodes:
                return
            node_id = ref.NodeId.to_string()
            name = ref.BrowseName.Name
            if node_id in visited or (depth == 0 and name == "Server"):  # skip cycles and diagnostics
                continue
            visited.add(node_id)
            child = client.get_node(ref.NodeId)
            child_path = f"{path}/{name}"
            if ref.NodeClass == ua.NodeClass.Variable:
                try:
                    value = await child.read_value()
                except Exception:
                    value = "<unreadable>"
                results.append({"nodeId": node_id, "path": child_path, "value": value})
            elif ref.NodeClass == ua.NodeClass.Object:
                await walk(child, depth + 1, child_path)

    async with client:
        start = client.get_node(root) if root else client.nodes.objects
        await walk(start, 0, root or "Objects")
    return results


async def read_current_values(machine: MachineConfig) -> list[dict]:
    """Read the configured tags once - a quick connectivity check."""
    client = _new_client(machine)
    if machine.security:
        await client.set_security_string(machine.security)
    rows = []
    async with client:
        for node_id, tag in machine.nodes.items():
            try:
                dv = await client.get_node(node_id).read_data_value()
                rows.append(build_event(machine, None, node_id, dv.Value.Value, dv))
            except Exception as exc:
                rows.append({"machineId": machine.machine_id, "nodeId": node_id, "tag": tag, "error": str(exc)})
    return rows


# --------------------------------------------------------------------------------------
# Fabric side
# --------------------------------------------------------------------------------------
def _check_fabric_settings(cfg: BridgeConfig) -> None:
    if not (cfg.connection_string or (cfg.namespace and cfg.eventhub)):
        raise SystemExit("Set EVENTSTREAM_CONNECTION_STRING, or EVENTSTREAM_NAMESPACE and EVENTSTREAM_HUB.")


@contextlib.asynccontextmanager
async def open_producer(cfg: BridgeConfig):
    """Event Hubs producer for the Eventstream custom endpoint, closed (with its credential) on exit.

    SAS connection string (quick start) or Microsoft Entra ID (recommended for production).
    """
    _check_fabric_settings(cfg)
    options: dict[str, Any] = {
        "transport_type": TransportType.AmqpOverWebsocket if cfg.use_websockets else TransportType.Amqp,
        "retry_total": cfg.send_retries,
    }
    credential = None
    if cfg.connection_string:
        producer = EventHubProducerClient.from_connection_string(cfg.connection_string, **options)
    else:
        # Uses AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET (service principal),
        # a managed identity or a developer sign-in such as the Azure CLI, in that order.
        from azure.identity.aio import DefaultAzureCredential
        credential = DefaultAzureCredential()
        producer = EventHubProducerClient(
            fully_qualified_namespace=cfg.namespace,
            eventhub_name=cfg.eventhub,
            credential=credential,
            **options,
        )
    try:
        async with producer:
            yield producer
    finally:
        if credential is not None:
            await credential.close()


async def send_events(producer: EventHubProducerClient, events: list[dict]) -> None:
    """Send events in as few batches as possible (each batch is limited to ~1 MB)."""
    batch = await producer.create_batch()
    for event in events:
        data = EventData(json.dumps(event, default=str))
        data.content_type = "application/json"
        try:
            batch.add(data)
        except ValueError:  # batch is full - send it and start a new one
            await producer.send_batch(batch)
            batch = await producer.create_batch()
            batch.add(data)
    if len(batch):
        await producer.send_batch(batch)


async def send_test_event(cfg: BridgeConfig) -> dict:
    """Send one synthetic event - checks the Fabric connection without OPC UA."""
    machine = cfg.machines[0]
    event = {
        "machineId": machine.machine_id, "runId": cfg.run_id, "gatewayId": socket.gethostname(),
        "nodeId": "test", "tag": "ConnectivityTest", "value": 1, "statusGood": True,
        "sourceTimestamp": datetime.now(timezone.utc).isoformat(),
        "ingestTimestamp": datetime.now(timezone.utc).isoformat(),
    }
    async with open_producer(cfg) as producer:
        await send_events(producer, [event])
    return event


class Spool:
    """Local store-and-forward file with one JSON event per line.

    Only the sender task reads and writes it. Replay happens in chunks so live data keeps
    flowing, and unreadable lines (for example after a power cut mid-write) are skipped.
    """

    def __init__(self, path: Path, max_mb: int):
        self.path = path
        self.max_bytes = max_mb * 1_048_576
        self.offset = 0          # bytes already replayed by this process
        self.pause_until = 0.0   # event-loop time before which Fabric isn't contacted again

    def pending(self) -> bool:
        return self.path.exists()

    def append(self, events: list[dict]) -> bool:
        """Append events. Returns False (nothing written) when the size limit is reached."""
        size = self.path.stat().st_size if self.path.exists() else 0
        if size >= self.max_bytes:
            return False
        with self.path.open("ab") as f:
            if size and not self._ends_with_newline():
                f.write(b"\n")  # close a line that was cut short, so the next event stays readable
            for event in events:
                f.write((json.dumps(event, default=str) + "\n").encode("utf-8"))
        return True

    def _ends_with_newline(self) -> bool:
        with self.path.open("rb") as f:
            f.seek(-1, os.SEEK_END)
            return f.read(1) == b"\n"

    def read_chunk(self, max_events: int) -> tuple[list[dict], int, int]:
        """Read up to max_events from the replay position. Returns (events, next_offset, bad_lines)."""
        events: list[dict] = []
        bad = 0
        with self.path.open("rb") as f:
            f.seek(self.offset)
            offset = self.offset
            while len(events) < max_events:
                line = f.readline()
                if not line:
                    break
                offset += len(line)
                if not line.strip():
                    continue
                try:
                    events.append(json.loads(line))
                except ValueError:
                    bad += 1
        return events, offset, bad

    def commit(self, offset: int) -> None:
        """Mark everything before offset as sent, and delete the file once it is all sent."""
        self.offset = offset
        if self.offset >= self.path.stat().st_size:
            self.path.unlink()
            self.offset = 0


REPLAY_CHUNK_EVENTS = 5_000   # spooled events resent per step, between live batches
FABRIC_RETRY_PAUSE_S = 30     # after a failed send, spool straight away for this long


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip()
    return text.splitlines()[0] if text else type(exc).__name__


def spool_events(events: list[dict], spool: Spool, stats: dict) -> None:
    if spool.append(events):
        stats["spooled"] += len(events)
    else:
        stats["dropped"] += len(events)
        log.error("Spool file is full (%s) - dropping %d events", spool.path, len(events))


async def replay_spool(producer: EventHubProducerClient, spool: Spool, stats: dict,
                       max_events: int = REPLAY_CHUNK_EVENTS) -> None:
    """Resend one chunk of spooled events. Duplicates are possible after a partial failure or
    a restart - the silver layer removes them (see kql/medallion.kql)."""
    if not spool.pending():
        return
    events, next_offset, bad = spool.read_chunk(max_events)
    if events:
        await send_events(producer, events)
        stats["replayed"] += len(events)
        log.info("Replayed %d spooled events", len(events))
    if bad:
        stats["dropped"] += bad
        log.warning("Skipped %d unreadable line(s) in %s", bad, spool.path)
    spool.commit(next_offset)


async def try_replay(producer: EventHubProducerClient, spool: Spool, stats: dict) -> None:
    loop = asyncio.get_running_loop()
    if not spool.pending() or loop.time() < spool.pause_until:
        return
    try:
        await replay_spool(producer, spool, stats)
    except Exception as exc:
        spool.pause_until = loop.time() + FABRIC_RETRY_PAUSE_S
        log.warning("Could not replay spooled events yet (%s)", _first_line(exc))


async def deliver(events: list[dict], producer: Optional[EventHubProducerClient],
                  spool: Spool, dry_run: bool, stats: dict) -> None:
    if dry_run or producer is None:
        for event in events:
            print(json.dumps(event, default=str))
        stats["sent"] += len(events)
        return
    loop = asyncio.get_running_loop()
    if loop.time() < spool.pause_until:  # Fabric was unreachable moments ago - don't wait on retries
        spool_events(events, spool, stats)
        return
    try:
        await send_events(producer, events)
    except Exception as exc:  # the SDK has already retried - keep the data locally
        spool.pause_until = loop.time() + FABRIC_RETRY_PAUSE_S
        log.error("Send failed (%s) - spooling %d events, next attempt in %ds",
                  _first_line(exc), len(events), FABRIC_RETRY_PAUSE_S)
        spool_events(events, spool, stats)
        return
    stats["sent"] += len(events)
    log.info("Sent %d events to Fabric Eventstream", len(events))
    await try_replay(producer, spool, stats)


async def sender_loop(queue: asyncio.Queue, cfg: BridgeConfig, stop: asyncio.Event,
                      dry_run: bool, stats: dict) -> None:
    """Collect events for up to BATCH_WINDOW_S seconds, then send them. Drains the queue on stop."""
    loop = asyncio.get_running_loop()
    spool = Spool(cfg.spool_file, cfg.spool_max_mb)
    async with contextlib.AsyncExitStack() as stack:
        producer = None if dry_run else await stack.enter_async_context(open_producer(cfg))
        while not (stop.is_set() and queue.empty()):
            try:
                events = [await asyncio.wait_for(queue.get(), timeout=0.5)]
            except asyncio.TimeoutError:
                if producer is not None and not stop.is_set():
                    await try_replay(producer, spool, stats)  # idle - work through the spool
                continue
            deadline = loop.time() + cfg.batch_window_s
            while len(events) < cfg.max_batch_events:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    events.append(await asyncio.wait_for(queue.get(), timeout=remaining))
                except asyncio.TimeoutError:
                    break
            await deliver(events, producer, spool, dry_run, stats)


async def run(cfg: BridgeConfig, duration: Optional[float] = None, dry_run: bool = False,
              stop: Optional[asyncio.Event] = None) -> dict:
    """Run the bridge until `stop` is set, `duration` seconds pass, or the task is cancelled."""
    if not dry_run:
        _check_fabric_settings(cfg)  # fail fast, before connecting to the machines
    stop = stop or asyncio.Event()
    stats = {"received": 0, "sent": 0, "spooled": 0, "replayed": 0, "dropped": 0}
    queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.queue_size)

    readers = [asyncio.create_task(opcua_loop(m, cfg, queue, stop, stats)) for m in cfg.machines]
    sender = asyncio.create_task(sender_loop(queue, cfg, stop, dry_run, stats))
    stop_wait = asyncio.create_task(stop.wait())
    log.info("Bridge started for %d machine(s)%s", len(cfg.machines), " [dry run]" if dry_run else "")
    try:
        await asyncio.wait({stop_wait, sender}, timeout=duration, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stop.set()                                   # readers disconnect, sender drains the queue
        stop_wait.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
        (sender_error,) = await asyncio.gather(sender, return_exceptions=True)
        log.info("Bridge stopped: %s", stats)
    if isinstance(sender_error, Exception):
        raise sender_error
    return stats


class _HideSdkSocketNoise(logging.Filter):
    """The Event Hubs WebSocket transport can leave aiohttp sessions open after a failed
    connection attempt, and asyncio then logs "Unclosed client session". It is harmless."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not str(record.msg).startswith(("Unclosed client session", "Unclosed connector"))


def hide_known_sdk_noise() -> None:
    logging.getLogger("asyncua").setLevel(logging.WARNING)
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("asyncio").addFilter(_HideSdkSocketNoise())


# --------------------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Stream OPC UA machine data to a Fabric Eventstream.")
    parser.add_argument("--env-file", help="Path to a .env file (default: .env in the current folder)")
    parser.add_argument("--dry-run", action="store_true", help="Print events instead of sending to Fabric")
    parser.add_argument("--duration", type=float, help="Stop after N seconds (default: run until Ctrl+C)")
    parser.add_argument("--browse", action="store_true", help="List variables on the first machine and exit")
    parser.add_argument("--browse-root", help="NodeId to start browsing from (default: the Objects folder)")
    parser.add_argument("--max-nodes", type=int, default=1000, help="Maximum variables to list with --browse")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    hide_known_sdk_noise()
    cfg = load_config(args.env_file)

    if args.browse:
        rows = asyncio.run(browse_variables(cfg.machines[0], max_nodes=args.max_nodes, root=args.browse_root))
        for row in rows:
            print(f"{row['nodeId']:<45} {row['path']:<60} {row['value']!r}")
        if len(rows) >= args.max_nodes:
            log.warning("Stopped at %d variables - use --browse-root or --max-nodes to see more", args.max_nodes)
        return

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(cfg, duration=args.duration, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
