"""Unit tests - no OPC UA server or Fabric needed."""
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from asyncua import ua

import opcua_to_eventstream as bridge


# ------------------------------------------------------------------ helpers
class StubBatch(list):
    def __init__(self, max_events: int):
        super().__init__()
        self.max_events = max_events

    def add(self, data):
        if len(self) >= self.max_events:
            raise ValueError("batch full")
        self.append(data)


class StubProducer:
    """Stands in for EventHubProducerClient."""

    def __init__(self, fail: bool = False, batch_size: int = 1000):
        self.fail, self.batch_size = fail, batch_size
        self.batches: list[list] = []

    async def create_batch(self):
        return StubBatch(self.batch_size)

    async def send_batch(self, batch):
        if self.fail:
            raise ConnectionError("Fabric unreachable")
        self.batches.append([json.loads(d.body_as_str()) for d in batch])

    @property
    def sent(self) -> list[dict]:
        return [e for b in self.batches for e in b]


def new_stats() -> dict:
    return {"received": 0, "sent": 0, "spooled": 0, "replayed": 0, "dropped": 0}


def machine(**kwargs) -> bridge.MachineConfig:
    return bridge.MachineConfig(machine_id="machine-01", endpoint="opc.tcp://localhost:50000",
                                nodes={"ns=3;s=SpikeData": "Temperature"}, **kwargs)


# ------------------------------------------------------------------ build_event
def test_build_event_uses_tag_name_and_utc_timestamps():
    data_value = SimpleNamespace(StatusCode=ua.StatusCode(ua.StatusCodes.Good),
                                 SourceTimestamp=datetime(2026, 10, 6, 3, 15, 2), ServerTimestamp=None)
    event = bridge.build_event(machine(), "RUN-1", "ns=3;s=SpikeData", 87.4, data_value)

    assert event["machineId"] == "machine-01"
    assert event["runId"] == "RUN-1"
    assert event["tag"] == "Temperature"
    assert event["value"] == 87.4
    assert event["statusGood"] is True
    assert event["sourceTimestamp"] == "2026-10-06T03:15:02+00:00"
    assert datetime.fromisoformat(event["ingestTimestamp"]).tzinfo is not None


def test_build_event_bad_status_and_unknown_node():
    data_value = SimpleNamespace(StatusCode=ua.StatusCode(ua.StatusCodes.BadNodeIdUnknown),
                                 SourceTimestamp=None, ServerTimestamp=datetime(2026, 1, 1, tzinfo=timezone.utc))
    event = bridge.build_event(machine(), None, "ns=9;s=Other", None, data_value)

    assert event["statusGood"] is False
    assert event["tag"] == "ns=9;s=Other"  # falls back to the NodeId
    assert event["sourceTimestamp"].startswith("2026-01-01T00:00:00")


# ------------------------------------------------------------------ load_config
@pytest.fixture
def clean_env(monkeypatch, tmp_path):
    for name in ("MACHINES_FILE", "OPCUA_URL", "MACHINE_ID", "NODE_MAP_JSON", "OPCUA_USER", "OPCUA_PASSWORD",
                 "OPCUA_SECURITY", "SAMPLING_INTERVAL_MS", "MONITORED_QUEUE_SIZE", "RUN_ID", "USE_WEBSOCKETS",
                 "EVENTSTREAM_CONNECTION_STRING", "EVENTSTREAM_NAMESPACE", "EVENTSTREAM_HUB", "SEND_RETRIES"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)  # no stray .env file
    return monkeypatch


def test_load_config_single_machine_defaults(clean_env):
    cfg = bridge.load_config()

    assert len(cfg.machines) == 1
    m = cfg.machines[0]
    assert m.endpoint == "opc.tcp://localhost:50000"
    assert m.nodes == bridge.DEFAULT_NODES
    assert m.sampling_interval_ms is None and m.queue_size == 1
    assert cfg.use_websockets is True and cfg.send_retries == 3


def test_load_config_single_machine_from_env(clean_env):
    clean_env.setenv("NODE_MAP_JSON", '{"ns=2;s=A": "Power"}')
    clean_env.setenv("SAMPLING_INTERVAL_MS", "100")
    clean_env.setenv("MONITORED_QUEUE_SIZE", "10")
    clean_env.setenv("USE_WEBSOCKETS", "false")
    clean_env.setenv("RUN_ID", "DOE-1")
    cfg = bridge.load_config()

    m = cfg.machines[0]
    assert m.nodes == {"ns=2;s=A": "Power"}
    assert m.sampling_interval_ms == 100.0 and m.queue_size == 10
    assert cfg.use_websockets is False and cfg.run_id == "DOE-1"


def test_load_config_machines_file_reads_secrets_from_env(clean_env, tmp_path):
    (tmp_path / "machines.json").write_text(json.dumps({"machines": [{
        "machineId": "machine-02", "endpoint": "opc.tcp://host:4840", "nodes": {"ns=2;s=B": "Speed"},
        "usernameEnv": "M02_USER", "passwordEnv": "M02_PASSWORD", "samplingIntervalMs": 250, "queueSize": 4,
    }]}))
    clean_env.setenv("MACHINES_FILE", str(tmp_path / "machines.json"))
    clean_env.setenv("M02_USER", "operator")
    clean_env.setenv("M02_PASSWORD", "from-env")
    m = bridge.load_config().machines[0]

    assert (m.machine_id, m.username, m.password) == ("machine-02", "operator", "from-env")
    assert m.sampling_interval_ms == 250.0 and m.queue_size == 4


def test_example_machines_file_is_valid(clean_env):
    root = Path(__file__).resolve().parents[1]
    clean_env.setenv("MACHINES_FILE", str(root / "config" / "machines.example.json"))
    cfg = bridge.load_config()
    assert [m.machine_id for m in cfg.machines] == ["machine-01", "machine-02"]


# ------------------------------------------------------------------ sending
def test_send_events_splits_full_batches():
    producer = StubProducer(batch_size=2)
    events = [{"n": i} for i in range(5)]
    asyncio.run(bridge.send_events(producer, events))

    assert [len(b) for b in producer.batches] == [2, 2, 1]
    assert producer.sent == events


def test_run_without_fabric_settings_fails_fast(clean_env):
    cfg = bridge.load_config()
    with pytest.raises(SystemExit):
        asyncio.run(bridge.run(cfg, duration=1))


# ------------------------------------------------------------------ spool (store and forward)
def test_spool_replays_in_chunks_and_deletes_file(tmp_path):
    spool = bridge.Spool(tmp_path / "spool.jsonl", max_mb=10)
    events = [{"n": i} for i in range(7)]
    assert spool.append(events)
    producer, stats = StubProducer(), new_stats()

    asyncio.run(bridge.replay_spool(producer, spool, stats, max_events=3))
    assert producer.sent == events[:3] and spool.pending()

    while spool.pending():
        asyncio.run(bridge.replay_spool(producer, spool, stats, max_events=3))
    assert producer.sent == events
    assert stats["replayed"] == 7


def test_spool_skips_corrupt_lines(tmp_path):
    path = tmp_path / "spool.jsonl"
    path.write_text('{"n": 1}\n{"n": 2, "trunc\n{"n": 3}\n')  # a line cut short by a crash
    spool, producer, stats = bridge.Spool(path, max_mb=10), StubProducer(), new_stats()

    asyncio.run(bridge.replay_spool(producer, spool, stats))

    assert producer.sent == [{"n": 1}, {"n": 3}]
    assert stats["dropped"] == 1
    assert not path.exists()


def test_spool_append_after_truncated_line_keeps_next_event(tmp_path):
    path = tmp_path / "spool.jsonl"
    path.write_text('{"n": 1}\n{"n": 2, "trunc')  # no trailing newline
    spool = bridge.Spool(path, max_mb=10)
    spool.append([{"n": 3}])
    events, _, bad = spool.read_chunk(10)

    assert events == [{"n": 1}, {"n": 3}] and bad == 1


def test_spool_keeps_events_when_replay_fails(tmp_path):
    spool = bridge.Spool(tmp_path / "spool.jsonl", max_mb=10)
    spool.append([{"n": 1}, {"n": 2}])
    stats = new_stats()

    with pytest.raises(ConnectionError):
        asyncio.run(bridge.replay_spool(StubProducer(fail=True), spool, stats))
    assert spool.pending() and stats["replayed"] == 0

    asyncio.run(bridge.replay_spool(StubProducer(), spool, stats))
    assert not spool.pending() and stats["replayed"] == 2


def test_spool_full_drops_events(tmp_path):
    spool = bridge.Spool(tmp_path / "spool.jsonl", max_mb=0)
    stats = new_stats()
    bridge.spool_events([{"n": 1}], spool, stats)

    assert stats["dropped"] == 1 and not spool.pending()


def test_deliver_spools_on_failure_then_pauses_fabric_calls(tmp_path):
    async def scenario():
        spool, stats = bridge.Spool(tmp_path / "spool.jsonl", max_mb=10), new_stats()
        failing = StubProducer(fail=True)
        await bridge.deliver([{"n": 1}], failing, spool, False, stats)
        await bridge.deliver([{"n": 2}], failing, spool, False, stats)  # paused: spooled without a send
        return spool, stats

    spool, stats = asyncio.run(scenario())
    assert stats["spooled"] == 2 and stats["sent"] == 0
    events, _, _ = spool.read_chunk(10)
    assert events == [{"n": 1}, {"n": 2}]


def test_deliver_replays_spool_after_successful_send(tmp_path):
    spool, stats = bridge.Spool(tmp_path / "spool.jsonl", max_mb=10), new_stats()
    spool.append([{"n": 1}])
    producer = StubProducer()

    asyncio.run(bridge.deliver([{"n": 2}], producer, spool, False, stats))

    assert producer.sent == [{"n": 2}, {"n": 1}]
    assert stats["sent"] == 1 and stats["replayed"] == 1 and not spool.pending()
