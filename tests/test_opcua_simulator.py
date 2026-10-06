"""Integration tests against an OPC UA server, such as the OPC PLC simulator (docker compose up -d).

Skipped unless OPCUA_TEST_URL is set, e.g.  OPCUA_TEST_URL=opc.tcp://localhost:50000 pytest
"""
import asyncio
import os

import pytest

import opcua_to_eventstream as bridge

URL = os.getenv("OPCUA_TEST_URL")
pytestmark = pytest.mark.skipif(not URL, reason="set OPCUA_TEST_URL to run against an OPC UA server")


def simulator_machine() -> bridge.MachineConfig:
    return bridge.MachineConfig(machine_id="machine-01", endpoint=URL, nodes=dict(bridge.DEFAULT_NODES))


def test_browse_skips_restricted_nodes_and_finds_simulator_tags():
    rows = asyncio.run(bridge.browse_variables(simulator_machine(), root="ns=3;s=OpcPlc", max_nodes=1000))
    node_ids = {r["nodeId"] for r in rows}

    assert set(bridge.DEFAULT_NODES) <= node_ids


def test_browse_from_objects_completes():
    rows = asyncio.run(bridge.browse_variables(simulator_machine(), max_nodes=200))
    assert len(rows) == 200


def test_read_current_values():
    rows = asyncio.run(bridge.read_current_values(simulator_machine()))

    assert len(rows) == len(bridge.DEFAULT_NODES)
    assert all("error" not in r and r["statusGood"] for r in rows)


def test_dry_run_streams_events(tmp_path, capsys):
    cfg = bridge.BridgeConfig(machines=[simulator_machine()], run_id="CI-RUN",
                              spool_file=tmp_path / "unsent.jsonl")
    stats = asyncio.run(bridge.run(cfg, duration=6, dry_run=True))

    assert stats["received"] > 0 and stats["sent"] == stats["received"] and stats["dropped"] == 0
    first = capsys.readouterr().out.splitlines()[0]
    assert '"runId": "CI-RUN"' in first and '"machineId": "machine-01"' in first
