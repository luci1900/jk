"""Unit tests with ops.testing (Scenario); run by `make test-charm` or `python -m pytest`."""

import pathlib
import sys

import ops
import yaml
from ops import testing

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
from charm import JKTestClientCharm  # noqa: E402

META = pathlib.Path(__file__).parent.parent


def ctx():
    return testing.Context(
        JKTestClientCharm,
        meta=yaml.safe_load((META / "metadata.yaml").read_text()),
        config=yaml.safe_load((META / "config.yaml").read_text()),
    )


def web(connect=True):
    return testing.Container("web", can_connect=connect)


def source(rid=5, local=None, app=None, remote_app=None, remotes=None):
    return testing.Relation(
        "source",
        interface="jk_test_data",
        id=rid,
        remote_app_name="jk-test",
        local_unit_data=local or {},
        local_app_data=app or {},
        remote_app_data=remote_app or {},
        remote_units_data=remotes if remotes is not None else {0: {"address": "10.0.0.1"}},
    )


def test_not_related_is_active_with_no_providers():
    c = web()
    out = ctx().run(ctx().on.config_changed(), testing.State(containers=[c]))
    assert out.unit_status == ops.ActiveStatus("providers: 0")
    assert out.opened_ports == frozenset({testing.TCPPort(8080)})


def test_not_connected_waits():
    c = web(connect=False)
    out = ctx().run(ctx().on.config_changed(), testing.State(containers=[c]))
    assert out.unit_status == ops.WaitingStatus("waiting for pebble")


def test_joined_publishes_unit_data_and_reads_provider_data():
    c = web()
    rel = source(remote_app={"provider-app": "jk-test", "generation": "3"}, remotes={0: {"address": "a"}, 1: {"address": "b"}})
    out = ctx().run(ctx().on.relation_joined(rel, remote_unit=1), testing.State(containers=[c], relations=[rel]))
    data = out.get_relation(5).local_unit_data
    assert data["client-unit"] == "jk-test-client/0"
    assert data["hello"] == "hello from jk-test-client/0"
    assert data["address"]
    assert data["seen-units"] == "jk-test/0,jk-test/1"
    assert data["seen-provider-app"] == "jk-test"
    assert data["seen-generation"] == "3"
    assert out.get_relation(5).local_app_data == {}  # not the leader
    assert out.unit_status == ops.ActiveStatus("providers: 2")


def test_leader_writes_app_data():
    c = web()
    rel = source()
    out = ctx().run(ctx().on.relation_changed(rel, remote_unit=0), testing.State(containers=[c], relations=[rel], leader=True))
    assert out.get_relation(5).local_app_data == {"client-app": "jk-test-client"}
    assert out.app_status == ops.ActiveStatus()


def test_changed_with_the_same_data_leaves_it_alone():
    c = web()
    rel = source()
    first = ctx().run(ctx().on.relation_changed(rel, remote_unit=0), testing.State(containers=[c], relations=[rel]))
    rel2 = source(local=dict(first.get_relation(5).local_unit_data))
    again = ctx().run(ctx().on.relation_changed(rel2, remote_unit=0), testing.State(containers=[c], relations=[rel2]))
    assert again.get_relation(5).local_unit_data == first.get_relation(5).local_unit_data


def test_departed_and_broken_drop_the_providers():
    c = web()
    rel = source(remotes={0: {}, 1: {}})
    out = ctx().run(ctx().on.relation_departed(rel, remote_unit=1, departing_unit=1), testing.State(containers=[c], relations=[rel]))
    assert out.unit_status.name == "active"
    out = ctx().run(ctx().on.relation_broken(rel), testing.State(containers=[c], relations=[rel]))
    assert out.unit_status == ops.ActiveStatus("providers: 0")
    assert out.get_relation(5).local_unit_data == {}  # nothing is written into a broken relation


def test_reads_the_granted_secret():
    c = web()
    secret = testing.Secret(tracked_content={"token": "t0"}, label="provider-secret")
    rel = source(remote_app={"secret-id": secret.id, "secret-granted": "true"})
    out = ctx().run(ctx().on.relation_changed(rel, remote_unit=0), testing.State(containers=[c], relations=[rel], secrets=[secret]))
    data = out.get_relation(5).local_unit_data
    assert data["secret-token"] == "t0"
    assert "secret-error" not in data
    assert out.unit_status == ops.ActiveStatus("providers: 1, token: t0")


def test_unreadable_secret_sets_an_error():
    c = web()
    rel = source(remote_app={"secret-id": "secret:cabbage1234567890abc", "secret-granted": "false"}, local={"secret-token": "t0"})
    out = ctx().run(ctx().on.relation_changed(rel, remote_unit=0), testing.State(containers=[c], relations=[rel]))
    data = out.get_relation(5).local_unit_data
    assert data["secret-error"]
    assert "secret-token" not in data
    assert data["seen-secret-granted"] == "false"


def test_secret_changed_refreshes_and_counts():
    c = web()
    secret = testing.Secret(tracked_content={"token": "t0"}, latest_content={"token": "t1"}, label="provider-secret")
    rel = source(remote_app={"secret-id": secret.id})
    out = ctx().run(ctx().on.secret_changed(secret), testing.State(containers=[c], relations=[rel], secrets=[secret]))
    data = out.get_relation(5).local_unit_data
    assert data["secret-token"] == "t1"
    assert data["secret-changes"] == "1"
    assert out.unit_status == ops.ActiveStatus("providers: 1, token: t1")


def test_upgrade_charm_is_handled():
    c = web()
    out = ctx().run(ctx().on.upgrade_charm(), testing.State(containers=[c]))
    assert out.unit_status.name == "active"
