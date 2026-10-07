"""Unit tests with ops.testing (Scenario); run by build.sh --test or `python -m pytest`."""

import pathlib
import sys

import ops
import pytest
import yaml
from ops import testing

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
from charm import JKTestCharm  # noqa: E402

META = pathlib.Path(__file__).parent.parent


def _load(name):
    return yaml.safe_load((META / name).read_text())


def action_params(name, **given):
    """The parameters juju hands the charm: the given ones over the defaults in actions.yaml."""
    out = {k: v["default"] for k, v in (_load("actions.yaml")[name].get("params") or {}).items() if "default" in v}
    out.update(given)
    return out


def ctx():
    return testing.Context(JKTestCharm, meta=_load("metadata.yaml"), config=_load("config.yaml"), actions=_load("actions.yaml"))


def web(connect=True):
    return testing.Container("web", can_connect=connect)


def test_pebble_ready_active():
    c = web()
    out = ctx().run(ctx().on.pebble_ready(c), testing.State(containers=[c], leader=True))
    assert out.unit_status == ops.ActiveStatus("greeting: hello, clients: 0, rev: 1")
    assert out.app_status == ops.ActiveStatus()
    assert out.get_container("web").service_statuses["web"] == ops.pebble.ServiceStatus.ACTIVE
    assert out.opened_ports == frozenset({testing.TCPPort(8080)})


def test_config_changed_greeting():
    c = web()
    out = ctx().run(ctx().on.config_changed(), testing.State(containers=[c], config={"greeting": "bonjour"}))
    assert out.unit_status == ops.ActiveStatus("greeting: bonjour, clients: 0, rev: 1")
    layer = out.get_container("web").layers["web"]
    assert "httpd" in layer.services["web"].command


def test_not_connected_waits():
    c = web(connect=False)
    out = ctx().run(ctx().on.config_changed(), testing.State(containers=[c]))
    assert out.unit_status == ops.WaitingStatus("waiting for pebble")


def test_update_status_and_stop():
    c = web()
    out = ctx().run(ctx().on.update_status(), testing.State(containers=[c]))
    assert out.unit_status.name == "active"
    ctx().run(ctx().on.stop(), testing.State(containers=[c]))


def peers(rid=7, local=None, peers_data=None, app=None):
    return testing.PeerRelation("cluster", id=rid, local_unit_data=local or {}, peers_data=peers_data or {}, local_app_data=app or {})


def test_peer_joined_counts_peers_and_writes_unit_data():
    c = web()
    rel = peers(peers_data={1: {}, 2: {}})
    out = ctx().run(ctx().on.relation_joined(rel, remote_unit=1), testing.State(containers=[c], relations=[rel]))
    assert out.unit_status == ops.ActiveStatus("greeting: hello, peers: 2, clients: 0, rev: 1")
    data = out.get_relation(7).local_unit_data
    assert data["counter"] == "1"
    assert data["address"]
    assert out.get_relation(7).local_app_data == {}  # not the leader


def test_peer_counter_increments_on_join_only():
    c = web()
    rel = peers(local={"counter": "4"}, peers_data={1: {}})
    out = ctx().run(ctx().on.relation_joined(rel, remote_unit=1), testing.State(containers=[c], relations=[rel]))
    assert out.get_relation(7).local_unit_data["counter"] == "5"


def test_peer_changed_does_not_write_counters():
    # relation-changed must not write: each write triggers relation-changed on the peers, so it would loop forever.
    c = web()
    rel = peers(local={"counter": "4"}, app={"generation": "2"}, peers_data={1: {}})
    out = ctx().run(ctx().on.relation_changed(rel, remote_unit=1), testing.State(containers=[c], relations=[rel], leader=True))
    r = out.get_relation(7)
    assert r.local_unit_data["counter"] == "4"
    assert r.local_app_data["generation"] == "2"


def test_peer_departed_reduces_peer_count():
    c = web()
    rel = peers(peers_data={1: {}})
    out = ctx().run(ctx().on.relation_departed(rel, remote_unit=2, departing_unit=2), testing.State(containers=[c], relations=[rel]))
    assert out.unit_status == ops.ActiveStatus("greeting: hello, peers: 1, clients: 0, rev: 1")


def test_leader_writes_app_data_and_creates_secret():
    c = web()
    rel = peers(peers_data={1: {}})
    out = ctx().run(ctx().on.relation_changed(rel, remote_unit=1), testing.State(containers=[c], relations=[rel], leader=True))
    app = out.get_relation(7).local_app_data
    assert app["leader"] == "jk-test/0"
    assert app["generation"] == "1"
    assert app["secret-id"].startswith("secret:")
    secret = out.get_secret(id=app["secret-id"])
    assert secret.owner == "app"
    assert secret.latest_content == {"bump": "0"}
    assert out.get_relation(7).local_unit_data["secret-bump"] == "0"


def test_leader_bump_writes_new_revision():
    c = web()
    secret = testing.Secret(tracked_content={"bump": "0"}, owner="app", label="cluster-secret")
    rel = peers(app={"secret-id": secret.id})
    out = ctx().run(ctx().on.config_changed(), testing.State(containers=[c], relations=[rel], secrets=[secret], leader=True, config={"bump-secret": 3}))
    assert out.get_secret(id=secret.id).latest_content == {"bump": "3"}


def test_non_leader_reads_shared_secret():
    c = web()
    secret = testing.Secret(tracked_content={"bump": "2"}, latest_content={"bump": "2"}, label="cluster-secret")
    rel = peers(peers_data={1: {}}, app={"secret-id": secret.id})
    out = ctx().run(ctx().on.relation_changed(rel, remote_unit=1), testing.State(containers=[c], relations=[rel], secrets=[secret]))
    assert out.get_relation(7).local_unit_data["secret-bump"] == "2"


def test_secret_changed_refreshes_and_counts():
    c = web()
    secret = testing.Secret(tracked_content={"bump": "0"}, latest_content={"bump": "1"}, label="cluster-secret")
    rel = peers(app={"secret-id": secret.id})
    out = ctx().run(ctx().on.secret_changed(secret), testing.State(containers=[c], relations=[rel], secrets=[secret]))
    data = out.get_relation(7).local_unit_data
    assert data["secret-changes"] == "1"
    assert data["secret-bump"] == "1"


def test_storage_attached_writes_marker_once():
    cx = ctx()
    c = web()
    st = testing.Storage("data")
    rel = peers()
    out = cx.run(cx.on.storage_attached(st), testing.State(containers=[c], storages=[st], relations=[rel]))
    marker = st.get_filesystem(cx) / "jk-marker"
    first = marker.read_text()
    assert first.startswith("storage-attached ")
    assert out.get_relation(7).local_unit_data["storage-first-event"] == "storage-attached"
    # a second event on the same volume keeps the marker (not fresh)
    cx.run(cx.on.install(), testing.State(containers=[c], storages=[st], relations=[rel]))
    assert marker.read_text() == first


# ---- data endpoint (provider) ----


def data_rel(rid=3, local=None, app=None, remote_app=None, remotes=None):
    return testing.Relation(
        "data",
        interface="jk_test_data",
        id=rid,
        remote_app_name="client",
        local_unit_data=local or {},
        local_app_data=app or {},
        remote_app_data=remote_app or {},
        remote_units_data=remotes if remotes is not None else {0: {}},
    )


def test_data_joined_publishes_unit_data_and_counts_clients():
    c = web()
    rel = data_rel(remotes={0: {}, 1: {}})
    out = ctx().run(ctx().on.relation_joined(rel, remote_unit=1), testing.State(containers=[c], relations=[rel]))
    assert out.unit_status == ops.ActiveStatus("greeting: hello, clients: 2, rev: 1")
    data = out.get_relation(3).local_unit_data
    assert data["provider-unit"] == "jk-test/0"
    assert data["clients"] == "client/0,client/1"
    assert data["joins"] == "1"
    assert data["address"]
    assert out.get_relation(3).local_app_data == {}  # not the leader


def test_data_changed_does_not_count_joins():
    c = web()
    rel = data_rel(local={"joins": "2"})
    out = ctx().run(ctx().on.relation_changed(rel, remote_unit=0), testing.State(containers=[c], relations=[rel]))
    assert out.get_relation(3).local_unit_data["joins"] == "2"


def test_data_leader_writes_app_data_and_reads_client_app_data():
    c = web()
    rel = data_rel(remote_app={"client-app": "client"})
    out = ctx().run(ctx().on.relation_changed(rel, remote_unit=0), testing.State(containers=[c], relations=[rel], leader=True))
    app = out.get_relation(3).local_app_data
    assert app["provider-app"] == "jk-test"
    assert app["generation"] == "1"
    assert app["client-app-seen"] == "client"


def test_data_generation_bumps_on_join_and_departure():
    c = web()
    rel = data_rel(app={"generation": "4"})
    st = testing.State(containers=[c], relations=[rel], leader=True)
    out = ctx().run(ctx().on.relation_joined(rel, remote_unit=0), st)
    assert out.get_relation(3).local_app_data["generation"] == "5"
    out = ctx().run(ctx().on.relation_departed(rel, remote_unit=0, departing_unit=0), st)
    assert out.get_relation(3).local_app_data["generation"] == "5"


def test_data_broken_status_ignores_the_relation():
    c = web()
    rel = data_rel(remotes={0: {}, 1: {}})
    out = ctx().run(ctx().on.relation_broken(rel), testing.State(containers=[c], relations=[rel]))
    assert out.unit_status == ops.ActiveStatus("greeting: hello, clients: 0, rev: 1")


def test_data_secret_created_and_granted_to_the_relation():
    c = web()
    rel = data_rel()
    peer = peers()
    out = ctx().run(ctx().on.relation_joined(rel, remote_unit=0), testing.State(containers=[c], relations=[rel, peer], leader=True))
    app = out.get_relation(3).local_app_data
    assert app["secret-id"].startswith("secret:")
    assert app["secret-granted"] == "true"
    secret = out.get_secret(id=app["secret-id"])
    assert secret.owner == "app"
    assert secret.latest_content == {"token": "t0"}
    assert out.get_relation(7).local_app_data["data-secret-id"] == app["secret-id"]
    assert secret.remote_grants == {3: {"client"}}


def test_data_secret_bump_writes_new_revision():
    c = web()
    secret = testing.Secret(tracked_content={"token": "t0"}, owner="app", label="data-secret")
    rel = data_rel(app={"secret-id": secret.id, "secret-granted": "true"})
    peer = peers(app={"data-secret-id": secret.id})
    out = ctx().run(ctx().on.config_changed(), testing.State(containers=[c], relations=[rel, peer], secrets=[secret], leader=True, config={"bump-data-secret": 2}))
    assert out.get_secret(id=secret.id).latest_content == {"token": "t2"}


def test_data_secret_revoked_when_grant_turned_off():
    c = web()
    secret = testing.Secret(tracked_content={"token": "t0"}, owner="app", label="data-secret", remote_grants={3: {"client"}})
    rel = data_rel(app={"secret-id": secret.id, "secret-granted": "true"})
    peer = peers(app={"data-secret-id": secret.id})
    out = ctx().run(ctx().on.config_changed(), testing.State(containers=[c], relations=[rel, peer], secrets=[secret], leader=True, config={"grant-data-secret": False}))
    assert out.get_relation(3).local_app_data["secret-granted"] == "false"
    assert not out.get_secret(id=secret.id).remote_grants.get(3)


# ---- upgrade ----


def test_upgrade_charm_records_old_and_new_revision():
    c = web()
    rel = peers(local={"revision": "0"})
    out = ctx().run(ctx().on.upgrade_charm(), testing.State(containers=[c], relations=[rel]))
    data = out.get_relation(7).local_unit_data
    assert data["upgraded-from"] == "0"
    assert data["upgraded-to"] == "1"
    assert data["upgrades"] == "1"
    assert data["revision"] == "1"
    assert out.unit_status == ops.ActiveStatus("greeting: hello, peers: 0, clients: 0, rev: 1")


def test_revision_is_the_file_content():
    import charm

    assert charm.REVISION == (META / "revision").read_text().strip()


# ---- actions ----


def test_echo_defaults_nested_results_and_logs():
    cx = ctx()
    cx.run(cx.on.action("echo", params=action_params("echo", text="hi")), testing.State())
    assert cx.action_results == {"echo": {"text": "hi", "times": 2, "repeated": "hi hi"}, "upper": False, "ratio": 0.5, "tags": ""}
    assert cx.action_logs == ["echo: hi", "times: 2"]


def test_echo_parameters():
    cx = ctx()
    cx.run(cx.on.action("echo", params=action_params("echo", text="hi", times=3, upper=True, ratio=1.5, tags=["a", "b"])), testing.State())
    assert cx.action_results["echo"] == {"text": "HI", "times": 3, "repeated": "HI HI HI"}
    assert cx.action_results["tags"] == "a,b"


def test_fail_sets_results_and_message():
    cx = ctx()
    with pytest.raises(testing.ActionFailed) as e:
        cx.run(cx.on.action("fail", params=action_params("fail", message="boom")), testing.State())
    assert e.value.message == "boom"
    assert cx.action_results == {"partial": "yes"}
    assert cx.action_logs == ["about to fail"]
    with pytest.raises(testing.ActionFailed) as e:
        cx.run(cx.on.action("fail", params=action_params("fail")), testing.State())
    assert e.value.message == "failed on purpose"


def test_sleep_returns_after_the_given_seconds(monkeypatch):
    slept = []
    monkeypatch.setattr("charm.time.sleep", slept.append)
    cx = ctx()
    cx.run(cx.on.action("sleep", params=action_params("sleep", seconds=7)), testing.State())
    assert slept == [7]
    assert cx.action_results == {"slept": 7}
    cx.run(cx.on.action("sleep", params=action_params("sleep")), testing.State())
    assert slept == [7, 30]


def test_whoami_reports_unit_and_leadership():
    cx = ctx()
    cx.run(cx.on.action("whoami"), testing.State(leader=True))
    res = cx.action_results
    assert (res["unit"], res["leader"], res["app"], res["action-name"]) == ("jk-test/0", "true", "jk-test", "whoami")
    assert {"action-id", "action-tag"} <= set(res)  # values come from the hook environment, set by juju
    cx.run(cx.on.action("whoami"), testing.State(leader=False))
    assert cx.action_results["leader"] == "false"


def test_upgrade_after_a_resumed_hook_still_reports_the_previous_revision():
    # The new pod resumed an interrupted hook before upgrade-charm, so `revision` already holds the new value.
    c = web()
    rel = peers(local={"revision": "1", "revisions-seen": "0,1"})
    out = ctx().run(ctx().on.upgrade_charm(), testing.State(containers=[c], relations=[rel]))
    assert out.get_relation(7).local_unit_data["upgraded-from"] == "0"
