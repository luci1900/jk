#!/usr/bin/env python3
"""Tiny Pebble charm for jk's end-to-end tests: busybox httpd serving the `greeting` option.

Also exercises peers, storage and secrets so integration can be tested without postgres:
- peer endpoint `cluster`: every unit publishes `address`, `counter` (peer joins and departures seen), `storage-*` and `secret-*` keys in its unit data;
  the leader publishes `leader`, `generation` (peer events seen by the leader) and `secret-id` in the app data;
- storage `data`: a marker file `<location>/jk-marker` is written on storage-attached or install, whichever comes first, and logged as fresh or pre-existing;
- an app-owned secret created by the leader (content `{"bump": <bump-secret>}`), shared by id through the peer app data; the leader writes a new revision when `bump-secret` changes;
- provider endpoint `data` (interface jk_test_data, required by jk-test-client): every unit publishes `address`, `provider-unit`, `joins` (clients that joined) and `clients` (the related units it sees) in its unit data;
  the leader publishes `provider-app`, `generation`, `client-app-seen`, `secret-id` and `secret-granted` in the app data, with a second app-owned secret (`{"token": "t<bump-data-secret>"}`) granted to the related application;
- actions `echo`, `fail`, `sleep` and `whoami`;
- `revision` file in the charm directory (stamped by build.sh): shown in the status message and recorded by the upgrade-charm handler as `upgraded-from`/`upgraded-to` in the peer unit data.
"""

import logging
import os
import pathlib
import socket
import time
import uuid

import ops

logger = logging.getLogger(__name__)

CONTAINER = "web"
SERVICE = "web"
WWW = "/www"
PORT = 8080
PEER = "cluster"
STORAGE = "data"
MARKER = "jk-marker"
SECRET_LABEL = "cluster-secret"
DATA = "data"
DATA_SECRET_LABEL = "data-secret"
REVISION = (pathlib.Path(__file__).resolve().parent.parent / "revision").read_text().strip()


def _set(data, key: str, value: str):
    """Writes only a changed value: a write of an equal value would still wake the other side's relation-changed."""
    if data.get(key) != value:
        data[key] = value


def _previous_revision(unit_data, current: str) -> str:
    """The latest revision this unit ran before `current`, from the set it recorded (or the legacy `revision` key)."""
    seen = {r for r in unit_data.get("revisions-seen", "").split(",") if r}
    if unit_data.get("revision"):
        seen.add(unit_data["revision"])
    seen.discard(current)
    return max(seen, key=lambda r: (len(r), r)) if seen else "none"


class JKTestCharm(ops.CharmBase):
    def __init__(self, framework: ops.Framework):
        super().__init__(framework)
        self.container = self.unit.get_container(CONTAINER)
        for event in (
            self.on.install,
            self.on.start,
            self.on.stop,
            self.on.remove,
            self.on.leader_elected,
            self.on.update_status,
            self.on.config_changed,
            self.on.web_pebble_ready,
            self.on[PEER].relation_created,
            self.on[PEER].relation_joined,
            self.on[PEER].relation_changed,
            self.on[PEER].relation_departed,
            self.on[PEER].relation_broken,
            self.on[STORAGE].storage_attached,
            self.on[STORAGE].storage_detaching,
            self.on.secret_changed,
            self.on.secret_remove,
            self.on.upgrade_charm,
            self.on[DATA].relation_created,
            self.on[DATA].relation_joined,
            self.on[DATA].relation_changed,
            self.on[DATA].relation_departed,
            self.on[DATA].relation_broken,
        ):
            framework.observe(event, self._log_event)
        framework.observe(self.on.upgrade_charm, self._on_upgrade)  # before _update, which records the new revision
        for event in (
            self.on[DATA].relation_created,
            self.on[DATA].relation_joined,
            self.on[DATA].relation_changed,
            self.on[DATA].relation_departed,
            self.on[DATA].relation_broken,
            self.on.upgrade_charm,
            self.on.config_changed,
            self.on.leader_elected,
        ):
            framework.observe(event, self._on_data_event)
        framework.observe(self.on.echo_action, self._on_echo)
        framework.observe(self.on.fail_action, self._on_fail)
        framework.observe(self.on.sleep_action, self._on_sleep)
        framework.observe(self.on.whoami_action, self._on_whoami)
        for event in (
            self.on[PEER].relation_created,
            self.on[PEER].relation_joined,
            self.on[PEER].relation_changed,
            self.on[PEER].relation_departed,
        ):
            framework.observe(event, self._on_peer_event)
        framework.observe(self.on.install, self._on_storage_event)
        framework.observe(self.on[STORAGE].storage_attached, self._on_storage_event)
        framework.observe(self.on.leader_elected, self._update)
        framework.observe(self.on.secret_changed, self._on_secret_changed)
        framework.observe(self.on.web_pebble_ready, self._update)
        framework.observe(self.on.config_changed, self._update)
        framework.observe(self.on.upgrade_charm, self._update)
        framework.observe(self.on.update_status, self._update)
        framework.observe(self.on.stop, self._on_stop)

    @property
    def greeting(self) -> str:
        return str(self.config["greeting"])

    def _log_event(self, event: ops.EventBase):
        extra = ""
        rel = getattr(event, "relation", None)
        if rel is not None:
            extra += f" relation={rel.name}:{rel.id}"
        unit = getattr(event, "unit", None)
        if unit is not None:
            extra += f" remote={unit.name}"
        if getattr(event, "departing_unit", None) is not None:
            extra += f" departing={event.departing_unit.name}"
        env = " ".join(f"{k}={os.environ.get(k, '')}" for k in ("JUJU_REMOTE_UNIT", "JUJU_REMOTE_APP", "JUJU_DEPARTING_UNIT", "JUJU_RELATION", "JUJU_RELATION_ID"))
        logger.info("event %s (leader=%s greeting=%s rev=%s)%s env %s", type(event).__name__, self.unit.is_leader(), self.greeting, REVISION, extra, env)

    def _layer(self) -> ops.pebble.LayerDict:
        return {
            "summary": "jk-test web",
            "services": {
                SERVICE: {
                    "override": "replace",
                    "summary": "busybox httpd",
                    "command": f"httpd -f -p {PORT} -h {WWW}",
                    "startup": "enabled",
                }
            },
        }

    # ---- peers ----

    @property
    def peers(self) -> "ops.Relation | None":
        return self.model.get_relation(PEER)

    def _address(self) -> str:
        try:
            binding = self.model.get_binding(PEER)
            addr = binding.network.bind_address if binding else None
            if addr:
                return str(addr)
        except (ops.ModelError, ops.RelationNotFoundError) as e:
            logger.warning("network-get failed: %s", e)
        return socket.gethostname()

    def _on_peer_event(self, event: ops.RelationEvent):
        rel = event.relation
        if isinstance(event, ops.RelationDepartedEvent) and event.departing_unit == self.unit:
            return  # this unit is leaving; leave the databag alone
        # Only membership changes bump the counters. Bumping on relation-changed would feed back:
        # every write triggers relation-changed on the peers, which write again, forever.
        if isinstance(event, (ops.RelationJoinedEvent, ops.RelationDepartedEvent)):
            data = rel.data[self.unit]
            data["counter"] = str(int(data.get("counter", "0")) + 1)
            if self.unit.is_leader():
                app = rel.data[self.app]
                app["generation"] = str(int(app.get("generation", "0")) + 1)
        elif self.unit.is_leader() and "generation" not in rel.data[self.app]:
            rel.data[self.app]["generation"] = "1"  # written once, so peers always see a generation
        self._update(event)

    def _peer_count(self) -> "int | None":
        rel = self.peers
        return None if rel is None else len(rel.units)

    # ---- storage ----

    def _marker_path(self) -> "pathlib.Path | None":
        try:
            stores = self.model.storages[STORAGE]
        except (ops.ModelError, KeyError):
            return None
        return stores[0].location / MARKER if stores else None

    def _on_storage_event(self, event: ops.EventBase):
        if isinstance(event, ops.StorageAttachedEvent):
            path = event.storage.location / MARKER
        else:
            path = self._marker_path()
        if path is None:
            logger.info("storage %s not attached at %s", STORAGE, type(event).__name__)
            return
        if path.exists():
            logger.info("storage %s fresh=false marker pre-existed: %s", STORAGE, path.read_text().strip())
        else:
            first = "storage-attached" if isinstance(event, ops.StorageAttachedEvent) else "install"
            path.write_text(f"{first} {uuid.uuid4().hex}\n")
            logger.info("storage %s fresh=true marker written at %s", STORAGE, path)
        self._update(event)

    def _storage_info(self) -> "dict[str, str]":
        path = self._marker_path()
        if path is None or not path.exists():
            return {}
        first, _, nonce = path.read_text().strip().partition(" ")
        return {"storage-first-event": first, "storage-marker": nonce}

    # ---- secret ----

    def _secret_content(self) -> "dict[str, str]":
        return {"bump": str(self.config["bump-secret"])}

    def _shared_secret(self) -> "ops.Secret | None":
        rel = self.peers
        if rel is None:
            return None
        sid = rel.data[self.app].get("secret-id")
        if not sid:
            return None
        try:
            return self.model.get_secret(id=sid, label=SECRET_LABEL)
        except (ops.SecretNotFoundError, ops.ModelError) as e:
            logger.warning("cannot get secret %s: %s", sid, e)
            return None

    def _sync_secret(self) -> "dict[str, str]":
        """Leader: create the secret or write a new revision; everyone: read it. Returns the unit data to publish."""
        rel = self.peers
        if rel is None:
            return {}
        if self.unit.is_leader():
            secret = self._shared_secret()
            if secret is None:
                if rel.data[self.app].get("secret-id"):
                    return {}
                secret = self.app.add_secret(self._secret_content(), label=SECRET_LABEL, description="jk-test shared secret")
                rel.data[self.app]["secret-id"] = secret.id
                logger.info("created secret %s", secret.id)
            elif secret.peek_content() != self._secret_content():
                secret.set_content(self._secret_content())
                logger.info("wrote a new revision of %s", secret.id)
            content = secret.peek_content()
            info = secret.get_info()
            return {"secret-bump": content["bump"], "secret-revision": str(info.revision)}
        secret = self._shared_secret()
        if secret is None:
            return {}
        content = secret.get_content()
        return {"secret-bump": content["bump"]}

    def _on_secret_changed(self, event: ops.SecretChangedEvent):
        content = event.secret.get_content(refresh=True)
        logger.info("secret %s changed, bump=%s", event.secret.id, content["bump"])
        rel = self.peers
        if rel is not None:
            data = rel.data[self.unit]
            data["secret-changes"] = str(int(data.get("secret-changes", "0")) + 1)
        self._update(event)

    # ---- upgrade ----

    def _on_upgrade(self, event: ops.UpgradeCharmEvent):
        rel = self.peers
        # A hook interrupted by the restart is resumed before upgrade-charm and runs the new code, which overwrites
        # `revision`; the set of revisions seen survives, so the previous one is the highest seen that is not this one.
        old = "none" if rel is None else _previous_revision(rel.data[self.unit], REVISION)
        logger.info("upgrade-charm from=%s to=%s", old, REVISION)
        if rel is not None:
            data = rel.data[self.unit]
            data["upgraded-from"] = old
            data["upgraded-to"] = REVISION
            data["upgrades"] = str(int(data.get("upgrades", "0")) + 1)

    # ---- provider ----

    def _data_relations(self, event: "ops.EventBase | None" = None) -> "list[ops.Relation]":
        """The data relations that are alive (not the one being broken)."""
        broken = event.relation.id if isinstance(event, ops.RelationBrokenEvent) else None
        return [r for r in self.model.relations[DATA] if r.id != broken]

    def _client_count(self, event: "ops.EventBase | None" = None) -> int:
        return sum(len(r.units) for r in self._data_relations(event))

    def _data_secret(self) -> "ops.Secret | None":
        """Leader: the app-owned secret granted to clients, created on first use; its id is kept in the peer app data."""
        peers = self.peers
        if peers is None:
            return None
        content = {"token": f"t{self.config['bump-data-secret']}"}
        sid = peers.data[self.app].get("data-secret-id")
        if sid:
            secret = self.model.get_secret(id=sid, label=DATA_SECRET_LABEL)
            if secret.peek_content() != content:
                secret.set_content(content)
                logger.info("wrote a new revision of the data secret %s", sid)
            return secret
        secret = self.app.add_secret(content, label=DATA_SECRET_LABEL, description="jk-test data secret")
        peers.data[self.app]["data-secret-id"] = secret.id
        logger.info("created the data secret %s", secret.id)
        return secret

    def _on_data_event(self, event: ops.EventBase):
        # Nothing is published into a relation that is gone, or by a unit that is leaving it.
        leaving = isinstance(event, ops.RelationBrokenEvent) or (isinstance(event, ops.RelationDepartedEvent) and event.departing_unit == self.unit)
        if not leaving:
            self._publish_data(event)
        if not isinstance(event, (ops.UpgradeCharmEvent, ops.ConfigChangedEvent, ops.LeaderElectedEvent)):
            self._update(event)  # these three are updated by their own observers

    def _publish_data(self, event: ops.EventBase):
        rels = self._data_relations(event)
        leader = self.unit.is_leader()
        secret = self._data_secret() if leader and rels else None
        grant = bool(self.config["grant-data-secret"])
        for rel in rels:
            udata = rel.data[self.unit]
            _set(udata, "address", self._address())
            _set(udata, "provider-unit", self.unit.name)
            _set(udata, "clients", ",".join(sorted(u.name for u in rel.units)))
            is_this = isinstance(event, ops.RelationEvent) and event.relation.id == rel.id
            if isinstance(event, ops.RelationJoinedEvent) and is_this:
                udata["joins"] = str(int(udata.get("joins", "0")) + 1)
            if not leader:
                continue
            adata = rel.data[self.app]
            _set(adata, "provider-app", self.app.name)
            if rel.app is not None:
                _set(adata, "client-app-seen", rel.data[rel.app].get("client-app", ""))
            if isinstance(event, (ops.RelationJoinedEvent, ops.RelationDepartedEvent)) and is_this:
                adata["generation"] = str(int(adata.get("generation", "0")) + 1)
            elif "generation" not in adata:
                adata["generation"] = "1"
            if secret is not None:
                was = adata.get("secret-granted") == "true"
                if grant and not was:
                    secret.grant(rel)
                elif was and not grant:
                    secret.revoke(rel)
                _set(adata, "secret-id", secret.id)
                _set(adata, "secret-granted", "true" if grant else "false")

    # ---- actions ----

    def _on_echo(self, event: ops.ActionEvent):
        params = event.params
        event.log(f"echo: {params['text']}")
        event.log(f"times: {params['times']}")
        text = params["text"].upper() if params["upper"] else params["text"]
        event.set_results(
            {
                "echo": {"text": text, "times": params["times"], "repeated": " ".join([text] * params["times"])},
                "upper": params["upper"],
                "ratio": params["ratio"],
                "tags": ",".join(params["tags"]),
            }
        )

    def _on_fail(self, event: ops.ActionEvent):
        event.log("about to fail")
        event.set_results({"partial": "yes"})
        event.fail(event.params["message"])

    def _on_sleep(self, event: ops.ActionEvent):
        seconds = event.params["seconds"]
        event.log(f"sleeping {seconds}s")
        time.sleep(seconds)
        event.set_results({"slept": seconds})

    def _on_whoami(self, event: ops.ActionEvent):
        event.set_results(
            {
                "unit": self.unit.name,
                "leader": str(self.unit.is_leader()).lower(),
                "app": self.app.name,
                "action-name": os.environ.get("JUJU_ACTION_NAME", ""),
                "action-id": os.environ.get("JUJU_ACTION_UUID", ""),
                "action-tag": os.environ.get("JUJU_ACTION_TAG", ""),
            }
        )

    # ---- status ----

    def _publish(self):
        rel = self.peers
        if rel is None:
            return
        data = rel.data[self.unit]
        seen = sorted(set(filter(None, data.get("revisions-seen", "").split(","))) | {REVISION})
        want = {"address": self._address(), "revision": REVISION, "revisions-seen": ",".join(seen),
                **self._storage_info(), **self._sync_secret()}
        for k, v in want.items():
            if data.get(k) != v:
                data[k] = v
        if self.unit.is_leader():
            app = rel.data[self.app]
            if app.get("leader") != self.unit.name:
                app["leader"] = self.unit.name

    def _update(self, event: ops.EventBase):
        self._publish()
        if not self.container.can_connect():
            self.unit.status = ops.WaitingStatus("waiting for pebble")
            return
        self.container.make_dir(WWW, make_parents=True)
        self.container.push(f"{WWW}/index.html", self.greeting + "\n")
        self.container.add_layer("web", self._layer(), combine=True)
        self.container.replan()
        self.unit.set_ports(PORT)
        msg = f"greeting: {self.greeting}"
        peers = self._peer_count()
        if peers is not None:
            msg += f", peers: {peers}"
        msg += f", clients: {self._client_count(event)}, rev: {REVISION}"
        self.unit.status = ops.ActiveStatus(msg)
        if self.unit.is_leader():
            self.app.status = ops.ActiveStatus()

    def _on_stop(self, event: ops.StopEvent):
        logger.info("stopping, greeting was %s", self.greeting)


if __name__ == "__main__":
    ops.main(JKTestCharm)
