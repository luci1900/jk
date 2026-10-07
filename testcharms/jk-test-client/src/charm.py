#!/usr/bin/env python3
"""Requirer side of jk-test's `data` endpoint, for jk's end-to-end tests.

Endpoint `source` (interface jk_test_data, limit 1):
- every unit publishes `address`, `client-unit`, `hello` and what it sees of the provider (`seen-provider-app`, `seen-units`, `seen-generation`) in its unit data;
  the leader publishes `client-app` in the app data;
- the secret the provider granted (id in the provider's app data) is read with a label; `secret-token` holds the content and `secret-changes` counts secret-changed events;
  `secret-error` holds the error type when it cannot be read (not granted or revoked).
The status message is `providers: <n>, token: <token>`.
"""

import logging
import os
import pathlib
import socket

import ops

logger = logging.getLogger(__name__)

CONTAINER = "web"
PORT = 8080
SOURCE = "source"
SECRET_LABEL = "provider-secret"
REVISION = (pathlib.Path(__file__).resolve().parent.parent / "revision").read_text().strip()


def _set(data, key: str, value: str):
    """Writes only a changed value: a write of an equal value would still wake the other side's relation-changed."""
    if data.get(key) != value:
        data[key] = value


def _drop(data, key: str):
    if key in data:
        del data[key]


class JKTestClientCharm(ops.CharmBase):
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
            self.on.upgrade_charm,
            self.on.web_pebble_ready,
            self.on[SOURCE].relation_created,
            self.on[SOURCE].relation_joined,
            self.on[SOURCE].relation_changed,
            self.on[SOURCE].relation_departed,
            self.on[SOURCE].relation_broken,
            self.on.secret_changed,
        ):
            framework.observe(event, self._log_event)
            framework.observe(event, self._update)

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
        logger.info("event %s (leader=%s rev=%s)%s env %s", type(event).__name__, self.unit.is_leader(), REVISION, extra, env)

    def _address(self) -> str:
        try:
            binding = self.model.get_binding(SOURCE)
            addr = binding.network.bind_address if binding else None
            if addr:
                return str(addr)
        except (ops.ModelError, ops.RelationNotFoundError) as e:
            logger.warning("network-get failed: %s", e)
        return socket.gethostname()

    def _relations(self, event: ops.EventBase) -> "list[ops.Relation]":
        broken = event.relation.id if isinstance(event, ops.RelationBrokenEvent) else None
        return [r for r in self.model.relations[SOURCE] if r.id != broken]

    def _read_secret(self, rel: ops.Relation, refresh: bool = False) -> "tuple[str | None, str | None]":
        """Returns (token, error) from the secret the provider granted."""
        sid = rel.data[rel.app].get("secret-id") if rel.app is not None else None
        if not sid:
            return None, None
        try:
            secret = self.model.get_secret(id=sid, label=SECRET_LABEL)
            content = secret.get_content(refresh=refresh)
            return content["token"], None
        except (ops.SecretNotFoundError, ops.ModelError) as e:
            logger.warning("cannot read secret %s: %s", sid, e)
            return None, type(e).__name__

    def _publish(self, event: ops.EventBase) -> "tuple[int, str]":
        """Writes the unit and app data; returns the number of provider units seen and the secret token."""
        if isinstance(event, ops.RelationBrokenEvent) or (isinstance(event, ops.RelationDepartedEvent) and event.departing_unit == self.unit):
            return 0, ""
        rels = self._relations(event)
        providers = 0
        token = ""
        for rel in rels:
            udata = rel.data[self.unit]
            _set(udata, "address", self._address())
            _set(udata, "client-unit", self.unit.name)
            _set(udata, "hello", f"hello from {self.unit.name}")
            names = sorted(u.name for u in rel.units)
            providers += len(names)
            _set(udata, "seen-units", ",".join(names))
            if rel.app is not None:
                padata = rel.data[rel.app]
                _set(udata, "seen-provider-app", padata.get("provider-app", ""))
                _set(udata, "seen-generation", padata.get("generation", ""))
                _set(udata, "seen-secret-granted", padata.get("secret-granted", ""))
            refresh = isinstance(event, ops.SecretChangedEvent)
            tok, err = self._read_secret(rel, refresh=refresh)
            if tok is not None:
                token = tok
                if udata.get("secret-token") != tok:
                    udata["secret-token"] = tok
                _drop(udata, "secret-error")
            elif err is not None:
                _drop(udata, "secret-token")
                _set(udata, "secret-error", err)
            if self.unit.is_leader():
                _set(rel.data[self.app], "client-app", self.app.name)
        return providers, token

    def _update(self, event: ops.EventBase):
        providers, token = self._publish(event)
        if isinstance(event, ops.SecretChangedEvent):
            for rel in self._relations(event):
                data = rel.data[self.unit]
                data["secret-changes"] = str(int(data.get("secret-changes", "0")) + 1)
        if not self.container.can_connect():
            self.unit.status = ops.WaitingStatus("waiting for pebble")
            return
        self.container.add_layer(
            "web",
            {
                "summary": "jk-test-client web",
                "services": {"web": {"override": "replace", "summary": "busybox httpd", "command": f"httpd -f -p {PORT} -h /tmp", "startup": "enabled"}},
            },
            combine=True,
        )
        self.container.replan()
        self.unit.set_ports(PORT)
        msg = f"providers: {providers}"
        if token:
            msg += f", token: {token}"
        self.unit.status = ops.ActiveStatus(msg)
        if self.unit.is_leader():
            self.app.status = ops.ActiveStatus()


if __name__ == "__main__":
    ops.main(JKTestClientCharm)
