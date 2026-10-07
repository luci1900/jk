## Charm inventory

Charmhub lists 308 charms. The 90 current sidecar k8s charms were downloaded and scanned, including their bundled libraries, so the counts overstate real use. The scan decides what is built and what is not supported.

- 90 are sidecar charms, 17 are podspec and 201 are machine or subordinate charms. All 90 sidecar charms use ops.
- Usage: leadership 94%, Pebble layers 93%, `JujuVersion` 78%, `resource-get` 76%, k8s API 72%, `network-get` 71%, app relation data 70%, secrets 52%, actions 50%, `<app>-endpoints` DNS 44%, patching their own StatefulSet 36% and Service 33%, ports 35%, storage 26% (filesystem only) and cluster-scoped objects 18%.
- Unused: `storage-add`, `credential-get`, `juju-reboot`, `secret-revoke`, secret rotation, Pebble notices and block, `multiple` and `shared` storage. Rare: `goal-state` (1 charm), Pebble check events (4) and `storage-detaching` (4).
- The top interfaces are `loki_push_api`, `prometheus_scrape`, `grafana_dashboard`, `tracing`, `ingress` and `tls-certificates`. Most of them come from COS in another model, which is why jk has offers.
