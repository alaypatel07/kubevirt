# IMEX Network Demo — Pre-configured Domain with Scale Up/Down

Demonstrates NVIDIA IMEX daemon running across KubeVirt VMIs with DNS-based peer discovery via a Kubernetes headless service. The `nodes_config.cfg` is pre-populated with the full domain size (5 nodes) and mounted via virtiofs from a ConfigMap. IMEX tolerates missing peers, so VMs can be added or removed without changing the config or restarting daemons on existing VMs.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Kubernetes Cluster                                                 │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  Headless Service: imex-service  (clusterIP: None)            │  │
│  │                                                               │  │
│  │  CoreDNS creates per-pod DNS records:                         │  │
│  │    imex-vm-01.imex-service.default.svc.cluster.local → Pod IP │  │
│  │    imex-vm-02.imex-service.default.svc.cluster.local → Pod IP │  │
│  │    imex-vm-03.imex-service.default.svc.cluster.local → Pod IP │  │
│  │    imex-vm-04 ... (no endpoint yet, DNS fails, IMEX retries)  │  │
│  │    imex-vm-05 ... (no endpoint yet, DNS fails, IMEX retries)  │  │
│  └───────────────────────────────────────────────────────────────┘  │
│         ▲              ▲              ▲                              │
│         │ hostname/    │ hostname/    │ hostname/                    │
│         │ subdomain    │ subdomain    │ subdomain                    │
│  ┌──────┴──────┐┌──────┴──────┐┌──────┴──────┐                     │
│  │  VMI:       ││  VMI:       ││  VMI:       │                     │
│  │  imex-vm-01 ││  imex-vm-02 ││  imex-vm-03 │  (vm-04,05 not     │
│  │             ││             ││             │   created yet)       │
│  │ ┌─────────┐ ││ ┌─────────┐ ││ ┌─────────┐ │                     │
│  │ │ IMEX    │◄┼┼─┤ IMEX    │◄┼┼─┤ IMEX    │ │  gRPC mesh on      │
│  │ │ daemon  ├─┼┼►│ daemon  ├─┼┼►│ daemon  │ │  port 50000        │
│  │ └────┬────┘ ││ └────┬────┘ ││ └────┬────┘ │                     │
│  │      │      ││      │      ││      │      │                     │
│  │      ▼      ││      ▼      ││      ▼      │                     │
│  │ /etc/nvidia-││ /etc/nvidia-││ /etc/nvidia-│                     │
│  │ imex/nodes_ ││ imex/nodes_ ││ imex/nodes_ │                     │
│  │ config.cfg  ││ config.cfg  ││ config.cfg  │                     │
│  │   (symlink) ││   (symlink) ││   (symlink) │                     │
│  │      │      ││      │      ││      │      │                     │
│  │      ▼      ││      ▼      ││      ▼      │                     │
│  │  virtiofs   ││  virtiofs   ││  virtiofs   │                     │
│  │  mount      ││  mount      ││  mount      │                     │
│  └──────┬──────┘└──────┬──────┘└──────┬──────┘                     │
│         │              │              │                              │
│         └──────────────┼──────────────┘                              │
│                        ▼                                             │
│  ┌─────────────────────────────────────┐                            │
│  │  ConfigMap: imex-nodes-config       │                            │
│  │                                     │                            │
│  │  nodes_config.cfg:                  │                            │
│  │    imex-vm-01.imex-service...       │                            │
│  │    imex-vm-02.imex-service...       │                            │
│  │    imex-vm-03.imex-service...       │                            │
│  │    imex-vm-04.imex-service...       │                            │
│  │    imex-vm-05.imex-service...       │                            │
│  └─────────────────────────────────────┘                            │
└─────────────────────────────────────────────────────────────────────┘
```

**How the headless service works**: A normal Kubernetes Service gets a single ClusterIP that load-balances across pods. A headless service (`clusterIP: None`) skips the load balancer — instead, CoreDNS creates individual A records for each pod that sets `hostname` and `subdomain` matching the service name. This gives each VMI a stable, resolvable DNS name that maps directly to its pod IP. When a VMI is deleted, its DNS record disappears; when recreated, it gets a new IP and a new DNS record under the same hostname.

Since the config lists all possible nodes upfront, IMEX on each VM continuously retries connections to peers that aren't running yet. When a new VM boots, existing daemons connect to it automatically on the next retry cycle — no config changes or daemon restarts needed.

### Key IMEX behaviors

- The daemon only reads `nodes_config.cfg` at startup. `SIGUSR1` triggers DNS re-resolution for the existing node list but does **not** re-read the config file.
- `IMEX_NODE_DISCONNECTED_GRACE_TIME=-1` (default): waits indefinitely for disconnected peers, so removing a VM doesn't trigger cleanup on the remaining nodes.
- `IMEX_WAIT_FOR_QUORUM=RECOVERY` (default): on first boot, starts immediately without waiting for all peers.

## Prerequisites

- KubeVirt cluster with `make cluster-sync` completed
- `virtctl` available via `./hack/virtctl.sh`

## Deploy (2 VMs)

```bash
kubectl create -f demo-multi-node-nvl/vmi.yaml
```

This creates:
- ConfigMap `imex-nodes-config` (pre-populated with 5-node domain)
- Secret `imex-cloudinit` (shared cloud-init that installs IMEX and mounts the ConfigMap)
- VMIs `imex-vm-01` and `imex-vm-02`
- Headless Service `imex-service`

Wait a few minutes for the VMs to boot, install `nvidia-imex` via `dnf`, and start the daemon. IMEX will connect to the 2 running peers and keep retrying the 3 absent ones in the background.

## Verify

Console into a VM:

```bash
./hack/virtctl.sh console imex-vm-01
```

Login: `fedora` / `fedora`, then `sudo -i` and check:

```bash
systemctl status nvidia-imex
tail -f /var/log/nvidia-imex.log
```

You should see connections established to the running peer and retry attempts for the absent nodes.

## Scale Up

Just create a new VMI — no config changes needed:

```bash
kubectl create -f demo-multi-node-nvl/scaleup-vmi.yaml
```

Once `imex-vm-03` boots and its IMEX daemon starts, the existing daemons on `imex-vm-01` and `imex-vm-02` will connect to it on their next retry cycle (within seconds).

## Scale Down

Just delete the VMI:

```bash
kubectl delete -f demo-multi-node-nvl/scaleup-vmi.yaml
```

IMEX on the remaining VMs will detect the disconnect and wait indefinitely (`IMEX_NODE_DISCONNECTED_GRACE_TIME=-1`), retrying until the peer comes back.

## Teardown

```bash
kubectl delete -f demo-multi-node-nvl/vmi.yaml
```

## IMEX Configuration

No custom `config.cfg` is needed — all IMEX defaults are used. The systemd service uses `Type=forking` to match the daemon's default `DAEMONIZE=1` behavior. Key defaults:

| Setting | Default | Effect |
|---------|---------|--------|
| `IMEX_WAIT_FOR_QUORUM` | `RECOVERY` | First boot starts immediately; after crash, waits for previously-connected peers |
| `IMEX_NODE_DISCONNECTED_GRACE_TIME` | `-1` | Waits indefinitely for disconnected peers to reconnect |

## Files

| File | Description |
|------|-------------|
| `vmi.yaml` | ConfigMap (5-node domain) + Secret + 2 VMIs + headless service |
| `scaleup-vmi.yaml` | Third VMI for scale-up testing |
