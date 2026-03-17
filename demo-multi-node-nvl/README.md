# IMEX Network Demo — Dynamic Scale Up/Down

Demonstrates NVIDIA IMEX daemon running across KubeVirt VMIs with DNS-based peer discovery via a Kubernetes headless service. The IMEX domain is pre-configured with 5 slots, but only a subset of VMs need to be running at any time.

## Architecture

- **Headless Service** (`imex-service`) provides stable DNS names for each VMI: `imex-vm-XX.imex-service.default.svc.cluster.local`
- **Shared Secret** (`imex-cloudinit`) contains cloud-init that installs and configures IMEX on boot
- **nodes_config.cfg** is pre-populated with all 5 DNS hostnames — IMEX retries unreachable peers indefinitely (`IMEX_NODE_DISCONNECTED_GRACE_TIME=-1`)
- When a VMI is created, CoreDNS resolves its hostname to the new pod IP and IMEX auto-connects
- When a VMI is deleted, IMEX detects the disconnect and waits for it to return

## Prerequisites

- KubeVirt cluster with `make cluster-sync` completed
- `virtctl` available via `./hack/virtctl.sh`

## Deploy (2 VMs)

```bash
kubectl create -f demo-multi-node-nvl/vmi.yaml
```

This creates:
- Secret `imex-cloudinit` (shared cloud-init with IMEX config for 5 nodes)
- VMIs `imex-vm-01` and `imex-vm-02`
- Headless Service `imex-service`

Wait a few minutes for the VMs to boot, install `nvidia-imex` via `dnf`, and start the daemon.

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

You should see connections established to all running peers and retry attempts for the nodes that aren't running yet.

## Scale Up

Add a third VM to the domain:

```bash
kubectl create -f demo-multi-node-nvl/scaleup-vmi.yaml
```

After boot and IMEX install (~2-3 minutes), check the logs on any running VM — you should see a new connection established to `imex-vm-03`.

## Scale Down

Remove the third VM:

```bash
kubectl delete -f demo-multi-node-nvl/scaleup-vmi.yaml
```

IMEX on the remaining VMs will detect the disconnect and continue retrying indefinitely until `imex-vm-03` comes back.

## Teardown

```bash
kubectl delete -f demo-multi-node-nvl/vmi.yaml
```

## IMEX Configuration Details

| Setting | Value | Purpose |
|---------|-------|---------|
| `DAEMONIZE` | `0` | Run in foreground (systemd manages lifecycle) |
| `IMEX_WAIT_FOR_QUORUM` | `NONE` | Start without waiting for all peers |
| `IMEX_NODE_DISCONNECTED_GRACE_TIME` | `-1` | Wait indefinitely for disconnected peers |
| `SERVER_PORT` | `50000` | IMEX peer communication port |
| `IMEX_CMD_PORT` | `50005` | IMEX command/control port |
| `--nogpu` | flag | Run without GPU hardware present |

## Files

| File | Description |
|------|-------------|
| `vmi.yaml` | Secret + 2 VMIs + headless service (5-node domain) |
| `scaleup-vmi.yaml` | Third VMI for scale-up testing |
