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

## Future Work: Dynamic nodes_config.cfg via Virtiofs + ConfigMap

The current demo pre-populates `nodes_config.cfg` at boot via cloud-init with a fixed set of hostnames. This means the domain size is static — adding a 6th node requires updating the secret and restarting all VMs.

To make the node list truly dynamic, mount `nodes_config.cfg` from a Kubernetes ConfigMap via virtiofs:

1. Create a ConfigMap with the current node list
2. Add a `filesystems` entry to the VMI spec with `virtiofs: {}`
3. Add the ConfigMap as a volume
4. Inside the VM, mount the virtiofs filesystem and symlink `nodes_config.cfg`
5. Update the ConfigMap when nodes join/leave — changes propagate to all VMs immediately
6. Send `SIGUSR1` to the IMEX process to trigger re-read and DNS re-resolution (see `k8s-dra-driver-gpu` `IMEXDaemonUpdateLoopWithDNSNames` for reference)

This is the approach used by the `compute-domain-daemon` in `k8s-dra-driver-gpu`. The key difference from the current static approach: the domain size can change at runtime without restarting IMEX or the VMs.

The IMEX config overrides (`DAEMONIZE=0`, `IMEX_WAIT_FOR_QUORUM=NONE`) would remain in cloud-init since they don't change, but `nodes_config.cfg` would come from the virtiofs-mounted ConfigMap.

## Files

| File | Description |
|------|-------------|
| `vmi.yaml` | Secret + 2 VMIs + headless service (5-node domain) |
| `scaleup-vmi.yaml` | Third VMI for scale-up testing |
