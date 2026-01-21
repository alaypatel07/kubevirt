# NVIDIA IMEX Daemon Demo

This demo shows two KubeVirt VMs running the NVIDIA IMEX (Inter-node Memory EXchange) daemon in `--nogpu` mode for testing multi-node communication without requiring actual GPUs.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Kubernetes                               │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │           ConfigMap: imex-nodes-config                  │    │
│  │   (nodes_config.cfg - update to add/remove nodes)       │    │
│  └─────────────────────────────────────────────────────────┘    │
│           │ virtiofs              │ virtiofs                     │
│           ▼                       ▼                              │
│  ┌─────────────────────┐ ┌─────────────────────┐                │
│  │      imex-vm-1      │ │      imex-vm-2      │                │
│  │                     │ │                     │                │
│  │  nvidia-imex daemon │◄──────────────────────►  nvidia-imex   │
│  │  (--nogpu mode)     │  gRPC :50000          │  (--nogpu)     │
│  └─────────────────────┘ └─────────────────────┘                │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              Headless Service: imex-service               │  │
│  │              Ports: 50000 (peer), 50005 (cmd)             │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## What This Demo Does

1. **Installs NVIDIA IMEX** via cloud-init on first boot
2. **Configures IMEX** with `--nogpu` flag for testing without GPU hardware
3. **Dynamic node discovery** via ConfigMap mounted with virtiofs
4. **Starts IMEX daemon** automatically after package installation

## Prerequisites

1. KubeVirt installed
2. VMs need internet access to download NVIDIA packages from CUDA repository

## Configuration

### DNS ClusterIP

Before deploying, update the DNS nameserver IP in `imex-cloud-init-secrets.yaml` to match your cluster's kube-dns ClusterIP:

```bash
# Find your cluster's kube-dns ClusterIP
DNS_IP=$(kubectl get svc -n kube-system kube-dns -o jsonpath='{.spec.clusterIP}')
echo "Your kube-dns ClusterIP: $DNS_IP"

# Update the secrets YAML (default is 10.96.0.10)
sed -i "s/10.96.0.10/$DNS_IP/g" demo-multi-node-nvl/imex-demo/imex-cloud-init-secrets.yaml
```

## Deploy

```bash
# 1. Create the ConfigMap for nodes configuration (can be updated dynamically)
kubectl apply -f demo-multi-node-nvl/imex-demo/imex-nodes-configmap.yaml

# 2. Create the cloud-init secrets
kubectl apply -f demo-multi-node-nvl/imex-demo/imex-cloud-init-secrets.yaml

# 3. Create the headless service (enables DNS-based peer discovery)
kubectl apply -f demo-multi-node-nvl/imex-demo/imex-headless-service.yaml

# 4. Create both VMs
kubectl apply -f demo-multi-node-nvl/imex-demo/vmi-pair-imex.yaml

# 5. Wait for VMs to be running
kubectl wait --for=jsonpath='{.status.phase}'=Running vmi/imex-vm-1 --timeout=600s
kubectl wait --for=jsonpath='{.status.phase}'=Running vmi/imex-vm-2 --timeout=600s
```

**Note:** First boot takes 5-10 minutes as VMs download and install nvidia-imex (~8MB + dependencies).

## Adding New Nodes Dynamically

The nodes configuration is stored in a ConfigMap and mounted via virtiofs. To add a new node:

```bash
# Edit the ConfigMap
kubectl edit configmap imex-nodes-config

# Add the new node entry:
#   imex-vm-3.imex-service.default.svc.cluster.local

# The change propagates to all VMs automatically via virtiofs!
# Then restart IMEX on each VM to pick up the new config:
# (inside each VM)
sudo systemctl restart nvidia-imex
```

## Verify

### 1. Check VMs are running

```bash
kubectl get vmi
```

### 2. Connect to imex-vm-1 and check IMEX status

```bash
kubectl virt console imex-vm-1
# Login: fedora / fedora

# Check if nvidia-imex is installed
rpm -qa | grep nvidia-imex

# Check virtiofs mount
mount | grep virtiofs
cat /mnt/imex-config/nodes_config.cfg

# Check symlink
ls -la /etc/nvidia-imex/nodes_config.cfg

# Check IMEX service status
sudo systemctl status nvidia-imex

# Check IMEX logs
sudo cat /var/log/nvidia-imex.log
```

### 3. Expected IMEX log output (success)

```
[INFO] IMEX version 590.48.01 is running in NO GPU MODE
[INFO] Treating node config entry 'imex-vm-1.imex-service.default.svc.cluster.local' as a host name.
[INFO] Treating node config entry 'imex-vm-2.imex-service.default.svc.cluster.local' as a host name.
[INFO] Identified this node as ID 0, using bind address of 'imex-vm-1.imex-service.default.svc.cluster.local'
[INFO] Creating gRPC channels to all peers (nPeers = 2).
[INFO] Started processing of incoming messages.
[INFO] Connection established to node 0 with address imex-vm-1.imex-service.default.svc.cluster.local
[INFO] Connection established to node 1 with address imex-vm-2.imex-service.default.svc.cluster.local
```

### 4. Test peer connectivity

```bash
# From imex-vm-1, ping imex-vm-2
ping -c 3 imex-vm-2.imex-service.default.svc.cluster.local

# Check IMEX ports are listening
sudo ss -tlnp | grep nvidia-imex

# Check established connections to peers
sudo ss -tnp | grep nvidia-imex
```

## IMEX Ports

| Port  | Purpose |
|-------|---------|
| 50000 | IMEX peer-to-peer gRPC communication (SERVER_PORT) |
| 50005 | IMEX command/control service (IMEX_CMD_PORT) |

## Troubleshooting

### IMEX service not starting

```bash
# Check if package installation completed
rpm -qa | grep nvidia-imex

# Check setup script logs
cat /var/log/imex-setup.log

# Check cloud-init logs
sudo cat /var/log/cloud-init-output.log | tail -100

# Try starting manually
sudo systemctl daemon-reload
sudo systemctl start nvidia-imex
journalctl -u nvidia-imex -n 50
```

### virtiofs not mounted

```bash
# Check if virtiofs is available
mount | grep virtiofs

# Try mounting manually
sudo mkdir -p /mnt/imex-config
sudo mount -t virtiofs imex-config /mnt/imex-config

# Check if config file is present
cat /mnt/imex-config/nodes_config.cfg
```

### DNS not resolving

```bash
# Check /etc/resolv.conf
cat /etc/resolv.conf

# Test full DNS name
nslookup imex-vm-2.imex-service.default.svc.cluster.local
```

### Peers not connecting

```bash
# Check nodes_config.cfg
cat /etc/nvidia-imex/nodes_config.cfg

# Check firewall (should be off by default on Fedora cloud images)
sudo firewall-cmd --state

# Test port connectivity
nc -zv imex-vm-2.imex-service.default.svc.cluster.local 50000
```

### Kind cluster networking issue (traffic drops after ~5 mins)

On Kind clusters with kindnet CNI, you may need to re-run the route fix:

```bash
sudo /etc/NetworkManager/dispatcher.d/99-fix-route
```

### Package download fails

VMs need internet access. If using a private cluster:
- Pre-bake nvidia-imex into a custom container disk image
- Or set up a local mirror of the NVIDIA CUDA repository

## How It Works

1. **ConfigMap** (`imex-nodes-config`) stores the list of IMEX nodes
2. **virtiofs** mounts the ConfigMap into each VM at `/mnt/imex-config`
3. **Symlink** connects `/etc/nvidia-imex/nodes_config.cfg` → `/mnt/imex-config/nodes_config.cfg`
4. **cloud-init** installs nvidia-imex and configures the `--nogpu` override
5. **Headless Service** provides DNS-based discovery for peer communication
6. **IMEX daemon** connects to peers via gRPC on port 50000

When you update the ConfigMap, changes propagate to all VMs automatically via virtiofs.

## Cleanup

```bash
kubectl delete -f demo-multi-node-nvl/imex-demo/vmi-pair-imex.yaml
kubectl delete -f demo-multi-node-nvl/imex-demo/imex-headless-service.yaml
kubectl delete -f demo-multi-node-nvl/imex-demo/imex-cloud-init-secrets.yaml
kubectl delete -f demo-multi-node-nvl/imex-demo/imex-nodes-configmap.yaml
```
