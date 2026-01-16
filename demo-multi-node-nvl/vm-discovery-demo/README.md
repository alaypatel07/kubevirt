# Two VM Discovery Demo

This demo shows two VMs discovering and pinging each other via a Kubernetes headless service using DNS.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Kubernetes                           │
│  ┌─────────────────────────────────────────────────┐   │
│  │         Headless Service: my-service            │   │
│  └─────────────────────────────────────────────────┘   │
│           │                         │                   │
│           ▼                         ▼                   │
│  ┌─────────────────┐       ┌─────────────────┐         │
│  │     vm-1        │       │     vm-2        │         │
│  │                 │ ping  │                 │         │
│  │ vm-discovery.sh │◄─────►│ vm-discovery.sh │         │
│  └─────────────────┘       └─────────────────┘         │
│                                                         │
│  DNS: vm-1.my-service      DNS: vm-2.my-service        │
└─────────────────────────────────────────────────────────┘
```

## Prerequisites

1. KubeVirt installed with `Sidecar` feature gate enabled:
   ```bash
   kubectl patch kubevirt kubevirt -n kubevirt --type='merge' \
     -p='{"spec":{"configuration":{"developerConfiguration":{"featureGates":["Sidecar"]}}}}'
   ```

2. Build and push the sidecar image:
   ```bash
   make bazel-push-images PUSH_TARGETS="systemd-service-sidecar"
   ```

## Configuration

### DNS ClusterIP (Important!)

Before deploying, update the DNS nameserver IP in `vmi-pair-discovery.yaml` to match your cluster's kube-dns ClusterIP:

```bash
# Find your cluster's kube-dns ClusterIP
kubectl get svc -n kube-system kube-dns -o jsonpath='{.spec.clusterIP}'
```

Common values:
- `10.96.0.10` - Kubernetes default
- `10.64.0.10` - Some Kind clusters

Update both occurrences in the YAML using sed:
```bash
# Replace 10.96.0.10 with your cluster's kube-dns ClusterIP
DNS_IP=$(kubectl get svc -n kube-system kube-dns -o jsonpath='{.spec.clusterIP}')
sed -i "s/10.96.0.10/$DNS_IP/g" demo-multi-node-nvl/vm-discovery-demo/vmi-pair-discovery.yaml
```

Or manually edit the YAML:
```yaml
resolv_conf:
  nameservers:
    - <YOUR_KUBE_DNS_IP>  # Replace with your cluster's kube-dns ClusterIP
```

### Kind Cluster Workaround

This demo includes a workaround for Kind clusters using kindnet CNI. The `runcmd` section removes a local route that breaks pod-to-pod communication:

```yaml
runcmd:
  - |
    # Fix for kindnet point-to-point routing
    IFACE=$(ip route | grep "^default" | awk '{print $5}')
    CIDR=$(ip route | grep "proto kernel" | grep "$IFACE" | awk '{print $1}')
    if [ -n "$CIDR" ]; then
      ip route del "$CIDR" dev "$IFACE" 2>/dev/null || true
    fi
```

**Note:** This workaround is typically NOT needed on production clusters with standard CNIs (Calico, Cilium, Flannel).

## Deploy

```bash
# Create the headless service first
kubectl apply -f demo-multi-node-nvl/vm-discovery-demo/headless-service.yaml

# Create both VMs
kubectl apply -f demo-multi-node-nvl/vm-discovery-demo/vmi-pair-discovery.yaml

# Wait for VMs to be running
kubectl wait --for=jsonpath='{.status.phase}'=Running vmi/vm-1 --timeout=300s
kubectl wait --for=jsonpath='{.status.phase}'=Running vmi/vm-2 --timeout=300s
```

## Verify

1. **Check both VMs are running:**
   ```bash
   kubectl get vmi
   ```

2. **Connect to vm-1 and check discovery logs:**
   ```bash
   kubectl virt console vm-1
   # Login: fedora / fedora
   
   # Check discovery service status
   systemctl status vm-discovery.service
   
   # Check discovery logs
   cat /var/log/vm-discovery.log
   ```

3. **Verify DNS resolution and connectivity:**
   ```bash
   # Inside vm-1 - test DNS
   nslookup vm-2.my-service.default.svc.cluster.local
   
   # Inside vm-1 - test ping
   ping -c 3 vm-2.my-service.default.svc.cluster.local
   ```

4. **Expected output in /var/log/vm-discovery.log:**
   ```
   Starting VM discovery service - target: vm-2.my-service.default.svc.cluster.local
   Fri Jan 16 22:30:00 UTC 2026: vm-2.my-service.default.svc.cluster.local is REACHABLE
   Fri Jan 16 22:30:30 UTC 2026: vm-2.my-service.default.svc.cluster.local is REACHABLE
   ```

## Troubleshooting

### DNS not resolving
- Verify kube-dns ClusterIP is correct in the YAML
- Check `/etc/resolv.conf` inside the VM
- Test with full DNS name: `nslookup vm-2.my-service.default.svc.cluster.local`

### Ping fails between VMs
- On Kind clusters, ensure the route fix `runcmd` is present
- Check VM has correct IP: `ip addr`
- Verify headless service has endpoints: `kubectl get endpoints my-service`

### ClusterIP not reachable
- ClusterIP services can't be pinged (ICMP) - only service ports work
- DNS queries (UDP 53) should work to the kube-dns ClusterIP

## Cleanup

```bash
kubectl delete -f demo-multi-node-nvl/vm-discovery-demo/vmi-pair-discovery.yaml
kubectl delete -f demo-multi-node-nvl/vm-discovery-demo/headless-service.yaml
```
