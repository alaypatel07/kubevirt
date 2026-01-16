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

3. **Verify DNS resolution:**
   ```bash
   # Inside vm-1
   ping -c 3 vm-2.my-service
   
   # Inside vm-2
   ping -c 3 vm-1.my-service
   ```

4. **Expected output in /var/log/vm-discovery.log:**
   ```
   Starting VM discovery service - target: vm-2.my-service
   Fri Jan 16 22:30:00 UTC 2026: vm-2.my-service is REACHABLE
   Fri Jan 16 22:30:30 UTC 2026: vm-2.my-service is REACHABLE
   ```

## Cleanup

```bash
kubectl delete -f demo-multi-node-nvl/vm-discovery-demo/vmi-pair-discovery.yaml
kubectl delete -f demo-multi-node-nvl/vm-discovery-demo/headless-service.yaml
```
