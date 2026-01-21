# Multi-Node NVL Demos

This folder contains demos for KubeVirt features related to multi-node NVL workloads.

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

## Demos

| Demo | Description |
|------|-------------|
| [systemd-service-demo](systemd-service-demo/) | Start systemd services on VM boot using a hook sidecar |
| [vm-discovery-demo](vm-discovery-demo/) | Two VMs discovering each other via headless service DNS |
| [imex-demo](imex-demo/) | NVIDIA IMEX daemon running in --nogpu mode for multi-node communication testing |
