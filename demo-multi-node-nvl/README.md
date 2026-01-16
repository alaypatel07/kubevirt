# Systemd Service Hook Sidecar Demo

This demo shows how to automatically start systemd services when a KubeVirt VM boots using a PreCloudInitIso hook sidecar.

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
kubectl apply -f demo-multi-node-nvl/vmi-with-systemd-service-sidecar.yaml
```

## Verify

1. **Check sidecar logs:**
   ```bash
   kubectl logs -l special=vmi-with-systemd-service-sidecar -c hook-sidecar-0
   ```

2. **Connect to VM:**
   ```bash
   kubectl virt console vmi-with-systemd-service-sidecar
   # Login: fedora / fedora
   ```

3. **Verify service is running:**
   ```bash
   systemctl status my-custom.service
   cat /var/log/my-custom.log
   ```

4. **Check cloud-init injected the command:**
   ```bash
   cat /var/log/cloud-init-output.log | grep systemctl
   ```

Press `Ctrl + ]` to exit console.

## How It Works

1. VMI annotation `kubevirt.io/start-services` specifies services to start
2. The `systemd-service` sidecar hooks into `PreCloudInitIso`
3. Sidecar injects `systemctl enable --now <service>` into cloud-init `runcmd`
4. Cloud-init executes the command on VM boot

## Cleanup

```bash
kubectl delete vmi vmi-with-systemd-service-sidecar
```
