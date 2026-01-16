# Systemd Service Hook Sidecar Demo

This demo shows how to automatically start systemd services when a KubeVirt VM boots using a PreCloudInitIso hook sidecar.

## Deploy

```bash
kubectl apply -f demo-multi-node-nvl/systemd-service-demo/vmi-with-systemd-service-sidecar.yaml
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
   ```
   [fedora@vmi-with-systemd-service-sidecar ~]$ systemctl status my-custom.service
   ● my-custom.service - My Custom Service
        Loaded: loaded (/etc/systemd/system/my-custom.service; enabled; preset: di>
       Drop-In: /usr/lib/systemd/system/service.d
                └─10-timeout-abort.conf
        Active: active (running) since Fri 2026-01-16 21:58:12 UTC; 1min 32s ago
    Invocation: eacd37fb95104dc080ecedcae81bb2e4
      Main PID: 902 (my-custom-scrip)
         Tasks: 2 (limit: 1015)
        Memory: 728K (peak: 1.1M)
           CPU: 8ms
        CGroup: /system.slice/my-custom.service
                ├─902 /bin/bash /usr/local/bin/my-custom-script.sh
                └─928 sleep 60

   [fedora@vmi-with-systemd-service-sidecar ~]$ cat /var/log/my-custom.log
   Custom service running at Fri Jan 16 09:58:12 PM UTC 2026
   Custom service running at Fri Jan 16 09:59:12 PM UTC 2026
   Custom service running at Fri Jan 16 10:00:12 PM UTC 2026
   ```

4. **Check cloud-init injected the command:**
   ```
   [root@vmi-with-systemd-service-sidecar ~]# cat /var/log/cloud-init-output.log | grep -C 7 my-custom
   ci-info: | Route | Destination | Gateway | Interface | Flags |
   ci-info: +-------+-------------+---------+-----------+-------+
   ci-info: |   0   |  fe80::/64  |    ::   |   enp1s0  |   U   |
   ci-info: |   2   |  multicast  |    ::   |   enp1s0  |   U   |
   ci-info: +-------+-------------+---------+-----------+-------+
   Cloud-init v. 25.2 running 'modules:config' at Fri, 16 Jan 2026 21:58:11 +0000. Up 10.25 seconds.
   Cloud-init v. 25.2 running 'modules:final' at Fri, 16 Jan 2026 21:58:11 +0000. Up 10.31 seconds.
   Created symlink '/etc/systemd/system/multi-user.target.wants/my-custom.service' → '/etc/systemd/system/my-custom.service'.
   Cloud-init v. 25.2 finished at Fri, 16 Jan 2026 21:58:12 +0000. Datasource DataSourceNoCloud [seed=/dev/vdb].  Up 10.70 seconds
   Generating public/private rsa key pair.
   Your identification has been saved in /etc/ssh/ssh_host_rsa_key
   Your public key has been saved in /etc/ssh/ssh_host_rsa_key.pub
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
