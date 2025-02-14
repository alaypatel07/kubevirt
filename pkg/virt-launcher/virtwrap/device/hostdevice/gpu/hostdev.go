/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright 2021 Red Hat, Inc.
 *
 */

package gpu

import (
	"fmt"

	v1 "kubevirt.io/api/core/v1"

	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/api"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/device"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/device/hostdevice"
)

const (
	failedCreateGPUHostDeviceFmt = "failed to create GPU host-devices: %v"
	AliasPrefix                  = "gpu-"
	DefaultDisplayOn             = true
)

func CreateHostDevices(vmi *v1.VirtualMachineInstance) ([]api.HostDevice, error) {
	vmiGPUs := vmi.Spec.Domain.Devices.GPUs
	hostDevices, err := CreateHostDevicesFromPools(vmiGPUs, NewPCIAddressPool(vmiGPUs), NewMDEVAddressPool(vmiGPUs))
	if err != nil {
		return nil, err
	}

	draPCIHostDevices, err := getDRAPCIHostDevices(vmi)
	if err != nil {
		return nil, fmt.Errorf(failedCreateGPUHostDeviceFmt, err)
	}
	draMDEVHostDevices, err := getDRAMDEVHostDevices(vmi, DefaultDisplayOn)
	if err != nil {
		return nil, fmt.Errorf(failedCreateGPUHostDeviceFmt, err)
	}

	hostDevices = append(hostDevices, draPCIHostDevices...)
	hostDevices = append(hostDevices, draMDEVHostDevices...)

	return hostDevices, nil
}

func CreateHostDevicesFromPools(vmiGPUs []v1.GPU, pciAddressPool, mdevAddressPool hostdevice.AddressPooler) ([]api.HostDevice, error) {
	pciPool := hostdevice.NewBestEffortAddressPool(pciAddressPool)
	mdevPool := hostdevice.NewBestEffortAddressPool(mdevAddressPool)

	hostDevicesMetaData := createHostDevicesMetadata(vmiGPUs)
	pciHostDevices, err := hostdevice.CreatePCIHostDevices(hostDevicesMetaData, pciPool)
	if err != nil {
		return nil, fmt.Errorf(failedCreateGPUHostDeviceFmt, err)
	}
	mdevHostDevices, err := hostdevice.CreateMDEVHostDevices(hostDevicesMetaData, mdevPool, DefaultDisplayOn)
	if err != nil {
		return nil, fmt.Errorf(failedCreateGPUHostDeviceFmt, err)
	}

	hostDevices := append(pciHostDevices, mdevHostDevices...)

	if err := validateCreationOfAllDevices(vmiGPUs, hostDevices); err != nil {
		return nil, fmt.Errorf(failedCreateGPUHostDeviceFmt, err)
	}

	return hostDevices, nil
}

func getDRAPCIHostDevices(vmi *v1.VirtualMachineInstance) ([]api.HostDevice, error) {
	hostDevices := []api.HostDevice{}
	if vmi.Status.DeviceStatus != nil {
		for _, gpu := range vmi.Status.DeviceStatus.GPUStatuses {
			if gpu.DeviceResourceClaimStatus != nil {
				if pciAddress, ok := gpu.DeviceResourceClaimStatus.DeviceAttributes["pciAddress"]; ok {
					hostAddr, err := device.NewPciAddressField(*pciAddress.String)
					if err != nil {
						return nil, fmt.Errorf("failed to create PCI device for %s: %v", gpu.Name, err)
					}
					hostDevices = append(hostDevices, api.HostDevice{
						Alias:   api.NewUserDefinedAlias(AliasPrefix + gpu.Name),
						Source:  api.HostDeviceSource{Address: hostAddr},
						Type:    api.HostDevicePCI,
						Managed: "no",
					})
				}
			}
		}
	}
	return hostDevices, nil
}

func getDRAMDEVHostDevices(vmi *v1.VirtualMachineInstance, defaultDisplayOn bool) ([]api.HostDevice, error) {
	hostDevices := []api.HostDevice{}
	if vmi.Status.DeviceStatus != nil {
		for _, gpu := range vmi.Status.DeviceStatus.GPUStatuses {
			if gpu.DeviceResourceClaimStatus != nil {
				if _, ok := gpu.DeviceResourceClaimStatus.DeviceAttributes["pciAddress"]; ok {
					continue
				}
				if uuid, ok := gpu.DeviceResourceClaimStatus.DeviceAttributes["uuid"]; ok {
					hostDevice := api.HostDevice{
						Alias: api.NewUserDefinedAlias(AliasPrefix + gpu.Name),
						Source: api.HostDeviceSource{
							Address: &api.Address{
								UUID: *uuid.String,
							},
						},
						Type:  api.HostDeviceMDev,
						Mode:  "subsystem",
						Model: "vfio-pci",
					}
					gpuSpec := getGPUSpecFromName(vmi, gpu.Name)
					if gpuSpec.VirtualGPUOptions != nil {
						if gpuSpec.VirtualGPUOptions.Display != nil {
							displayEnabled := gpuSpec.VirtualGPUOptions.Display.Enabled
							if displayEnabled == nil || *displayEnabled {
								hostDevice.Display = "on"
								if gpuSpec.VirtualGPUOptions.Display.RamFB == nil || *gpuSpec.VirtualGPUOptions.Display.RamFB.Enabled {
									hostDevice.RamFB = "on"
								}
							}
						}
					}
					hostDevices = append(hostDevices, hostDevice)
				}
			}
		}
	}
	if defaultDisplayOn && !isVgpuDisplaySet(vmi.Spec.Domain.Devices.GPUs) && len(hostDevices) > 0 {
		hostDevices[0].Display = "on"
		hostDevices[0].RamFB = "on"
	}
	return hostDevices, nil
}

func isVgpuDisplaySet(gpuSpecs []v1.GPU) bool {
	for _, gpu := range gpuSpecs {
		if gpu.VirtualGPUOptions != nil &&
			gpu.VirtualGPUOptions.Display != nil {
			return true
		}
	}
	return false
}

func getGPUSpecFromName(vmi *v1.VirtualMachineInstance, gpu string) *v1.GPU {
	for _, g := range vmi.Spec.Domain.Devices.GPUs {
		if g.Name == gpu {
			return &g
		}
	}
	return nil
}

func createHostDevicesMetadata(vmiGPUs []v1.GPU) []hostdevice.HostDeviceMetaData {
	var hostDevicesMetaData []hostdevice.HostDeviceMetaData
	for _, dev := range vmiGPUs {
		if dev.Claim == nil {
			hostDevicesMetaData = append(hostDevicesMetaData, hostdevice.HostDeviceMetaData{
				AliasPrefix:       AliasPrefix,
				Name:              dev.Name,
				ResourceName:      dev.DeviceName,
				VirtualGPUOptions: dev.VirtualGPUOptions,
			})
		}
	}
	return hostDevicesMetaData
}

// validateCreationOfAllDevices validates that all specified GPU/s have a matching host-device.
// On validation failure, an error is returned.
// The validation assumes that the assignment of a device to a specified GPU is correct,
// therefore a simple quantity check is sufficient.
func validateCreationOfAllDevices(gpus []v1.GPU, hostDevices []api.HostDevice) error {
	gpusWithDP := []v1.GPU{}
	for _, gpu := range gpusWithDP {
		if gpu.Claim != nil {
			continue
		}
		gpusWithDP = append(gpusWithDP, gpu)
	}

	if len(gpusWithDP) != len(hostDevices) {
		return fmt.Errorf(
			"the number of GPU/s do not match the number of devices:\nGPU: %v\nDevice: %v", gpus, hostDevices,
		)
	}
	return nil
}
