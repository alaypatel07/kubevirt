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
 * Copyright The KubeVirt Authors.
 *
 */

package dra

import (
	"fmt"

	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "kubevirt.io/api/core/v1"
)

const (
	// CPUDeviceClassName is the device class name for CPU DRA resources
	// Note: The actual DRA driver uses "dra.cpu" not "cpu.dra.k8s.io"
	CPUDeviceClassName = "dra.cpu"

	// CPUSocketIDAttribute is the attribute name for CPU socket ID
	CPUSocketIDAttribute = "dra.cpu/socketID"

	// CPUNUMANodeAttribute is the attribute name for NUMA node
	CPUNUMANodeAttribute = "dra.cpu/numaNode"
)

// GenerateCPUResourceClaim generates a ResourceClaim for CPU DRA based on the VMI's CPU topology.
// It creates one request per physical core (with all threads from that core) and adds constraints
// to ensure proper topology: cores stay together, and cores are grouped by socket.
func GenerateCPUResourceClaim(vmi *v1.VirtualMachineInstance) (*resourcev1.ResourceClaim, error) {
	cpu := vmi.Spec.Domain.CPU
	if cpu == nil || cpu.DRA == nil || !cpu.DRA.Auto {
		return nil, nil
	}

	// Normalize to defaults (Kubernetes-style: default to 1 if not specified)
	cores := cpu.Cores
	if cores == 0 {
		cores = 1
	}
	sockets := cpu.Sockets
	if sockets == 0 {
		sockets = 1
	}
	threads := cpu.Threads
	if threads == 0 {
		threads = 1
	}

	var requests []resourcev1.DeviceRequest
	var constraints []resourcev1.DeviceConstraint

	// ================================================================
	// STEP 1: Create one request per physical core
	// Each request asks for 'threads' number of CPUs (all threads from that core)
	// ================================================================
	for socketIdx := uint32(0); socketIdx < sockets; socketIdx++ {
		for coreIdx := uint32(0); coreIdx < cores; coreIdx++ {
			requestName := fmt.Sprintf("socket-%d-core-%d", socketIdx, coreIdx)

			requests = append(requests, resourcev1.DeviceRequest{
				Name: requestName,
				Exactly: &resourcev1.ExactDeviceRequest{
					DeviceClassName: CPUDeviceClassName,
					AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
					Count:           int64(threads),
				},
			})
		}
	}

	// ================================================================
	// STEP 2: Add socketID constraint for each VM socket
	// Groups all cores within a VM socket to come from the same physical socket
	// Note: We skip per-core constraints because the DRA CPU driver doesn't expose coreID
	// The driver will automatically allocate from complete cores based on the count
	// ================================================================
	for socketIdx := uint32(0); socketIdx < sockets; socketIdx++ {
		var socketRequests []string
		for coreIdx := uint32(0); coreIdx < cores; coreIdx++ {
			socketRequests = append(socketRequests,
				fmt.Sprintf("socket-%d-core-%d", socketIdx, coreIdx))
		}

		// All cores in this VM socket must come from the same physical socket
		attrName := resourcev1.FullyQualifiedName(CPUSocketIDAttribute)
		constraints = append(constraints, resourcev1.DeviceConstraint{
			MatchAttribute: &attrName,
			Requests:       socketRequests,
		})
	}

	// Create the ResourceClaim
	claimName := fmt.Sprintf("%s-cpu-claim", vmi.Name)
	claim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claimName,
			Namespace: vmi.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(vmi, v1.VirtualMachineInstanceGroupVersionKind),
			},
			Labels: map[string]string{
				v1.CreatedByLabel:      string(vmi.UID),
				v1.AppLabel:            "virt-launcher",
				"kubevirt.io/resource": "cpu-dra",
			},
		},
		Spec: resourcev1.ResourceClaimSpec{
			Devices: resourcev1.DeviceClaim{
				Requests:    requests,
				Constraints: constraints,
			},
		},
	}

	return claim, nil
}

// GetCPUClaimName returns the expected name of the auto-generated CPU ResourceClaim for a VMI.
func GetCPUClaimName(vmiName string) string {
	return fmt.Sprintf("%s-cpu-claim", vmiName)
}

// GetCPUClaimReference returns the claim reference for auto-generated CPU DRA.
// This is used to add the claim to the pod's ResourceClaims list.
func GetCPUClaimReference(vmiName string) string {
	return fmt.Sprintf("%s-cpu-claim-ref", vmiName)
}
