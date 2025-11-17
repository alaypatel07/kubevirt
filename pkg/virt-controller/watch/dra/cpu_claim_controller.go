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
	"context"
	"fmt"

	k8sv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"

	drautil "kubevirt.io/kubevirt/pkg/dra"
	"kubevirt.io/kubevirt/pkg/pointer"
)

// InjectCPUResourceClaim injects a CPU ResourceClaim reference into the VMI spec if auto mode is enabled.
// It creates the ResourceClaim object if it doesn't exist and adds a PodResourceClaim reference to
// vmi.Spec.ResourceClaims. This function should be called before creating the pod.
func InjectCPUResourceClaim(vmi *v1.VirtualMachineInstance, clientset kubecli.KubevirtClient) error {
	if !drautil.IsCPUDRAAuto(vmi) {
		return nil
	}

	logger := log.Log.Object(vmi)
	logger.V(4).Infof("Injecting auto-generated CPU ResourceClaim for VMI")

	// Generate the ResourceClaim
	claim, err := GenerateCPUResourceClaim(vmi)
	if err != nil {
		return fmt.Errorf("failed to generate CPU ResourceClaim: %v", err)
	}

	// Create the ResourceClaim if it doesn't exist
	// Note: clientset embeds kubernetes.Interface, so we can access ResourceV1() directly
	existingClaim, err := clientset.ResourceV1().ResourceClaims(vmi.Namespace).Get(context.TODO(), claim.Name, metav1.GetOptions{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to check for existing ResourceClaim: %v", err)
		}
		// Create the claim
		logger.V(4).Infof("Creating CPU ResourceClaim %s/%s", claim.Namespace, claim.Name)
		_, err = clientset.ResourceV1().ResourceClaims(vmi.Namespace).Create(context.TODO(), claim, metav1.CreateOptions{})
		if err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create CPU ResourceClaim: %v", err)
		}
	} else {
		logger.V(4).Infof("CPU ResourceClaim %s/%s already exists", existingClaim.Namespace, existingClaim.Name)
	}

	// Add PodResourceClaim to vmi.Spec.ResourceClaims if not already present
	claimRefName := GetCPUClaimReference(vmi.Name)
	claimExists := false
	for _, rc := range vmi.Spec.ResourceClaims {
		if rc.Name == claimRefName {
			claimExists = true
			break
		}
	}

	if !claimExists {
		logger.V(4).Infof("Adding PodResourceClaim reference %s to VMI spec", claimRefName)
		vmi.Spec.ResourceClaims = append(vmi.Spec.ResourceClaims, k8sv1.PodResourceClaim{
			Name:              claimRefName,
			ResourceClaimName: pointer.P(claim.Name),
		})
	}

	return nil
}
