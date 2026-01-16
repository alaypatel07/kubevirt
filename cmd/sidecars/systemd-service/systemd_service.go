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

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"

	v1 "kubevirt.io/api/core/v1"

	cloudinit "kubevirt.io/kubevirt/pkg/cloud-init"
)

const (
	// AnnotationStartServices is the annotation key for specifying services to start
	AnnotationStartServices = "kubevirt.io/start-services"
)

// CloudConfig represents the cloud-init cloud-config format
type CloudConfig struct {
	RunCmd   []interface{}          `yaml:"runcmd,omitempty"`
	Password string                 `yaml:"password,omitempty"`
	ChPasswd map[string]interface{} `yaml:"chpasswd,omitempty"`
	// Preserve other fields
	Other map[string]interface{} `yaml:",inline"`
}

func preCloudInitIso(logger *log.Logger, vmiJSON, cloudInitDataJSON []byte) (string, error) {
	logger.Print("systemd-service hook: PreCloudInitIso callback invoked")

	// Unmarshal VMI to read annotations
	vmi := v1.VirtualMachineInstance{}
	if err := json.Unmarshal(vmiJSON, &vmi); err != nil {
		return "", fmt.Errorf("failed to unmarshal VMI spec: %w", err)
	}

	// Check for the start-services annotation
	servicesAnnotation, found := vmi.Annotations[AnnotationStartServices]
	if !found || servicesAnnotation == "" {
		logger.Print("systemd-service hook: No start-services annotation found, returning unchanged cloud-init data")
		return string(cloudInitDataJSON), nil
	}

	logger.Printf("systemd-service hook: Found services to start: %s", servicesAnnotation)

	// Parse the services list (comma-separated)
	services := parseServices(servicesAnnotation)
	if len(services) == 0 {
		logger.Print("systemd-service hook: No valid services found in annotation")
		return string(cloudInitDataJSON), nil
	}

	// Unmarshal CloudInitData
	cloudInitData := cloudinit.CloudInitData{}
	if err := json.Unmarshal(cloudInitDataJSON, &cloudInitData); err != nil {
		return "", fmt.Errorf("failed to unmarshal CloudInitData: %w", err)
	}

	// Modify UserData to include systemctl commands
	modifiedUserData, err := injectSystemctlCommands(logger, cloudInitData.UserData, services)
	if err != nil {
		return "", fmt.Errorf("failed to inject systemctl commands: %w", err)
	}

	cloudInitData.UserData = modifiedUserData

	// Marshal and return
	response, err := json.Marshal(cloudInitData)
	if err != nil {
		return "", fmt.Errorf("failed to marshal CloudInitData: %w", err)
	}

	logger.Printf("systemd-service hook: Successfully injected commands for %d services", len(services))
	return string(response), nil
}

// parseServices parses a comma-separated list of service names
func parseServices(annotation string) []string {
	var services []string
	for _, s := range strings.Split(annotation, ",") {
		service := strings.TrimSpace(s)
		if service != "" {
			// Ensure service name ends with .service if not already
			if !strings.HasSuffix(service, ".service") &&
				!strings.HasSuffix(service, ".socket") &&
				!strings.HasSuffix(service, ".timer") {
				service = service + ".service"
			}
			services = append(services, service)
		}
	}
	return services
}

// injectSystemctlCommands modifies the cloud-init UserData to include systemctl enable --now commands
func injectSystemctlCommands(logger *log.Logger, userData string, services []string) (string, error) {
	// Handle empty or minimal UserData
	if userData == "" {
		userData = "#cloud-config\n"
	}

	// Check if this is a cloud-config format
	if !strings.HasPrefix(strings.TrimSpace(userData), "#cloud-config") {
		// Not a cloud-config format, we cannot safely modify it
		logger.Print("systemd-service hook: UserData is not in cloud-config format, cannot inject commands")
		return userData, nil
	}

	// Parse existing cloud-config
	cloudConfig := make(map[string]interface{})
	// Remove the #cloud-config header before parsing
	yamlContent := strings.TrimPrefix(strings.TrimSpace(userData), "#cloud-config")
	yamlContent = strings.TrimSpace(yamlContent)

	if yamlContent != "" {
		if err := yaml.Unmarshal([]byte(yamlContent), &cloudConfig); err != nil {
			return "", fmt.Errorf("failed to parse cloud-config YAML: %w", err)
		}
	}

	// Get or create runcmd section
	var runcmd []interface{}
	if existingRuncmd, ok := cloudConfig["runcmd"]; ok {
		if runcmdSlice, ok := existingRuncmd.([]interface{}); ok {
			runcmd = runcmdSlice
		}
	}

	// Add systemctl enable --now commands for each service
	for _, service := range services {
		// Use array format for proper command parsing
		cmd := []string{"systemctl", "enable", "--now", service}
		runcmd = append(runcmd, cmd)
		logger.Printf("systemd-service hook: Added command to start service: %s", service)
	}

	cloudConfig["runcmd"] = runcmd

	// Marshal back to YAML
	modifiedYAML, err := yaml.Marshal(cloudConfig)
	if err != nil {
		return "", fmt.Errorf("failed to marshal modified cloud-config: %w", err)
	}

	// Reconstruct with header
	return "#cloud-config\n" + string(modifiedYAML), nil
}

func main() {
	var vmiJSON, cloudInitDataJSON string
	pflag.StringVar(&vmiJSON, "vmi", "", "Current VMI, in JSON format")
	pflag.StringVar(&cloudInitDataJSON, "cloud-init", "", "The CloudInitData, in JSON format")
	pflag.Parse()

	logger := log.New(os.Stderr, "systemd-service: ", log.Ldate|log.Ltime)

	if vmiJSON == "" || cloudInitDataJSON == "" {
		logger.Printf("Bad input: vmi length=%d, cloud-init length=%d", len(vmiJSON), len(cloudInitDataJSON))
		os.Exit(1)
	}

	result, err := preCloudInitIso(logger, []byte(vmiJSON), []byte(cloudInitDataJSON))
	if err != nil {
		logger.Printf("preCloudInitIso failed: %s", err)
		panic(err)
	}

	// Output the modified CloudInitData to stdout
	fmt.Println(result)
}
