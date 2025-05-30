// Code generated by applyconfiguration-gen. DO NOT EDIT.

package v1

import (
	consolev1 "github.com/openshift/api/console/v1"
)

// ConsolePluginBackendApplyConfiguration represents a declarative configuration of the ConsolePluginBackend type for use
// with apply.
type ConsolePluginBackendApplyConfiguration struct {
	Type    *consolev1.ConsolePluginBackendType     `json:"type,omitempty"`
	Service *ConsolePluginServiceApplyConfiguration `json:"service,omitempty"`
}

// ConsolePluginBackendApplyConfiguration constructs a declarative configuration of the ConsolePluginBackend type for use with
// apply.
func ConsolePluginBackend() *ConsolePluginBackendApplyConfiguration {
	return &ConsolePluginBackendApplyConfiguration{}
}

// WithType sets the Type field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the Type field is set to the value of the last call.
func (b *ConsolePluginBackendApplyConfiguration) WithType(value consolev1.ConsolePluginBackendType) *ConsolePluginBackendApplyConfiguration {
	b.Type = &value
	return b
}

// WithService sets the Service field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the Service field is set to the value of the last call.
func (b *ConsolePluginBackendApplyConfiguration) WithService(value *ConsolePluginServiceApplyConfiguration) *ConsolePluginBackendApplyConfiguration {
	b.Service = value
	return b
}
