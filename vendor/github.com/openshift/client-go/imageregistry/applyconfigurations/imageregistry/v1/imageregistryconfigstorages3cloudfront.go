// Code generated by applyconfiguration-gen. DO NOT EDIT.

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ImageRegistryConfigStorageS3CloudFrontApplyConfiguration represents a declarative configuration of the ImageRegistryConfigStorageS3CloudFront type for use
// with apply.
type ImageRegistryConfigStorageS3CloudFrontApplyConfiguration struct {
	BaseURL    *string                   `json:"baseURL,omitempty"`
	PrivateKey *corev1.SecretKeySelector `json:"privateKey,omitempty"`
	KeypairID  *string                   `json:"keypairID,omitempty"`
	Duration   *metav1.Duration          `json:"duration,omitempty"`
}

// ImageRegistryConfigStorageS3CloudFrontApplyConfiguration constructs a declarative configuration of the ImageRegistryConfigStorageS3CloudFront type for use with
// apply.
func ImageRegistryConfigStorageS3CloudFront() *ImageRegistryConfigStorageS3CloudFrontApplyConfiguration {
	return &ImageRegistryConfigStorageS3CloudFrontApplyConfiguration{}
}

// WithBaseURL sets the BaseURL field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the BaseURL field is set to the value of the last call.
func (b *ImageRegistryConfigStorageS3CloudFrontApplyConfiguration) WithBaseURL(value string) *ImageRegistryConfigStorageS3CloudFrontApplyConfiguration {
	b.BaseURL = &value
	return b
}

// WithPrivateKey sets the PrivateKey field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the PrivateKey field is set to the value of the last call.
func (b *ImageRegistryConfigStorageS3CloudFrontApplyConfiguration) WithPrivateKey(value corev1.SecretKeySelector) *ImageRegistryConfigStorageS3CloudFrontApplyConfiguration {
	b.PrivateKey = &value
	return b
}

// WithKeypairID sets the KeypairID field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the KeypairID field is set to the value of the last call.
func (b *ImageRegistryConfigStorageS3CloudFrontApplyConfiguration) WithKeypairID(value string) *ImageRegistryConfigStorageS3CloudFrontApplyConfiguration {
	b.KeypairID = &value
	return b
}

// WithDuration sets the Duration field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the Duration field is set to the value of the last call.
func (b *ImageRegistryConfigStorageS3CloudFrontApplyConfiguration) WithDuration(value metav1.Duration) *ImageRegistryConfigStorageS3CloudFrontApplyConfiguration {
	b.Duration = &value
	return b
}
