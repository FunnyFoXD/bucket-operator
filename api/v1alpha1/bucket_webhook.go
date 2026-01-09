/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// nolint:unused
// log is for logging in this package.
var bucketlog = logf.Log.WithName("bucket-resource") // SetupWebhookWithManager will setup the manager to manage the webhooks.
func (r *Bucket) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		WithValidator(&BucketCustomValidator{}).
		WithDefaulter(&BucketCustomDefaulter{}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// +kubebuilder:webhook:path=/mutate-storage-mydomain-com-v1alpha1-bucket,mutating=true,failurePolicy=fail,sideEffects=None,groups=storage.mydomain.com,resources=buckets,verbs=create;update,versions=v1alpha1,name=mbucket-v1alpha1.kb.io,admissionReviewVersions=v1

// +kubebuilder:object:generate=false
// BucketCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind Bucket when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type BucketCustomDefaulter struct {
	// TODO(user): Add more fields as needed for defaulting
}

var _ webhook.CustomDefaulter = &BucketCustomDefaulter{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind Bucket.
func (d *BucketCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	bucket, ok := obj.(*Bucket)

	if !ok {
		return fmt.Errorf("expected an Bucket object but got %T", obj)
	}
	bucketlog.Info("Defaulting for Bucket", "name", bucket.GetName())

	// TODO(user): fill in your defaulting logic.

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: If you want to customise the 'path', use the flags '--defaulting-path' or '--validation-path'.
// +kubebuilder:webhook:path=/validate-storage-mydomain-com-v1alpha1-bucket,mutating=false,failurePolicy=fail,sideEffects=None,groups=storage.mydomain.com,resources=buckets,verbs=create;update,versions=v1alpha1,name=vbucket-v1alpha1.kb.io,admissionReviewVersions=v1

// +kubebuilder:object:generate=false
// BucketCustomValidator struct is responsible for validating the Bucket resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type BucketCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

var _ webhook.CustomValidator = &BucketCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type Bucket.
func (v *BucketCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	bucket, ok := obj.(*Bucket)
	if !ok {
		return nil, fmt.Errorf("expected a Bucket object but got %T", obj)
	}
	bucketlog.Info("Validation for Bucket upon creation", "name", bucket.GetName())

	var allErrors field.ErrorList
	specPath := field.NewPath("spec")

	// Validate size
	if bucket.Spec.Size < 1 {
		allErrors = append(allErrors, field.Invalid(
			specPath.Child("size"),
			bucket.Spec.Size,
			"size must be at least 1",
		))
	}

	// Validate name
	if bucket.Spec.Name != "" && len(bucket.Spec.Name) == 0 {
		allErrors = append(allErrors, field.Invalid(
			specPath.Child("name"),
			bucket.Spec.Name,
			"name cannot be empty if specified",
		))
	}

	// Validate region
	if bucket.Spec.Region != "" && len(bucket.Spec.Region) == 0 {
		allErrors = append(allErrors, field.Invalid(
			specPath.Child("region"),
			bucket.Spec.Region,
			"region cannot be empty if specified",
		))
	}

	// Validate storageType
	if bucket.Spec.StorageType != "" {
		validStorageTypes := map[string]bool{
			"standard":           true,
			"reduced-redundancy": true,
			"glacier":            true,
		}

		if !validStorageTypes[bucket.Spec.StorageType] {
			allErrors = append(allErrors, field.NotSupported(
				specPath.Child("storageType"),
				bucket.Spec.StorageType,
				[]string{"standard", "reduced-redundancy", "glacier"},
			))
		}
	}

	if len(allErrors) > 0 {
		return nil, apierrors.NewInvalid(
			schema.GroupKind{Group: "storage.mydomain.com", Kind: "Bucket"},
			bucket.Name,
			allErrors,
		)
	}

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type Bucket.
func (v *BucketCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	oldBucket, ok := oldObj.(*Bucket)
	if !ok {
		return nil, fmt.Errorf("expected a Bucket object for the oldObj but got %T", oldObj)
	}

	newBucket, ok := newObj.(*Bucket)
	if !ok {
		return nil, fmt.Errorf("expected a Bucket object for the newObj but got %T", newObj)
	}

	bucketlog.Info("Validation for Bucket upon update",
		"name", newBucket.GetName(),
		"oldSize", oldBucket.Spec.Size,
		"newSize", newBucket.Spec.Size,
	)

	var allErrs field.ErrorList
	specPath := field.NewPath("spec")

	// Validate size
	if newBucket.Spec.Size < 1 {
		allErrs = append(allErrs, field.Invalid(
			specPath.Child("size"),
			newBucket.Spec.Size,
			"size must be at least 1",
		))
	}

	// Validate storageType
	if newBucket.Spec.StorageType != "" {
		validStorageTypes := map[string]bool{
			"standard":           true,
			"reduced-redundancy": true,
			"glacier":            true,
		}

		if !validStorageTypes[newBucket.Spec.StorageType] {
			allErrs = append(allErrs, field.NotSupported(
				specPath.Child("storageType"),
				newBucket.Spec.StorageType,
				[]string{"standard", "reduced-redundancy", "glacier"},
			))
		}
	}

	// Size is not decreasible
	if newBucket.Spec.Size < oldBucket.Spec.Size {
		allErrs = append(allErrs, field.Invalid(
			specPath.Child("size"),
			newBucket.Spec.Size,
			fmt.Sprintf("size cannot be decreased from %d to %d", oldBucket.Spec.Size, newBucket.Spec.Size),
		))
	}

	// Name cannot be change after creation
	if oldBucket.Spec.Name != "" && newBucket.Spec.Name != oldBucket.Spec.Name {
		allErrs = append(allErrs, field.Forbidden(
			specPath.Child("name"),
			"name cannot be changed after creation",
		))
	}

	if len(allErrs) > 0 {
		return nil, apierrors.NewInvalid(
			schema.GroupKind{Group: "storage.mydomain.com", Kind: "Bucket"},
			newBucket.Name,
			allErrs,
		)
	}

	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type Bucket.
func (v *BucketCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	bucket, ok := obj.(*Bucket)
	if !ok {
		return nil, fmt.Errorf("expected a Bucket object but got %T", obj)
	}
	bucketlog.Info("Validation for Bucket upon deletion", "name", bucket.GetName())

	// TODO(user): fill in your validation logic upon object deletion.

	return nil, nil
}
