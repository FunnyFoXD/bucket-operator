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

package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	storagev1alpha1 "github.com/funnyfoxd/bucket-operator/api/v1alpha1"
	"github.com/go-logr/logr"
)

const (
	// Finalizer
	finalizer = "bucket-controller.storage.mydomain.com/finalizer"

	// ConfigMap
	configMapSuffix = "-config"
	appLabelKey     = "app"
	bucketLabelKey  = "bucket-name"
	appName         = "bucket-operator"

	// Condition types
	conditionTypeReady       = "Ready"
	conditionTypeAvailable   = "Available"
	conditionTypeProgressing = "Progressing"

	// Condition reasons
	reasonReconciling             = "Reconciling"
	reasonConfigMapCreated        = "ConfigMapCreated"
	reasonConfigMapCreationFailed = "ConfigMapCreationFailed"
	reasonBucketAvailable         = "BucketAvailable"
	reasonReconcileComplete       = "ReconcileComplete"
	reasonReconcileFailed         = "ReconcileFailed"

	// Retry configurations
	retryAnnotationKey = "bucket.storage.mydomain.com/retry-count"
	maxRetryAttempt    = 5
	baseRetryDelay     = time.Second * 2
	maxRetryDelay      = time.Minute
)

// TemporaryError represents temporary error, that can be fixed with retry
type TemporaryError struct {
	Err error
}

func (e *TemporaryError) Error() string {
	return fmt.Sprintf("temporary error: %v", e.Err)
}

func (e *TemporaryError) Unwrap() error {
	return e.Err
}

// BucketReconciler reconciles a Bucket object
type BucketReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=storage.mydomain.com,resources=buckets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storage.mydomain.com,resources=buckets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=storage.mydomain.com,resources=buckets/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Bucket object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.4/pkg/reconcile
func (r *BucketReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Initialize logger with context
	log := logf.FromContext(ctx).WithValues(
		"bucket", req.Name,
		"namespace", req.Namespace,
	)

	log.V(1).Info("Starting reconciliation")

	// Read and fetch bucket resource
	var bucket storagev1alpha1.Bucket
	if err := r.Get(ctx, req.NamespacedName, &bucket); err != nil {
		if !errors.IsNotFound(err) {
			log.Error(err, "unable to fetch Bucket")
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Log current state
	log.V(1).Info("Bucket resource fetched",
		"size", bucket.Spec.Size,
		"name", bucket.Spec.Name,
		"region", bucket.Spec.Region,
		"storageType", bucket.Spec.StorageType,
		"deletionTimestamp", bucket.DeletionTimestamp,
	)

	// Reconcile in progress mark
	r.setProgressingCondition(&bucket, metav1.ConditionTrue, reasonReconciling, "Reconciling bucket resource")

	// Deletion process
	if !bucket.DeletionTimestamp.IsZero() {
		log.Info("Processing bucket deletion")
		return r.handleDeletion(ctx, &bucket, log)
	}

	// Ensure, that finalizer is added
	if err := r.ensureFinalizer(ctx, &bucket, log); err != nil {
		return r.handleErrorWithRetry(ctx, &bucket, err, log)
	}

	// Create/Update ConfigMaps
	if err := r.reconcileConfigMap(ctx, &bucket, log); err != nil {
		log.Error(err, "failed to reconcile ConfigMap")
		r.setConditionsOnError(&bucket, err)

		// Try to update status
		if updateErr := r.Status().Update(ctx, &bucket); updateErr != nil {
			log.Error(updateErr, "unable to update status after error")
		}

		// If temporary error, retry
		return r.handleErrorWithRetry(ctx, &bucket, err, log)
	}

	// Set condition on success
	r.setConditionsOnSuccess(&bucket)

	// Reset count with success
	r.resetRetryCount(&bucket)
	if err := r.Update(ctx, &bucket); err != nil {
		log.V(1).Info("unable to reset retry count, but it's not critical",
			"error", err,
		)
	}

	// Update status
	if err := r.updateStatus(ctx, &bucket, log); err != nil {
		return r.handleErrorWithRetry(ctx, &bucket, err, log)
	}

	log.Info("Reconciliation completed successfully")
	return ctrl.Result{}, nil
}

// handleDeletion process deletion bucket resource
func (r *BucketReconciler) handleDeletion(ctx context.Context, bucket *storagev1alpha1.Bucket, log logr.Logger) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(bucket, finalizer) {
		log.V(1).Info("No finalizer present, deletion can proceed")
		return ctrl.Result{}, nil
	}

	log.Info("Processing bucket deletion",
		"finalizer", finalizer,
	)

	cmName := bucket.Name + configMapSuffix
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: bucket.Namespace,
		},
	}

	log.Info("Deleting associated ConfigMap",
		"configmap", cmName,
		"namespace", bucket.Namespace,
	)

	if err := r.Delete(ctx, cm); err != nil {
		if !errors.IsNotFound(err) {
			log.Error(err, "unable to delete ConfigMap",
				"configmap", cmName,
				"namespace", bucket.Namespace)

			return r.handleErrorWithRetry(ctx, bucket, err, log)
		}
		log.Info("ConfigMap already deleted",
			"configmap", cmName,
		)
	} else {
		log.Info("ConfigMap deleted successfully",
			"configmap", cmName,
		)
	}

	log.Info("Removing finalizer")
	controllerutil.RemoveFinalizer(bucket, finalizer)

	if err := r.Update(ctx, bucket); err != nil {
		log.Error(err, "couldn't remove finalizer",
			"finalizer", finalizer,
		)

		return r.handleErrorWithRetry(ctx, bucket, err, log)
	}

	log.Info("Bucket is deleted successfully")
	return ctrl.Result{}, nil
}

// ensureFinalizer add finalizer if it is not in resource
func (r *BucketReconciler) ensureFinalizer(ctx context.Context, bucket *storagev1alpha1.Bucket, log logr.Logger) error {
	if controllerutil.ContainsFinalizer(bucket, finalizer) {
		return nil
	}

	log.Info("Adding finalizer...")
	controllerutil.AddFinalizer(bucket, finalizer)

	if err := r.Update(ctx, bucket); err != nil {
		log.Error(err, "unable to add finalizer",
			"bucket", bucket.Name,
			"namespace", bucket.Namespace,
		)

		if r.isTemporaryError(err) {
			return &TemporaryError{Err: err}
		}

		return err
	}

	log.Info("Finalizer added successfully",
		"finalizer", finalizer,
		"bucket", bucket.Name,
	)

	return nil
}

// reconcileConfigMap create or update ConfigMap
func (r *BucketReconciler) reconcileConfigMap(ctx context.Context, bucket *storagev1alpha1.Bucket, log logr.Logger) error {
	log.Info("Reconciling ConfigMap",
		"configmap", bucket.Name+configMapSuffix,
		"namespace", bucket.Namespace,
	)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bucket.Name + configMapSuffix,
			Namespace: bucket.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = map[string]string{
			appLabelKey:    appName,
			bucketLabelKey: bucket.Name,
		}

		cm.Data = r.buildConfigMapData(bucket)

		return ctrl.SetControllerReference(bucket, cm, r.Scheme)
	})
	if err != nil {
		log.Error(err, "unable to create/update ConfigMap",
			"configmap", cm.Name,
			"namespace", cm.Namespace,
			"bucket", bucket.Name,
		)

		if r.isTemporaryError(err) {
			return &TemporaryError{Err: err}
		}

		return err
	}

	log.Info("ConfigMap created/updated successfully",
		"configmap", cm.Name,
		"namespace", cm.Namespace,
	)

	return nil
}

// buildConfigMapData return ready ConfigMap
func (r *BucketReconciler) buildConfigMapData(bucket *storagev1alpha1.Bucket) map[string]string {
	data := map[string]string{
		"size":  fmt.Sprintf("%d", bucket.Spec.Size),
		"owner": bucket.Name,
	}

	if bucket.Spec.Name != "" {
		data["name"] = bucket.Spec.Name
	}
	if bucket.Spec.Region != "" {
		data["region"] = bucket.Spec.Region
	}
	if bucket.Spec.StorageType != "" {
		data["storageType"] = bucket.Spec.StorageType
	}

	return data
}

// setConditionsOnError set all condition types for error
func (r *BucketReconciler) setConditionsOnError(bucket *storagev1alpha1.Bucket, err error) {
	if r.isTemporaryError(err) {
		r.setReadyCondition(bucket, metav1.ConditionFalse, reasonConfigMapCreationFailed,
			fmt.Sprintf("Temporary error creating ConfigMap: %v. Will retry", err))
		r.setProgressingCondition(bucket, metav1.ConditionFalse, reasonConfigMapCreationFailed,
			fmt.Sprintf("Temporary error creating ConfigMap: %v. Will retry", err))
		r.setAvailableCondition(bucket, metav1.ConditionFalse, reasonReconcileFailed,
			fmt.Sprintf("Temporary error reconciling bucket: %v. Will retry", err))
	} else {
		r.setReadyCondition(bucket, metav1.ConditionFalse, reasonConfigMapCreationFailed,
			fmt.Sprintf("Failed to create ConfigMap: %v", err))
		r.setProgressingCondition(bucket, metav1.ConditionFalse, reasonConfigMapCreationFailed,
			fmt.Sprintf("Failed to create ConfigMap: %v", err))
		r.setAvailableCondition(bucket, metav1.ConditionFalse, reasonReconcileFailed,
			fmt.Sprintf("Reconcile failed: %v", err))
	}
}

// setConditionsOnSuccess set all condition types for success
func (r *BucketReconciler) setConditionsOnSuccess(bucket *storagev1alpha1.Bucket) {
	r.setReadyCondition(bucket, metav1.ConditionTrue, reasonConfigMapCreated,
		"ConfigMap successfully created")
	r.setProgressingCondition(bucket, metav1.ConditionFalse, reasonBucketAvailable,
		"Bucket is available and ready")
	r.setAvailableCondition(bucket, metav1.ConditionTrue, reasonReconcileComplete,
		"Reconciliation completed successfully")
}

// setProgressingCondition set progressing condition on true/false
func (r *BucketReconciler) setProgressingCondition(bucket *storagev1alpha1.Bucket, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&bucket.Status.Conditions, metav1.Condition{
		Type:    conditionTypeProgressing,
		Status:  status,
		Reason:  reason,
		Message: message,
	})
}

// setReadyCondition set ready condition on true/false
func (r *BucketReconciler) setReadyCondition(bucket *storagev1alpha1.Bucket, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&bucket.Status.Conditions, metav1.Condition{
		Type:    conditionTypeReady,
		Status:  status,
		Reason:  reason,
		Message: message,
	})
}

// setAvailableCondition set available condition on true/false
func (r *BucketReconciler) setAvailableCondition(bucket *storagev1alpha1.Bucket, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&bucket.Status.Conditions, metav1.Condition{
		Type:    conditionTypeAvailable,
		Status:  status,
		Reason:  reason,
		Message: message,
	})
}

// updateStatus update resource status in a cluster
func (r *BucketReconciler) updateStatus(ctx context.Context, bucket *storagev1alpha1.Bucket, log logr.Logger) error {
	if err := r.Status().Update(ctx, bucket); err != nil {
		if errors.IsConflict(err) {
			log.Info("Status conflict, will retry",
				"bucket", bucket.Name,
				"namespace", bucket.Namespace,
				"error", err,
			)
			return &TemporaryError{Err: err}
		}
		log.Error(err, "unable to update status",
			"bucket", bucket.Name,
			"namespace", bucket.Namespace)
		return err
	}

	log.Info("Status update successfully",
		"bucket", bucket.Name,
		"namespace", bucket.Namespace,
	)

	return nil
}

// isTemporaryError checks if error is temporary
func (r *BucketReconciler) isTemporaryError(err error) bool {
	if err == nil {
		return false
	}

	// Conflicts is temporary
	if errors.IsConflict(err) {
		return true
	}

	// Timeouts is temporary
	if errors.IsServerTimeout(err) || errors.IsTimeout(err) {
		return true
	}

	// Service unavailable is temporary
	if errors.IsServiceUnavailable(err) {
		return true
	}

	// Too many requests is temporary
	if errors.IsTooManyRequests(err) {
		return true
	}

	// Internal server can be a temporary
	if errors.IsInternalError(err) {
		return true
	}

	return false
}

// handleErrorWithRetry handles error using retry logic
func (r *BucketReconciler) handleErrorWithRetry(ctx context.Context, bucket *storagev1alpha1.Bucket, err error, log logr.Logger) (ctrl.Result, error) {
	if !r.isTemporaryError(err) {
		r.resetRetryCount(bucket)
		return ctrl.Result{}, err
	}

	retryCount := r.getRetryCount(bucket)

	if retryCount >= maxRetryAttempt {
		log.Error(err, "max retry attempts reached",
			"retryCount", retryCount,
			"maxAttempts", maxRetryAttempt,
		)
		r.resetRetryCount(bucket)
		r.setConditionsOnError(bucket, fmt.Errorf("max retry attempts (%d) reached: %v", maxRetryAttempt, err))
		_ = r.Status().Update(ctx, bucket)

		return ctrl.Result{}, fmt.Errorf("max retry attempts reached: %w", err)
	}

	retryCount++
	r.setRetryCount(bucket, retryCount)

	delay := r.calculateRetryDelay(retryCount)

	log.Info("Temporary error detected, will retry",
		"error", err,
		"retryCount", retryCount,
		"maxAttempts", maxRetryAttempt,
		"retryAfter", delay,
	)

	if updateErr := r.Update(ctx, bucket); updateErr != nil {
		log.Error(updateErr, "unable to update retry count")
	}

	return ctrl.Result{RequeueAfter: delay}, nil
}

// getRetryCount gets number of tries from annotations
func (r *BucketReconciler) getRetryCount(bucket *storagev1alpha1.Bucket) int {
	if bucket.Annotations == nil {
		return 0
	}

	countStr, exists := bucket.Annotations[retryAnnotationKey]
	if !exists {
		return 0
	}

	var count int
	if _, err := fmt.Sscanf(countStr, "%d", &count); err != nil {
		return 0
	}

	return count
}

// setRetryCount set number of tries in the annotations
func (r *BucketReconciler) setRetryCount(bucket *storagev1alpha1.Bucket, count int) {
	if bucket.Annotations == nil {
		bucket.Annotations = make(map[string]string)
	}

	bucket.Annotations[retryAnnotationKey] = fmt.Sprintf("%d", count)
}

// resetRetryCount resets retries count
func (r *BucketReconciler) resetRetryCount(bucket *storagev1alpha1.Bucket) {
	if bucket.Annotations == nil {
		return
	}

	delete(bucket.Annotations, retryAnnotationKey)
}

// calculateRetryDelay calculate delay using exp backoff
func (r *BucketReconciler) calculateRetryDelay(attemptCount int) time.Duration {
	delay := baseRetryDelay * time.Duration(1<<uint(attemptCount))

	if delay > maxRetryDelay {
		delay = maxRetryDelay
	}

	return delay
}

// SetupWithManager sets up the controller with the Manager.
func (r *BucketReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&storagev1alpha1.Bucket{}).
		Named("bucket").
		Complete(r)
}
