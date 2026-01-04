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
)

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
	// Initilize logger
	log := logf.FromContext(ctx).WithValues("bucket", req.NamespacedName)

	// Read and fetch bucket resource
	var bucket storagev1alpha1.Bucket
	if err := r.Get(ctx, req.NamespacedName, &bucket); err != nil {
		if !errors.IsNotFound(err) {
			log.Error(err, "unable to fetch Bucket")
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Reconcile in progress mark
	r.setProgressingCondition(&bucket, metav1.ConditionTrue, reasonReconciling, "Reconciling bucket resource")

	log.Info("Reconcile is triggered")

	if !bucket.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, &bucket, log)
	}

	if err := r.ensureFinalizer(ctx, &bucket, log); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileConfigMap(ctx, &bucket, log); err != nil {
		r.setConditionsOnError(&bucket, err)
		_ = r.Status().Update(ctx, &bucket)
		return ctrl.Result{}, err
	}

	r.setConitionsOnSuccess(&bucket)

	if err := r.updateStatus(ctx, &bucket, log); err != nil {
		return ctrl.Result{}, nil
	}

	log.Info("Reconciliation completed successfully")

	return ctrl.Result{}, nil
}

// handleDeletion process deletion bucket resource
func (r *BucketReconciler) handleDeletion(ctx context.Context, bucket *storagev1alpha1.Bucket, log logr.Logger) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(bucket, finalizer) {
		return ctrl.Result{}, nil
	}

	log.Info("Bucket deletion in progress...")

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bucket.Name + configMapSuffix,
			Namespace: bucket.Namespace,
		},
	}

	if err := r.Delete(ctx, cm); err != nil {
		if !errors.IsNotFound(err) {
			log.Error(err, "unable to delete ConfigMap")
			return ctrl.Result{}, err
		}

		log.Info("ConfigMap already deleted")
	} else {
		log.Info("Config deleted successfully")
	}

	controllerutil.RemoveFinalizer(bucket, finalizer)
	if err := r.Update(ctx, bucket); err != nil {
		log.Error(err, "couldn't remove finalizer")
		return ctrl.Result{}, err
	}

	log.Info("Finalizer is removed")
	log.Info("Bucket is deleted successfully")
	return ctrl.Result{}, nil
}

// ensureFinalizer add finalizer if it os not in resource
func (r *BucketReconciler) ensureFinalizer(ctx context.Context, bucket *storagev1alpha1.Bucket, log logr.Logger) error {
	if controllerutil.ContainsFinalizer(bucket, finalizer) {
		return nil
	}

	log.Info("Adding finalizer...")
	controllerutil.AddFinalizer(bucket, finalizer)

	if err := r.Update(ctx, bucket); err != nil {
		log.Error(err, "couldn't add finalizer")
		return err
	}

	return nil
}

// reconcileConfigMap create or update ConfigMap
func (r *BucketReconciler) reconcileConfigMap(ctx context.Context, bucket *storagev1alpha1.Bucket, log logr.Logger) error {
	log.Info("Creating ConfigMap...")

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
		log.Error(err, "unable to create/update ConfigMap")
		return err
	}

	log.Info("ConfigMap created/updated successfully")

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
	r.setReadyCondition(bucket, metav1.ConditionFalse, reasonConfigMapCreationFailed,
		fmt.Sprintf("Failed to create ConfigMap: %v", err))
	r.setProgressingCondition(bucket, metav1.ConditionFalse, reasonConfigMapCreationFailed,
		fmt.Sprintf("Failed to create ConfigMap: %v", err))
	r.setAvailableCondition(bucket, metav1.ConditionFalse, reasonReconcileFailed,
		fmt.Sprintf("Reconcile failed: %v", err))
}

// setConditionsOnSuccess set all condition types for success
func (r *BucketReconciler) setConitionsOnSuccess(bucket *storagev1alpha1.Bucket) {
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
			log.Info("Status conflict, retry...", "error", err)
			return err
		}
		log.Error(err, "unable to update status")
		return err
	}

	log.Info("Status update with Conditions successfully")

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *BucketReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&storagev1alpha1.Bucket{}).
		Named("bucket").
		Complete(r)
}
