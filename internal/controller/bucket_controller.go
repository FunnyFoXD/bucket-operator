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

	// Read and fetch "bucket" resource
	var bucket storagev1alpha1.Bucket
	if err := r.Get(ctx, req.NamespacedName, &bucket); err != nil {
		if !errors.IsNotFound(err) {
			log.Error(err, "unable to fetch Bucket")
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Reconcile in progress
	meta.SetStatusCondition(&bucket.Status.Conditions, metav1.Condition{
		Type:    "Progressing",
		Status:  metav1.ConditionTrue,
		Reason:  "Reconciling",
		Message: "Reconciling bucket resource",
	})

	log.Info("Reconcile is triggered", "size", bucket.Spec.Size)

	// Finalizer init and logic
	finalizer := "bucket-controller.storage.mydomain.com/finalizer"

	if !bucket.DeletionTimestamp.IsZero() {
		log.Info("Bucket deletion in progress...")
		if controllerutil.ContainsFinalizer(&bucket, finalizer) {
			log.Info("Removing finalizer...")

			// Delete ConfigMap that we created
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      bucket.Name + "-config",
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
				log.Info("ConfigMap deleted successfully")
			}

			controllerutil.RemoveFinalizer(&bucket, finalizer)

			if err := r.Update(ctx, &bucket); err != nil {
				log.Error(err, "couldn't remove finalizer")
				return ctrl.Result{}, err
			}
			log.Info("Finilizer is removed")
		}

		log.Info("Bucket is deleted")
		return ctrl.Result{}, nil
	} else {
		if !controllerutil.ContainsFinalizer(&bucket, finalizer) {
			log.Info("Adding finalizer...")
			controllerutil.AddFinalizer(&bucket, finalizer)

			if err := r.Update(ctx, &bucket); err != nil {
				log.Error(err, "couldn't add finalizer")
				return ctrl.Result{}, err
			}

			log.Info("Finalizer is added")
		}
	}

	// Initial ConfigMap
	log.Info("Creating ConfigMap...")
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      bucket.Name + "-config",
			Namespace: bucket.Namespace,
		},
	}

	// Creating or updating bucket ConfigMap
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = map[string]string{
			"app":         "bucket-operator",
			"bucket-name": bucket.Name,
		}
		cm.Data = map[string]string{
			"size":  fmt.Sprintf("%d", bucket.Spec.Size),
			"owner": bucket.Name,
		}

		if bucket.Spec.Name != "" {
			cm.Data["name"] = bucket.Spec.Name
		}
		if bucket.Spec.Region != "" {
			cm.Data["region"] = bucket.Spec.Region
		}
		if bucket.Spec.StorageType != "" {
			cm.Data["storageType"] = bucket.Spec.StorageType
		}

		return ctrl.SetControllerReference(&bucket, cm, r.Scheme)
	})
	if err != nil {
		log.Error(err, "unable to create/update ConfigMap")

		// Set conditions to false
		meta.SetStatusCondition(&bucket.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "ConfigMapCreationFailed",
			Message: fmt.Sprintf("Failed to create ConfigMap: %v", err),
		})

		meta.SetStatusCondition(&bucket.Status.Conditions, metav1.Condition{
			Type:    "Available",
			Status:  metav1.ConditionFalse,
			Reason:  "ConfigMapCreationFailed",
			Message: fmt.Sprintf("Failed to create ConfigMap: %v", err),
		})

		meta.SetStatusCondition(&bucket.Status.Conditions, metav1.Condition{
			Type:    "Progressing",
			Status:  metav1.ConditionFalse,
			Reason:  "ReconcileFalse",
			Message: fmt.Sprintf("Reconcile failed: %v", err),
		})

		// Update status
		_ = r.Status().Update(ctx, &bucket)

		return ctrl.Result{}, err
	}

	meta.SetStatusCondition(&bucket.Status.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  metav1.ConditionTrue,
		Reason:  "ConfigMapCreated",
		Message: "ConfigMap successfully created",
	})

	meta.SetStatusCondition(&bucket.Status.Conditions, metav1.Condition{
		Type:    "Available",
		Status:  metav1.ConditionTrue,
		Reason:  "BucketAvailable",
		Message: "Bucket is available and ready",
	})

	meta.SetStatusCondition(&bucket.Status.Conditions, metav1.Condition{
		Type:    "Progressing",
		Status:  metav1.ConditionFalse,
		Reason:  "ReconcileComplete",
		Message: "Reconciliation completed successfully",
	})

	if err := r.Status().Update(ctx, &bucket); err != nil {
		if errors.IsConflict(err) {
			log.Info("Status conflict, retry...", "error", err)
			return ctrl.Result{Requeue: true}, nil
		}

		log.Error(err, "unable to update status")
		return ctrl.Result{}, err
	}
	log.Info("Status updated with Conditions")

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *BucketReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&storagev1alpha1.Bucket{}).
		Named("bucket").
		Complete(r)
}
