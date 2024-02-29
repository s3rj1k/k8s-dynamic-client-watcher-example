package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/s3rj1k/go-randset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

type ResourceEvent struct {
	types.NamespacedName
	schema.GroupVersionResource
}

type ResourceWatcher struct {
	Client dynamic.Interface
	Events *randset.RandomizedSet[ResourceEvent]
}

func NewResourceWatcher(dynamicClient dynamic.Interface) *ResourceWatcher {
	return &ResourceWatcher{
		Client: dynamicClient,
		Events: randset.NewWithInitialSize[ResourceEvent](1024),
	}
}

func (rw *ResourceWatcher) WatchResource(ctx context.Context, gvr schema.GroupVersionResource, namespace string, opts metav1.ListOptions) error {
	watcher, err := rw.Client.Resource(gvr).Namespace(namespace).Watch(ctx, opts)
	if err != nil {
		return err
	}

	go func() {
		defer watcher.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-watcher.ResultChan():
				if !ok {
					return
				}

				var uObj *unstructured.Unstructured

				uObj, ok = event.Object.(*unstructured.Unstructured)
				if !ok {
					continue
				}

				rw.Events.Add(ResourceEvent{
					NamespacedName: types.NamespacedName{
						Namespace: uObj.GetNamespace(),
						Name:      uObj.GetName(),
					},
					GroupVersionResource: gvr,
				})
			}
		}
	}()

	return nil
}

func (rw *ResourceWatcher) Run(ctx context.Context, interval time.Duration, processEvent func(ctx context.Context, client dynamic.Interface, event ResourceEvent)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	f := func(ctx context.Context, rw *ResourceWatcher) {
		for !rw.Events.IsEmpty() {
			event, ok := rw.Events.LoadAndDelete()
			if !ok {
				break
			}

			processEvent(ctx, rw.Client, event)
		}
	}

	for {
		select {
		case <-ctx.Done():
			f(ctx, rw)

			return
		case <-ticker.C:
			f(ctx, rw)
		}
	}
}

func main() {
	config, err := clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	if err != nil {
		log.Fatalf("Error building kubeconfig: %s", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error building dynamic client: %s", err)
	}

	resourceWatcher := NewResourceWatcher(dynamicClient)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	err = resourceWatcher.WatchResource(ctx, schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, "", metav1.ListOptions{})
	if err != nil {
		log.Fatalf("Error starting resourse watcher: %s", err)
	}

	err = resourceWatcher.WatchResource(ctx, schema.GroupVersionResource{Version: "v1", Resource: "secrets"}, "", metav1.ListOptions{})
	if err != nil {
		log.Fatalf("Error starting resourse watcher: %s", err)
	}

	resourceWatcher.Run(ctx, 1*time.Minute, func(ctx context.Context, client dynamic.Interface, event ResourceEvent) {
		backoff := wait.Backoff{
			Steps:    5,
			Duration: 1 * time.Second,
			Factor:   2.0,
			Jitter:   0.1,
		}

		retriable := func(err error) bool {
			return errors.IsServerTimeout(err) || errors.IsTooManyRequests(err) || errors.IsInternalError(err)
		}

		operation := func() error {
			resource, err := client.Resource(event.GroupVersionResource).Namespace(event.Namespace).Get(ctx, event.Name, metav1.GetOptions{})

			if errors.IsNotFound(err) {
				log.Printf("Resource %s/%s not found.\n", event.Namespace, event.Name)
				return nil
			} else if err != nil {
				return err
			}

			if resource.GetDeletionTimestamp() != nil {
				log.Printf("Resource %s/%s is being deleted at %s\n", event.Namespace, event.Name, resource.GetDeletionTimestamp())
			} else {
				log.Printf("Successfully processed %s: %s/%s\n", resource.GetKind(), resource.GetNamespace(), resource.GetName())
			}

			return nil
		}

		err := retry.OnError(backoff, retriable, operation)
		if err != nil {
			log.Printf("After retries, failed to process event for %s/%s: %v\n", event.Namespace, event.Name, err)
		}
	})
}
