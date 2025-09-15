package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type Operation interface {
	GetId() string
	Encode() []byte
	GetProperties() map[string][]string
	// Execute executes an operation, executor can pass configuration
	Execute([]byte, map[string]interface{}) ([]byte, error)
}

type K8sJobOperation struct {
	Id         string
	Properties map[string][]string
	JobSpec    []byte // Serialized k8s Job spec (YAML or JSON)
}

func (ops *K8sJobOperation) GetId() string {
	return ops.Id
}

func (ops *K8sJobOperation) Encode() []byte {
	return ops.JobSpec
}

func (ops *K8sJobOperation) GetProperties() map[string][]string {
	return ops.Properties
}

// Execute would submit the job to Kubernetes. Here, just a stub for integration.
func (ops *K8sJobOperation) Execute(data []byte, option map[string]interface{}) ([]byte, error) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		return nil, fmt.Errorf("KUBECONFIG environment variable not set")
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %v", err)
	}

	var job batchv1.Job
	if err := json.Unmarshal(ops.JobSpec, &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job spec: %v", err)
	}

	namespace := "default"
	if ns, ok := option["namespace"].(string); ok && ns != "" {
		namespace = ns
	}

	createdJob, err := clientset.BatchV1().Jobs(namespace).Create(context.TODO(), &job, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to submit k8s job: %v", err)
	}
	result, err := json.Marshal(createdJob)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal job result: %v", err)
	}
	return result, nil
}

type BlankOperation struct {
}

func (ops *BlankOperation) GetId() string {
	return "end"
}

func (ops *BlankOperation) Encode() []byte {
	return []byte("")
}

func (ops *BlankOperation) GetProperties() map[string][]string {
	return make(map[string][]string)
}

func (ops *BlankOperation) Execute(data []byte, option map[string]interface{}) ([]byte, error) {
	return data, nil
}
 