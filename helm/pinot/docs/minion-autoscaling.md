# Pinot Minion Auto-scaling

This guide explains how to set up auto-scaling for Pinot Minions based on task queue size.

## Overview

Pinot Minions execute various tasks, such as segment merging, reindexing, or purging. As the number of tasks increases, you may need to scale the number of minion instances to handle the load efficiently. This guide demonstrates how to implement auto-scaling for minions based on the number of pending tasks.

## Approaches to Auto-scaling

There are two main approaches to auto-scaling Pinot Minions:

1. **Kubernetes-based auto-scaling**: Using Horizontal Pod Autoscaler (HPA) with custom metrics
2. **Custom auto-scaling**: Implementing your own scaling logic using the Pinot API

## Prerequisites

- Kubernetes cluster with Helm installed
- Prometheus for metrics collection
- Prometheus Adapter for custom metrics in Kubernetes

## Enabling Metrics

The Pinot controller emits metrics about the task queue size that can be used for auto-scaling:

- `pinot_controller_TOTAL_PENDING_MINION_TASKS`: Total number of pending tasks across all task types
- `pinot_controller_PENDING_MINION_TASKS_PER_TYPE`: Number of pending tasks per task type

## Option 1: Kubernetes HPA with Custom Metrics

### Step 1: Deploy Prometheus and Prometheus Adapter

If you don't already have Prometheus and Prometheus Adapter installed, deploy them:

```bash
# Add Prometheus Helm repo
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Install Prometheus
helm install prometheus prometheus-community/prometheus

# Install Prometheus Adapter
helm install prometheus-adapter prometheus-community/prometheus-adapter -f prometheus-adapter-values.yaml
```

Create a `prometheus-adapter-values.yaml` file that includes rules for exposing Pinot task metrics:

```yaml
rules:
  default: false
  custom:
    - seriesQuery: 'pinot_controller_TOTAL_PENDING_MINION_TASKS'
      resources:
        overrides:
          namespace:
            resource: namespace
          pod:
            resource: pod
      name:
        matches: "^pinot_controller_TOTAL_PENDING_MINION_TASKS"
        as: "pinot_pending_tasks"
      metricsQuery: sum(<<.Series>>)
```

### Step 2: Enable Auto-scaling in Pinot Helm Chart

Update your Pinot Helm values to enable auto-scaling for minion stateless:

```yaml
minionStateless:
  enabled: true
  replicaCount: 1
  
  # Enable auto-scaling
  autoscaling:
    enabled: true
    minReplicas: 1
    maxReplicas: 10
    # CPU-based autoscaling (optional)
    cpuTargetPercentage: 80
    # Task queue based autoscaling (recommended)
    taskQueueMetric:
      enabled: true
      metricName: "pinot_pending_tasks"
      # Target value is tasks per minion - e.g., 5 means scale to have ~5 tasks per minion
      targetValue: 5
```

Deploy or upgrade Pinot with these settings:

```bash
helm upgrade --install pinot pinot/pinot -f values.yaml
```

### Step 3: Verify Auto-scaling Works

Generate some tasks and observe the HPA in action:

```bash
# Monitor the HPA
kubectl get hpa -w

# Check scaling decisions
kubectl describe hpa pinot-minion-stateless
```

## Option 2: Custom Auto-scaling with Pinot API

For more complex scaling logic, you can use the Pinot API to implement your own auto-scaling mechanism.

### Step 1: Get Task Counts from Pinot API

The Pinot controller exposes endpoints to get task counts:

```bash
# Get overall task counts
curl http://pinot-controller-external:9000/minions/taskcount

# Get task counts for a specific task type
curl http://pinot-controller-external:9000/minions/taskcount/SegmentGenerationAndPushTask
```

### Step 2: Implement a Custom Scaler

Create a script or service that:

1. Periodically polls the task count API
2. Makes scaling decisions based on the number of tasks
3. Updates the number of minion replicas through the Kubernetes API

Example Python script:

```python
import requests
import kubernetes
from kubernetes import client, config
import time
import math

# Configure Kubernetes client
config.load_kube_config()
apps_v1_api = client.AppsV1Api()

# Pinot controller endpoint
PINOT_CONTROLLER = "http://pinot-controller-external:9000"

# Scaling parameters
MIN_REPLICAS = 1
MAX_REPLICAS = 10
TARGET_TASKS_PER_MINION = 5
NAMESPACE = "default"
DEPLOYMENT_NAME = "pinot-minion-stateless"

while True:
    try:
        # Get task count
        response = requests.get(f"{PINOT_CONTROLLER}/minions/taskcount")
        data = response.json()
        
        total_pending_tasks = data.get("totalPendingTasks", 0)
        current_minions = data.get("totalMinionInstances", 1)
        
        # Calculate desired number of minions
        desired_replicas = math.ceil(total_pending_tasks / TARGET_TASKS_PER_MINION)
        desired_replicas = max(MIN_REPLICAS, min(desired_replicas, MAX_REPLICAS))
        
        # Only scale if change is needed
        if desired_replicas != current_minions:
            print(f"Scaling minions from {current_minions} to {desired_replicas}")
            
            # Update the deployment
            apps_v1_api.patch_namespaced_deployment_scale(
                name=DEPLOYMENT_NAME,
                namespace=NAMESPACE,
                body={"spec": {"replicas": desired_replicas}}
            )
    
    except Exception as e:
        print(f"Error: {e}")
    
    # Wait before checking again
    time.sleep(60)
```

## Tips for Efficient Auto-scaling

1. **Set appropriate scaling thresholds**: Too aggressive scaling can cause thrashing, while too conservative scaling might not provide enough resources.

2. **Consider task-specific scaling**: Different task types may have different resource requirements. You might want to scale minions for specific task types separately.

3. **Add scale-down delay**: To prevent rapid scale-down after task completion, add a stabilization window (implemented in the HPA definition).

4. **Monitor performance**: Keep track of task execution times and adjust scaling parameters as needed.

5. **Resource efficiency**: If you're frequently running tasks, consider keeping a minimum number of minions ready to reduce startup latency.

## Conclusion

Auto-scaling Pinot Minions based on task queue size helps ensure that your system can handle varying workloads efficiently. By using either Kubernetes HPA with custom metrics or a custom scaling solution, you can optimize resource usage while maintaining good performance.