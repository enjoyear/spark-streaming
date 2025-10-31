Preparation
```bash
brew install kind kubectl
```

1. Create a kind cluster
[Reference](https://kind.sigs.k8s.io/docs/user/quick-start/)
```bash
kind create cluster --config ./k8s/kind-config.yaml --name prod-test

# list all clusters
kind get clusters
# delete a cluster
kind delete cluster --name prod-test

# Get cluster info
kubectl cluster-info --context kind-prod-test
```

2. Verify
```bash
docker ps --filter "name=prod-test"
# To manually introduce some interruptions
docker restart prod-test-worker

# Context is used to connect to different K8S clusters
kubectl config current-context
kubectl config get-contexts

# Listing All API Resources (Including CRD: Custom Resources Definition)
kubectl api-resources

# Listing Resources of a Specific Type
# kubectl get <resource-type> [--all-namespaces]
kubectl get nodes
kubectl get pods
kubectl get deployments
kubectl get services
# Listing All Common Resource Types
kubectl get all --all-namespaces

# Getting Detailed Information about a Resource:
# kubectl describe <resource-type>/<resource-name> -n <namespace>
kubectl describe pod/my-app-pod-1234

# Monitoring Resource Usage
kubectl top pods
kubectl top nodes

# Log into a pod
kubectl exec -n kubernetes-dashboard -it dashboard-metrics-scraper-5ffb7d645f-ttvb9 -- /bin/bash
# See the running process
kubectl get pod -n kubernetes-dashboard dashboard-metrics-scraper-5ffb7d645f-ttvb9 -o jsonpath='{.spec.containers[*].image}'
# Check what command it's running
kubectl get pod -n kubernetes-dashboard dashboard-metrics-scraper-5ffb7d645f-ttvb9 -o jsonpath='{.spec.containers[*].command}'
# Get the args
kubectl get pod -n kubernetes-dashboard dashboard-metrics-scraper-5ffb7d645f-ttvb9 -o jsonpath='{.spec.containers[*].args}'
```

3. Install K8S Dashboard
```bash
# `kubectl apply` declares the desired state, and let K8S figure out how to achieve it 
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml
```
