Preparation
```bash
brew install kind kubectl
```

1. Create a kind cluster
```bash
kind create cluster --config ./k8s/kind-config.yaml --name prod-test

# Get cluster info
kubectl cluster-info --context kind-prod-test
```

2. Verify
```bash
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
```

3. Install K8S Dashboard
```bash
# `kubectl apply` declares the desired state, and let K8S figure out how to achieve it 
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml
```


