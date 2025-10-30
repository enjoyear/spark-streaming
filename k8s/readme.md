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
kubectl get nodes
# context is used to connect to different K8S clusters
kubectl config current-context
kubectl config get-contexts
```

