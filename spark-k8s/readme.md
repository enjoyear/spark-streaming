# Install the Spark Operator
Helm is the package manager for Kubernetes, like apt or brew for Linux/macOS.
* It manages Helm charts, which are pre-configured templates that describe how to deploy applications (pods, services, RBAC, etc.) onto a Kubernetes cluster.
* A chart defines everything needed to run an app — images, configs, volumes, etc.
```bash
brew install helm
helm repo list

kubectl create namespace spark-operator
kubectl get namespaces
```

The Spark Operator manages Spark applications as Kubernetes native resources. 
* [Reference](https://github.com/kubeflow/spark-operator)
```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

# Show all charts in a repo
helm search repo spark-operator

# Install the Deployment and CRDs
# First spark-operator → Helm release name
# Second spark-operator → Helm repository alias
# Third spark-operator → chart name within that repo
helm install spark-operator spark-operator/spark-operator --namespace spark-operator

# To upgrade
helm upgrade [RELEASE_NAME] spark-operator/spark-operator [flags]
# To uninstall
helm uninstall [RELEASE_NAME]
# To list
helm list --all-namespaces

# Show details for a chart
helm show values spark-operator/spark-operator
# Look for image.registry, image.repository, image.tag, 
helm show values spark-operator/spark-operator | grep -A 5 "image:"
# To overwrite/provide parameters during install, see this example below to use [--set param.path=value]
helm install spark-operator spark-operator/spark-operator --namespace spark-operator \
  --set webhook.enable=true

```

How to debug spark-operator failures
```
kubectl get pods -n spark-operator 
kubectl describe pod -n spark-operator spark-operator-controller-7f5557c6cd-knlpn
```

# Deploy a Spark job
```
# Apply the Spark application
kubectl apply -f spark-pi-example.yaml

# Watch the pods being created
kubectl get pods -w

# Check the application status
kubectl get sparkapplications

# Get logs from the driver
kubectl logs spark-pi-driver

# Describe the Spark application
kubectl describe sparkapplication spark-pi
```

# Others
Create a namespace + service account + RBAC for Spark driver
```
kubectl create ns spark
kubectl -n spark create serviceaccount spark
kubectl create clusterrolebinding spark-rb \
  --clusterrole=edit \
  --serviceaccount=spark:spark
```

Pick a multi-arch Spark image
Bitnami’s images are multi-arch and easy on M-series Macs
```
# Pull once locally so kind can load it
docker pull bitnamilegacy/spark:3.5.3
kind load docker-image bitnamilegacy/spark:3.5.3 --name spark-k8s
```
