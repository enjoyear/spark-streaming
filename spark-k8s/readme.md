Step 1: Install the Spark Operator
The Spark Operator manages Spark applications as Kubernetes native resources. This is what many companies use in production.
Helm is the package manager for Kubernetes, like apt or brew for Linux/macOS.
* It manages Helm charts, which are pre-configured templates that describe how to deploy applications (pods, services, RBAC, etc.) onto a Kubernetes cluster.
* A chart defines everything needed to run an app — images, configs, volumes, etc.
```bash
brew install helm
kubectl create namespace spark-operator
kubectl get namespaces

helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

# Install the operator
# First spark-operator → Helm release name
# Second spark-operator → Helm repository alias
# Third spark-operator → chart name within that repo
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  # a webhook is just an HTTP callback that the API server calls when certain events occur (like creating or updating a resource).
  --set webhook.enable=true \
  --set image.repository=openlake/spark-operator \
  --set image.tag=3.1.1
  # --set serviceAccounts.spark.name=spark \
```

Deploy and monitor
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


```
kind get clusters
kind delete cluster --name spark-k8s
kind create cluster --name spark-k8s --config kind-spark.yaml


# Each Kind cluster = 1 or more Docker containers (nodes):
docker ps --filter "name=spark-k8s"
docker restart spark-k8s-control-plane
# If you have worker nodes
docker restart spark-k8s-worker
docker restart spark-k8s-worker2

kubectl cluster-info --context kind-spark-k8s
kubectl cluster-info
kubectl get nodes
# Not sure what to do next?
# Check out https://kind.sigs.k8s.io/docs/user/quick-start/
```

2. Create a namespace + service account + RBAC for Spark driver
```
kubectl create ns spark
kubectl -n spark create serviceaccount spark
kubectl create clusterrolebinding spark-rb \
  --clusterrole=edit \
  --serviceaccount=spark:spark
```

3. Pick a multi-arch Spark image
Bitnami’s images are multi-arch and easy on M-series Macs
```
# Pull once locally so kind can load it
docker pull bitnamilegacy/spark:3.5.3
kind load docker-image bitnamilegacy/spark:3.5.3 --name spark-k8s
```
