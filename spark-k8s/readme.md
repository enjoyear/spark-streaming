# Install the Spark Operator
Helm is the package manager for Kubernetes, like apt or brew for Linux/macOS.
* It manages Helm charts, which are pre-configured templates that describe how to deploy applications (pods, services, RBAC, etc.) onto a Kubernetes cluster.
* A chart defines everything needed to run an app — images, configs, volumes, etc.
** [version (Chart Version)](https://github.com/kubeflow/spark-operator/blob/07e2981442d1ba793f6dae77a9443ea578c84a09/charts/spark-operator-chart/Chart.yaml#L23) is the version of Helm chart itself (the packaging/deployment code). This tracks changes to the chart's templates, configurations, and structure. 
** [appVersion (Application Version)](https://github.com/kubeflow/spark-operator/blob/07e2981442d1ba793f6dae77a9443ea578c84a09/charts/spark-operator-chart/Chart.yaml#L25) is the version of actual application being deployed (the Spark Operator, the go binary/container that manages Spark applications in Kubernetes). Indicates which version of the Spark Operator software is running. The format could be `<API version (SparkApplication CRD version)>-<Spark Operator version>-<Apache Spark version it supports>`, e.g. `v1beta2-1.6.2-3.5.0`

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

# Inspect What Was Actually Deployed
# Get the full manifest of what's currently deployed
helm get values spark-operator -n spark-operator --all
# Or see the complete computed values (defaults + your overrides)
helm get values spark-operator -n spark-operator
```

Check chart files
* https://github.com/kubeflow/spark-operator/tree/master/charts/spark-operator-chart
Or they can be downloaded locally through `helm pull spark-operator/spark-operator --untar`

Debug spark-operator installment failures
```
kubectl get pods -n spark-operator 
kubectl describe pod -n spark-operator spark-operator-controller-7f5557c6cd-knlpn
```

# Deploy a Spark job
```bash
kubectl apply -f https://raw.githubusercontent.com/kubeflow/spark-operator/refs/heads/master/examples/spark-pi.yaml
kubectl apply -f ./spark-k8s/example-jobs/spark-pi-example.yaml

# Watch the pods being created
kubectl get pods -w
```

## Check the Spark job
```bash
# List the Spark applications
# The Spark Operator only (re)submits on create or when the spec changes; an identical spec won’t trigger another run!
# You need to delete the same application beforehand `kubectl delete sparkapplication <application_name>`
kubectl get sparkapplications
# Describe the Spark application to show what YAML file was submitted
kubectl describe/delete sparkapplication <application_name>
```

The Spark Operator automatically creates a Kubernetes Service(`<name>-ui-svc`) for the Spark UI to provide:
- [Doc](https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md#sparkuiconfiguration)
- Stable endpoint: Pod IPs are ephemeral and change if the pod restarts, but the Service ClusterIP remains stable
- DNS resolution: The service gets a DNS name (spark-pi-ui-svc.default.svc.cluster.local)
- Load balancing: If there were multiple driver pods (though unusual), the service could distribute traffic
```
# Information can also be found through `kubectl get/describe service spark-pi-ui-svc`

Web UI Address:       10.96.236.122:4040    # This is the ClusterIP, only accessible within the cluster. That's why you need to port-forward below.
                                            # The service below will still be available when the driver is still running
Web UI Port:          4040                  
Web UI Service Name:  spark-pi-ui-svc
```
The internal routing is: `Browser (port-forward)` → `Service (10.96.236.122:4040)` → `Driver Pod (10.244.2.7:4040)`
- Pod IP can be found through `kubectl get pod spark-pi-driver -o wide`
- To see node IPs `kubectl get nodes -o wide`
```bash
# Access the UI at http://localhost:4040 through port forwarding (LOCAL_PORT:REMOTE_PORT)
# REMOTE_PORT: The port number that the spark-pi-ui-svc (and the underlying application in the pod) is listening on inside the Kubernetes cluster.
kubectl port-forward service/spark-pi-ui-svc 4040:4040
```









```bash
kubectl get pods --field-selector=status.phase=Succeeded
# Get logs from the driver
kubectl logs <pod_name>

# 
kubectl delete pods --field-selector=status.phase=Succeeded -l spark-role=driver
```

# Others
Create a namespace + service account + RBAC for Spark driver
```bash
# Check images cached on each Node
kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{range .status.images[*]}:{.names}{"\n"}{end}{end}'

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
