# Preparation
Helm is the package manager for Kubernetes, like apt or brew for Linux/macOS.
* It manages Helm charts, which are pre-configured templates that describe how to deploy applications (pods, services, RBAC, etc.) onto a Kubernetes cluster.
* A chart defines everything needed to run an app — images, configs, volumes, etc.
  - [version (Chart Version)](https://github.com/kubeflow/spark-operator/blob/07e2981442d1ba793f6dae77a9443ea578c84a09/charts/spark-operator-chart/Chart.yaml#L23) is the version of Helm chart itself (the packaging/deployment code). This tracks changes to the chart's templates, configurations, and structure. 
  - [appVersion (Application Version)](https://github.com/kubeflow/spark-operator/blob/07e2981442d1ba793f6dae77a9443ea578c84a09/charts/spark-operator-chart/Chart.yaml#L25) is the version of actual application being deployed (the Spark Operator, the go binary/container that manages Spark applications in Kubernetes). Indicates which version of the Spark Operator software is running. The format could be `<API version (SparkApplication CRD version)>-<Spark Operator version>-<Apache Spark version it supports>`, e.g. `v1beta2-1.6.2-3.5.0`

```bash
brew install helm
helm repo list

kubectl create namespace spark-operator
kubectl get namespaces
```

# Spark Operator
[Reference](https://github.com/kubeflow/spark-operator)

The Spark Operator manages Spark applications as Kubernetes native resources.
The `spark-operator-spark` service account will also be created by the Spark Operator installation and has the necessary RBAC roles to:
* Create and manage executor pods
* Access pod information
* Create services for the UI

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

## Check Chart Files
* https://github.com/kubeflow/spark-operator/tree/master/charts/spark-operator-chart
Or they can be downloaded locally through `helm pull spark-operator/spark-operator --untar`

## Check Role Permission
RBAC based ACL management.
Minimal permissions a Spark driver needs:
```yaml
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch", "create", "delete"]
- apiGroups: [""]
  resources: ["services"]
  verbs: ["get", "create", "delete"]
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["get", "create", "delete"]
```
```bash
# Find RoleBindings in the current namespace
# The RoleBindings shows the linkages between `roleRef` and `subjects`(service accounts, etc)
kubectl get rolebinding -o json
kubectl describe rolebinding spark-operator-spark
# Find ClusterRoleBindings (cluster-wide permissions)
kubectl get clusterrolebinding -o json

# To find the ROLE bound to a SERVICE ACCOUNT
# kubectl get rolebindings --namespace=<namespace> -o json | jq '.items[] | select(.subjects[]? | .name == "<service-account-name>") | .roleRef.name'
# kubectl get clusterrolebindings -o json | jq '.items[] | select(.subjects[]? | .name == "<service-account-name>" and .subjects[]?.namespace == "<namespace>") | .roleRef.name'
kubectl get rolebindings --namespace=default -o json | jq '.items[] | select(.subjects[]? | .name == "spark-operator-spark") | .roleRef.name'


# Check the permissions assigned to a role
kubectl get role spark-operator-spark -o json # within the same specific namespace
kubectl auth can-i create pods --as=system:serviceaccount:default:spark-operator-spark
# For a ClusterRole
kubectl get clusterrole <role-name> -o yaml  # can be accessed across different namespaces in the cluster
```

# Submit a Spark job
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
# This only works when the driver pod is alive
kubectl port-forward service/spark-pi-ui-svc 4040:4040
```

## Spark History Server
```bash
# Create PersistentVolumeClaim
kubectl apply -f ./spark-k8s/pvc/spark-events-storage.yaml
kubectl get pv
kubectl get pvc

# Check the files
docker exec -it prod-test-worker /bin/bash

kubectl exec spark-pi-driver -- ls -la /tmp/spark-events/
```

### Understanding the internals
Dynamic vs Static Provisioning:
* storageClassName: `standard` tells K8s to use the dynamic provisioner (kind's local-path-provisioner)
* Dynamic provisioner creates PVs automatically when you create a PVC
* The manually created PV has no storageClassName, so it's for static provisioning

Binding Logic:
* K8s only binds PV and PVC if their storageClassNames match exactly
* Setting both to "" or the same string tells K8s: "use static/manual binding, not dynamic"

WaitForFirstConsumer:
* The "waiting for first consumer" message is from the dynamic provisioner
* It's waiting for a pod to use the PVC before creating the PV




## Debug the failure
```bash
kubectl logs spark-pi-driver
kubectl describe sparkapplication spark-pi

kubectl get pods --field-selector=status.phase=Succeeded
kubectl delete pods --field-selector=status.phase=Succeeded -l spark-role=driver
```

# Others
Create a namespace + service account + RBAC for Spark driver
```bash
# Check images cached on each Node
kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{range .status.images[*]}:{.names}{"\n"}{end}{end}'
```
Pick a multi-arch Spark image
Bitnami’s images are multi-arch and easy on M-series Macs
```bash
# Pull once locally so kind can load it
docker pull bitnamilegacy/spark:3.5.3
kind load docker-image bitnamilegacy/spark:3.5.3 --name spark-k8s
```
