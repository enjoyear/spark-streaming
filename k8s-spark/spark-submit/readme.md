# Role Setup
The `spark-operator-spark` role is created by the K8S Spark Operator.
This RoleBinding is managed by Helm, a future helm upgrade may overwrite it.
To retrieve its specification
```bash
kubectl get rolebinding spark-operator-spark -n default -o yaml
```

## Option 1: Patch the role binding
```bash
# `-` is the JSON Patch operation to indicate array appending
kubectl patch rolebinding spark-operator-spark \
  -n default \
  --type='json' \
  -p='[
    {
      "op": "add",
      "path": "/subjects/-",
      "value": {
        "kind": "ServiceAccount",
        "name": "chen-guo",
        "namespace": "default"
      }
    }
  ]'
```
## Option 2: Edit interactively
```bash
kubectl edit rolebinding spark-operator-spark -n default
```

# Trigger through `spark-submit`

1. Run the driver as chen-guo, which has enough position to manage pods
- Optionally add `--conf spark.kubernetes.authenticate.executor.serviceAccountName=chen-guo` to be consistent
2. The `local://` prefix tells Spark the file is already present in the container image.
- What about `file://`? 
```bash
k exec -it spark-submit -- /bin/bash

/opt/spark/bin/spark-submit \
    --master k8s://https://kubernetes.default.svc \
    --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=docker.io/library/spark:4.0.0 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=chen-guo \
    local:///opt/spark/examples/jars/spark-examples_2.13-4.0.0.jar \
    1000
```
