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

1. Run the driver as chen-guo, which has enough permissions to manage pods
- Optionally add `--conf spark.kubernetes.authenticate.executor.serviceAccountName=chen-guo` to be consistent
2. The `local://` prefix tells Spark the file is already present in the container image (don’t upload)
- `file://` means a local file that must be uploaded to somewhere shared. Spark tries to upload it to a staging location (spark.kubernetes.file.upload.path)
  - [doc](https://spark.apache.org/docs/latest/running-on-kubernetes.html#using-kubernetes-volumes)
```bash
k exec -it spark-submit -- /bin/bash

# [cluster mode] trigger a jar available in the image
/opt/spark/bin/spark-submit \
    --master k8s://https://kubernetes.default.svc \
    --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.kubernetes.namespace=default \
    --conf spark.executor.instances=2 \
    --conf spark.kubernetes.container.image=docker.io/library/spark:4.0.0 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=chen-guo \
    local:///opt/spark/examples/jars/spark-examples_2.13-4.0.0.jar \
    1000

# [client mode] trigger a jar available in the image
# Need extra configuration to allow executors to reach driver. In the log, you can find
# INFO Utils: Successfully started service 'sparkDriver' on port 7078.
# INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 7079.
export SPARK_DRIVER_POD_IP=$(hostname -i)
/opt/spark/bin/spark-submit \
    --master k8s://https://kubernetes.default.svc \
    --deploy-mode client \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.kubernetes.namespace=default \
    --conf spark.executor.instances=2 \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.driver.host=${SPARK_DRIVER_POD_IP} \
    --conf spark.driver.port=7078 \
    --conf spark.blockManager.port=7079 \
    --conf spark.kubernetes.container.image=docker.io/library/spark:4.0.0 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=chen-guo \
    local:///opt/spark/examples/jars/spark-examples_2.13-4.0.0.jar \
    1000

# [cluster mode] trigger with a customized jar
# Option 1: Make the jar available inside the driver container (via image or PVC)
/opt/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc \
  --deploy-mode cluster \
  --name word-count-emr \
  --class com.chen.guo.WordCountEMR \
  --conf spark.kubernetes.namespace=default \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=50 \
  --conf spark.dynamicAllocation.initialExecutors=2 \
  --conf spark.kubernetes.container.image=docker.io/library/spark:4.0.0 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=chen-guo \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.custom-jars.options.claimName=custom-jars-pvc2 \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.custom-jars.mount.path=/custom-jars \
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.custom-jars.mount.readOnly=true \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.custom-jars.options.claimName=custom-jars-pvc2 \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.custom-jars.mount.path=/custom-jars \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.custom-jars.mount.readOnly=true \
  local:///custom-jars/word-count.jar \
  p1 p2


# Option 2: Ask Spark to copy the jar
# 1. In the submit pod's file system, 
# copy `file:///custom-jars/word-count.jar` into `s3://bucket/spark-upload/<spark-upload-UUID>/word-count.jar` and refer to it from the driver.
# 2. Then pass the staged URI `s3://bucket/spark-upload/<spark-upload-UUID>/word-count.jar` to the driver pod to `driver pod`
# 3. In the driver pod, SparkSubmit tries to "download" it
# copy `s3://bucket/spark-upload/<spark-upload-UUID>/word-count.jar` → `/opt/spark/work-dir/word-count.jar`
/opt/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc \
  --deploy-mode cluster \
  --name word-count-emr \
  --class com.chen.guo.WordCountEMR \
  --conf spark.kubernetes.namespace=default \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=50 \
  --conf spark.dynamicAllocation.initialExecutors=2 \
  --conf spark.kubernetes.container.image=docker.io/library/spark:4.0.0 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=chen-guo \
  --conf spark.kubernetes.file.upload.path=s3://bucket/spark-upload \
  file:///custom-jars/word-count.jar \
  p1 p2
```

## `spark-submit` from local
`brew install apache-spark`
Or download the tar file, then set $SPARK_HOME
