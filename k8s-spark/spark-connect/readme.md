Benefits of all-purpose clusters through Spark Connect
* Dynamic/Warm executors
* Cached data across many interactive commands/notebooks (talking to the same driver)

# Setup
Create configmap `spark-connect-default-conf`
```bash
kubectl -n spark-operator create configmap spark-connect-default-conf --from-file=./k8s-spark/spark-connect/spark-connect-defaults.conf

# to update it later, you need to delete it first
kubectl -n spark-operator delete configmap spark-connect-default-conf
```

# Usage
## Connect to the Spark Connect
* To access the `spark-connect-svc` locally, run `kubectl port-forward -n spark-operator svc/spark-connect-svc 15002:15002`
* To access the `spark-connect-svc` within K8S
```python
spark = SparkSession.builder \
    .remote("sc://spark-connect-svc.spark-operator.svc.cluster.local:15002") \
    .appName("JupyterNotebook") \
    .getOrCreate()
```

## Access the Web UI
Run `kubectl port-forward -n spark-operator deployment/spark-connect 4040:4040` or 
`kubectl port-forward -n spark-operator svc/spark-connect-svc 4040:4040` (if exposed through service)






