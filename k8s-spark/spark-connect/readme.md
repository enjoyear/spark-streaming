Benefits:
* Warm executors
* Cached data across many interactive commands/notebooks (talking to the same driver)

Create configmap `spark-connect-default-conf`
```bash
kubectl -n spark-operator create configmap spark-connect-default-conf --from-file=./k8s-spark/spark-connect/spark-connect-defaults.conf

# to update it later, you need to delete it first
kubectl -n spark-operator delete configmap spark-connect-default-conf
```


Clients then attach to `spark-connect-svc:15002` and share that driver.
To access the `spark-connect-svc` locally, run `kubectl -n spark-operator port-forward svc/spark-connect-svc 15002:15002`





