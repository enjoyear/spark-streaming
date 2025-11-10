# Spark History Server

```bash
# Create PersistentVolumeClaim first, then start the spark application
kubectl apply -f ./k8s-spark/pvc/spark-events-storage.yaml
kubectl get pv
kubectl get pvc

# Deploy the History Server
# This can be used to deploy the changes for a Deployment. In this case, K8S will roll out a new pod with the updated configuration,
# then terminates the old pod once the new one is ready.
# 1. New ReplicaSet created with updated pod template (hash: 77fbcdcc8b)
# 2. New ReplicaSet scales up to desired replicas (1)
# 3. Old ReplicaSet scales down to 0 (hash: 7ddd8f65f4)
# 4. Old ReplicaSet kept for rollback capability
# 5. After 10(default) updates, oldest ReplicaSet deleted automatically
kubectl apply -f ./k8s-spark/spark-history-server/spark-history-server.yaml
# Check rollout status
kubectl rollout status deployment/spark-history-server
# Some changes (like environment variables) trigger automatic rollout, but others might not. To force a restart:
kubectl rollout restart deployment/spark-history-server

# Kubernetes keeps old ReplicaSets (with 0 replicas) to enable quick rollbacks if the new version has issues.
# By default, Kubernetes keeps the last 10 revisions. This is controlled by `revisionHistoryLimit`
# Show deployment historical rollout versions
kubectl rollout history deployment/spark-history-server
# Rollback to previous version
kubectl rollout undo deployment/spark-history-server # Can be used to `undo an undo`. Without --to-revision always goes to the previous revision.
# Rollback to specific revision
kubectl rollout undo deployment/spark-history-server --to-revision=1


kubectl get pods -l app=spark-history-server

# Check the spark history files
# Log into the pod where the Spark driver run
docker exec -it prod-test-worker2 /bin/bash
ls /tmp/spark-events
# http://localhost:18080/
kubectl port-forward service/spark-history-server 18080:18080

kubectl exec spark-pi-driver -- ls -la /tmp/spark-events/
```

## Understanding the internals
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
