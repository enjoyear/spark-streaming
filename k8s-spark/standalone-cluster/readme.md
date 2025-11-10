
```bash
kubectl create namespace spark-standalone
```

# Driver for Standalone mode
Driver connects to the master via `spark://...`

## Dynamic Allocation
The driver initially requests some executors but can dynamically add/release them as tasks finish or pending tasks grow.

## Dynamic Allocation on Standalone
Requirements:
* The Standalone cluster must have a shuffle service running on each worker.
Driver Configs:
```
spark.dynamicAllocation.enabled=true
spark.shuffle.service.enabled=true
```
* Actual add/remove of executors is managed by the driver via heartbeats with the shuffle service.

## Dynamic Allocation on K8S
Driver Configs:
```
spark.dynamicAllocation.enabled=true

# Must be true if you rely on an external shuffle service to safely remove executors. 
# On K8S this is usually false, since executors are short-lived pods.
spark.shuffle.service.enabled=false

# Enables shuffle tracking (added in Spark 3.2+) — a driver-side mechanism that tracks shuffle completion 
# without requiring an external shuffle service. 
# Needed for dynamic allocation on Kubernetes.
spark.dynamicAllocation.shuffleTracking.enabled=true
```

# Different Master Mode
Standalone Mode: Master = Separate long-running process
* Runs independently as its own JVM process
* Manages cluster resources. Workers register with it

Kubernetes Mode (with Spark Operator): Master = Kubernetes API Server
* NOT a Spark process
* K8s scheduler handles resource allocation
* No separate "Spark Master" at all

Local Mode: Master = Embedded in driver process
* Everything runs in single JVM
* No separate master process
* Used for testing/development

YARN Mode: Master = YARN ResourceManager
* NOT a Spark process
* Spark just talks to existing YARN RM

