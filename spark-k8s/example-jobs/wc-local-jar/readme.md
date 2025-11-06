sparkoperator.k8s.io/v1beta2 [API Doc](https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md)

Found the Spark application jar at spark-streaming/word-count/build/libs/word-count.jar
```bash
./gradlew clean :word-count:build

# Trigger the application
kubectl apply -f ./spark-k8s/example-jobs/wc-local-jar/word-count-local-jar.yaml
```

# Upload the jar
To mount the local JAR into the pods. There are a few options:
## Option A: Use a ConfigMap (for smaller JARs < 1MB)
- ConfigMap is stored in etcd/cluster

### Overall Flow
1. Your machine: JAR file exists locally
2. ConfigMap creation: kubectl reads the file and stores it in the cluster (in etcd)
- kubectl base64-encodes the binary content of the jar file, then stores it in the ConfigMap with the key `word-count.jar`
3. Pod creation: Kubernetes mounts the ConfigMap data into the pod's filesystem
4. Inside container: The file appears at mountPath location
5. Spark reads: Spark accesses the JAR from the container's filesystem

```bash
# option --from-file=[]:
# 	Key file can be specified using its file path, in which case file basename will be used as configmap key, or
#	optionally with a key and file path, in which case the given key will be used.  Specifying a directory will
#	iterate each named file in the directory whose basename is a valid configmap key.
kubectl create configmap word-count-jar --from-file=word-count.jar=/Users/chenguo/src/chen/spark-streaming/word-count/build/libs/word-count.jar
# check KVPs in the configmap
kubectl describe configmap word-count-jar
```

To update the jar
1. delete then create a new one
2. Use kubectl replace (One Command)
`-f -` a single dash at the end means read from standard input (stdin). So, it reads the YAML that came through the pipe.
3. Versioned ConfigMaps
```bash
# Option 1
kubectl delete configmap word-count-jar

# Option 2
kubectl create configmap word-count-jar --from-file=word-count.jar=/Users/chenguo/src/chen/spark-streaming/word-count/build/libs/word-count.jar --dry-run=client -o yaml | kubectl replace -f -

# Option 3
kubectl create configmap word-count-jar-v1 --from-file=word-count.jar=/Users/chenguo/...
```


## Option B: Use a PersistentVolume (recommended for larger JARs)
Create a PV/PVC and copy your JAR there, then mount it similar to how you're mounting spark-events.

## Option C: Build a custom Docker image (most production-ready)
