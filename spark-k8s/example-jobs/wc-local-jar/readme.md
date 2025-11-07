sparkoperator.k8s.io/v1beta2 [API Doc](https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md)

Found the Spark application jar at spark-streaming/word-count/build/libs/word-count.jar
```bash
./gradlew clean :word-count:build
```

# Upload the jar
To mount the local JAR into the pods. There are a few options:
## Option A: Use a ConfigMap (for smaller JARs < 1MB)
- ConfigMap is stored in etcd/cluster
```bash
kubectl apply -f ./spark-k8s/example-jobs/wc-local-jar/word-count-configmap.yaml
```

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
Build jar into Docker image, and trigger job with the image
```bash
kubectl apply -f ./spark-k8s/example-jobs/wc-local-jar/word-count-dockerimage.yaml
```

```bash
# build the image with a tag
# the last `.` indicates the context path, which controls what files are visible to the Dockerfile during build (e.g. what COPY/ADD can see)
docker build -t spark-wordcount:v1 -f spark-k8s/example-jobs/wc-local-jar/Dockerfile .

# Verify the image was created
docker images | grep spark-wordcount
docker images --digests
docker image inspect spark-wordcount:v1

# Load the Image into kind
# --name part is for kind cluster name (`kind get clusters`)
kind load docker-image spark-wordcount:v1 --name prod-test
```

Verify the image is available in kind
Kubernetes doesn’t talk to Docker directly anymore — it talks to a container runtime via the CRI API (a gRPC interface). Crictl lets you:
* Inspect containers, pods, and images directly at the runtime level.
* Troubleshoot when a pod is failing to start and you want to **bypass kubectl** and see what’s happening under the hood.
* Pull or remove images in the runtime manually.
* View logs or exec into containers even if kubelet or kubectl are down.
```bash
docker exec -it prod-test-worker crictl images | grep spark-wordcount

# crictl is the go-to tool when something is broken below Kubernetes, especially in ContainerCreating or ImagePullBackOff situations.
crictl pods
crictl ps -a

# list all layers of an image
docker history spark:4.0.0
# to dive deeper into layer content
brew install dive
dive spark:4.0.0

# Check docker container disk usage
docker system df -v
docker inspect prod-test-worker --format '{{json .Mounts}}' | jq
```

debugging commands
```bash
# Check the JAR file permissions inside the container
kubectl exec spark-wordcount-driver -- ls -la /opt/spark-work-dir/
# Verify the JAR contains your class
docker run --rm spark-wordcount:v1 jar tf /opt/spark-work-dir/word-count.jar | grep WordCountEMR
```
