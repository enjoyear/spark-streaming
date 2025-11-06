sparkoperator.k8s.io/v1beta2 [API Doc](https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md)

Found the Spark application jar at spark-streaming/word-count/build/libs/word-count.jar
```bash
./gradlew clean :word-count:build
```

To mount the local JAR into the pods. There are a few options:
* Use a ConfigMap (for smaller JARs < 1MB)
- ConfigMap is stored in etcd/cluster

The flow: kubectl base64-encodes the binary content of the jar file, then stores it in the ConfigMap with the key `word-count.jar`
```bash
# option --from-file=[]:
# 	Key file can be specified using its file path, in which case file basename will be used as configmap key, or
#	optionally with a key and file path, in which case the given key will be used.  Specifying a directory will
#	iterate each named file in the directory whose basename is a valid configmap key.
kubectl create configmap word-count-jar --from-file=word-count.jar=/Users/chenguo/src/chen/spark-streaming/word-count/build/libs/word-count.jar

# check KVPs in the configmap
kubectl describe configmap word-count-jar
```




