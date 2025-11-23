commands
```bash
./gradlew :word-count:build
```

spark-submit
```bash
brew install apache-spark

$SPARK_HOME/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc \
  --deploy-mode cluster \
  --name word-count-emr \
  --class com.chen.guo.WordCountEMR \
  --conf spark.kubernetes.namespace=spark-operator \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-all-purpose-driver1 \
  --conf spark.kubernetes.container.image=docker.io/library/spark:4.0.0 \
  local:///opt/spark/jars/word-count.jar \
  p1 p2
  

spark-submit \
  --master k8s://https://kubernetes.default.svc \
  --deploy-mode client \
  --name word-count-emr \
  --class com.chen.guo.WordCountEMR \
  --conf spark.kubernetes.namespace=spark-operator \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-all-purpose-driver1 \
  --conf spark.kubernetes.container.image=docker.io/library/spark:4.0.0 \
  local:///opt/spark/jars/word-count.jar \
  p1 p2
```
