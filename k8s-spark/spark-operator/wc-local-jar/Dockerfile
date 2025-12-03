# The full format is: [registry/][namespace/]repository:tag
# Full name is `docker.io/library/spark:4.0.0`, which is equivalent to `docker.io/_/spark`
# Registry defaults to Docker Hub (docker.io). To check registries `docker info | grep Registry`
# Namespace defaults to library (official images)

# Search Docker Hub: docker search spark
# To inspect the image: docker image inspect spark:4.0.0
FROM spark:4.0.0

# root directory is determined by the `context path` through the `docker build` command
COPY word-count/build/libs/word-count.jar /opt/spark/jars/word-count.jar