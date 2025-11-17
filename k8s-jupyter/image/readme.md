```bash
# build the image with a tag
# the last `.` indicates the context path, which controls what files are visible to the Dockerfile during build (e.g. what COPY/ADD can see)
docker build -t jupyter-scala:v1 -f ./k8s-jupyter/image/Dockerfile .

# Verify the image was created
docker images | grep jupyter-scala
docker images --digests
docker image inspect jupyter-scala:v1

# Load the Image into kind
# --name part is for kind cluster name (`kind get clusters`)
kind load docker-image jupyter-scala:v1 --name prod-test
```