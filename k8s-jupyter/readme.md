```bash
# access Jupyter notebook through http://localhost:8888/
kubectl port-forward service/jupyter-notebook-service 8888:8888
```

# Local Commands
```bash
# to install jupyter notebook
pip3 install jupyterlab

# start the jupyter notebook locally using current folder
jupyter lab
```

Sync files between local and pod
```bash
kubectl exec -n default jupyter-notebook-f687777f6-hxt94 -- mkdir -p /home/jovyan/my_notebooks

# Copy local files to pod
# container name "jupyter" is needed when there are multiple containers in the pod
kubectl cp -c jupyter ./k8s-jupyter/notebooks/pods/. default/jupyter-notebook-75b646b999-p5phx:/home/jovyan/work/

# Copy pod files to local
kubectl cp default/jupyter-notebook-75b646b999-p5phx:/home/jovyan/work ./k8s-jupyter/notebooks/pods
```

The only difference between the local Jupyter file and remote Jupyter file (on pod) is how the kube config is loaded
* Compare the difference between [legacy_way_pod.ipynb](notebooks/pods/legacy_way_pod.ipynb) and [legacy_way_local.ipynb](notebooks/local/legacy_way_local.ipynb) to see the details

# Jupyter in K8S
When you start the container for the first time (without setting a password), Jupyter automatically generates
a one-time login token for security.

You’ll see something like this in the container logs:
```
To access the server, open this file in a browser:
    file:///home/jovyan/.local/share/jupyter/runtime/jpserver-11-open.html
Or copy and paste one of these URLs:
    http://jupyter-notebook-f687777f6-hxt94:8888/lab?token=ea936d053d06e686524b5bfbc3b30ab77f1bc75aa39a8e3e
    http://127.0.0.1:8888/lab?token=ea936d053d06e686524b5bfbc3b30ab77f1bc75aa39a8e3e
```
or get Jupyter token as below
```bash
kubectl logs jupyter-notebook-f687777f6-hxt94 | grep "token="
```

# Run Scala in Jupyter
## Local setup
```bash
# Install coursier
brew install coursier/formulas/coursier
cs --version
# Install Almond
cs launch --use-bootstrap almond:0.14.1 --scala 2.13.16 -- --install --force

# set env var before starting Jupyter
# or keep it in ~/Library/Jupyter/kernels/scala/kernel.json
export JDK_JAVA_OPTIONS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED $JDK_JAVA_OPTIONS"
jupyter lab
```

## install Spark jars on the classpath
```bash
# To install Spark jars on the classpath, run below in the Jupyter notebook
import $ivy.`org.apache.spark::spark-connect-client-jvm:4.0.0`
```

