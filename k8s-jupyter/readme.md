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
kubectl cp ./k8s-jupyter/notebooks/legacy_way_in_pod.ipynb default/jupyter-notebook-f687777f6-hxt94:/home/jovyan/my_notebooks

# Copy pod files to local
kubectl cp default/jupyter-notebook-f687777f6-hxt94:/home/jovyan/my_notebooks/legacy_way_in_pod.ipynb ./k8s-jupyter/notebooks/legacy_way.ipynb
# Copy the whole directory
kubectl cp default/jupyter-notebook-f687777f6-hxt94:/home/jovyan/my_notebooks ./k8s-jupyter/notebooks
```

The only difference between the local Jupyter file and remote Jupyter file (on pod) is how the kube config is loaded
* Compare the difference between [legacy_way_in_pod.ipynb](notebooks/legacy_way_in_pod.ipynb) and [legacy_way_local.ipynb](notebooks/legacy_way_local.ipynb) to see the details

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

