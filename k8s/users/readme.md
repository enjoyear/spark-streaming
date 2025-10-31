Create an Admin User
```bash
kubectl apply -f ./k8s/users/admin-user.yaml
```

Get the Access Token
```bash
kubectl -n kubernetes-dashboard create token admin-user
# eyJhbGciOiJSUzI1NiIsImtpZCI6Im5IRGtGR0pJYnRqSGFCQXB0V3ZSTy1KYl9HdjF5MUFyVXVfaERfTmRLV2cifQ.eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiXSwiZXhwIjoxNzYxODgxNzc5LCJpYXQiOjE3NjE4NzgxNzksImlzcyI6Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwianRpIjoiMDljYWZmNTYtMTQwZi00M2Q3LWJiYzQtODZkNTcwZDVkMTQyIiwia3ViZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJrdWJlcm5ldGVzLWRhc2hib2FyZCIsInNlcnZpY2VhY2NvdW50Ijp7Im5hbWUiOiJhZG1pbi11c2VyIiwidWlkIjoiZDRiZGRjMDMtODFmYi00MmUwLTljMjUtZjQ4OWU1NzhmMTEwIn19LCJuYmYiOjE3NjE4NzgxNzksInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDprdWJlcm5ldGVzLWRhc2hib2FyZDphZG1pbi11c2VyIn0.W_WvFRWURV3HOPpmwnrs2cA7MV1tFzeGo_ILixQQ0pMG5dQKRK2cKwK9qgD0pOd90uxC2EqsjeiorqC08YMAhdHglmkcV3QZd4ErQOpRJ_hfBwdqvcmX2jaCa3NEsnoZrBMbfyoF5yyWiu9xnFoi0NBEBjDH5nDyvYskIDI4rH6yE6ygSeARa4XuxqwFChQNtehsVC2sGraHCIqe2-AouGEJ5Owf-SMIn0rTfKfCFcarlR2hic7gjiT7aWCDG9DT_xmbHMtaK_0gJ5USsAtgPQ4QehhpExZW_rwpscrcpEBvsgyfIVZChvw8GhZMWN9ky2B3BROamwwDOnQ589igog
```

Access the Dashboard
`kubectl proxy` creates a local HTTP server that acts as a secure gateway to the Kubernetes API server:
With proxy:
* Your Browser → kubectl proxy (localhost:8001) → Kubernetes API Server (with auth)
* kubectl proxy handles all authentication for you (uses your kubeconfig credentials)
* Exposes a plain HTTP endpoint locally
    * [dashboard URL](http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/)
        * `https:kubernetes-dashboard:` is the service identifier
            - `https:` - the protocol/port name of the service
            - `kubernetes-dashboard` - the service name
            - The final `:` is a separator
        * `/proxy/` tells the API server "proxy HTTP requests through to this service"
    * Alternative approach, use port-forward instead: `kubectl -n kubernetes-dashboard port-forward svc/kubernetes-dashboard 8443:443`
        * Now just visit: https://localhost:8443
* You can make API calls without managing certificates or tokens

Without proxy:
* API server requires client certificates or bearer tokens with every request
* Uses HTTPS with self-signed certificates
* Direct communication: https://127.0.0.1:6443 (API server address)

```bash
kubectl proxy
# Start proxy in background
# kubectl proxy &

# Example APIs
curl http://localhost:8001/api/v1/namespaces
curl http://localhost:8001/api/v1/namespaces/default/pods

# Stop the proxy
killall kubectl
```
