# InodeDB on Kubernetes

Three InodeDB servers on FoundationDB, as a StatefulSet: one writes and two
stand by. Clients connect to the `inodedb` service. A standby answers reads
at a version the client has seen and sends everything else to the writer,
which the client then talks to directly. When the writer stops renewing,
a standby takes over after the writer lease and margin, six seconds by
default ([Fencing](../../DESIGN.md#fencing)).

`fdb.yaml` runs a one-process FoundationDB cluster for trying this out;
production uses a cluster the FoundationDB operator runs, whose cluster
file it publishes as the `fdb-cluster` ConfigMap.

Each server asks that the node's cores stay out of deep idle states, which
takes write access to `/dev/cpu_dma_latency`. Without it a server runs as usual,
and each thread a request wakes on an idle core pays that core's exit latency,
190 µs on an EC2 i7ie ([Idle states](../../BENCHMARKS.md#idle-states)). On nodes
you control, run the node's `tuned` under its `latency-performance` profile, or
give the pod the device.

## On a local kind cluster

```sh
docker build -t inodedb:dev -f deploy/kubernetes/Dockerfile .
kind create cluster --name inodedb
kind load docker-image inodedb:dev --name inodedb
kubectl apply -f deploy/kubernetes/fdb.yaml
kubectl rollout status statefulset/fdb
kubectl create secret generic inodedb-key \
  --from-literal=key="$(openssl rand -hex 8).$(openssl rand -hex 16)"
kubectl apply -f deploy/kubernetes/inodedb.yaml
kubectl rollout status statefulset/inodedb
```

Write through the service, kill the writer, and write again:

```sh
idb() {
  kubectl exec inodedb-2 -- sh -c \
    "INODEDB_ENDPOINT=http://inodedb:7070 INODEDB_KEY=\$(cat /etc/inodedb-key/key) idb $*"
}
idb mkdir t && idb put t/a hello && idb ls -l t
kubectl logs -l app=inodedb --prefix | grep msg=writing    # the writer
kubectl delete pod inodedb-0 --grace-period=0 --force       # if it writes
idb put t/b after                                           # once a standby takes over
idb ls t
```
