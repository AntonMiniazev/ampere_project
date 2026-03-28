ssh -J oppie@oppie-server -o BatchMode=yes ampere@ampere-k8s-master 'kubectl --kubeconfig=$HOME/.kube/config -n ampere get pods -l task_id=run__sparkapp__group_snapshots-mutable-dims -o name'
