ssh -J oppie@oppie-server -o BatchMode=yes ampere@ampere-k8s-master 'kubectl --kubeconfig=$HOME/.kube/config -n ampere get sparkapplications -o wide | grep 20260328t041500'
