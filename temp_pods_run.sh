ssh -J oppie@oppie-server -o BatchMode=yes ampere@ampere-k8s-master 'kubectl --kubeconfig=$HOME/.kube/config -n ampere get pods -o wide | grep 041500'
