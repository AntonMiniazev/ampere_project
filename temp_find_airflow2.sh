ssh -J oppie@oppie-server -o BatchMode=yes ampere@ampere-k8s-master 'kubectl --kubeconfig=$HOME/.kube/config get pods -A | grep -E "triggerer|worker"'
