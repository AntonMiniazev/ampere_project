ssh -J oppie@oppie-server -o BatchMode=yes ampere@ampere-k8s-master 'kubectl --kubeconfig=$HOME/.kube/config -n airflow get pods | grep -E "web|scheduler"'
