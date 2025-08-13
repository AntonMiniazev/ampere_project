PROJECT_NAME = "ampere"
NODES = {
  "master" => { name: "#{PROJECT_NAME}-k8s-master", ip: "192.168.10.100", cpus: 2, memory: 4096 },
  "node1"  => { name: "#{PROJECT_NAME}-k8s-node1",  ip: "192.168.10.101", cpus: 2, memory: 4096 },
  "node2"  => { name: "#{PROJECT_NAME}-k8s-node2",  ip: "192.168.10.102", cpus: 2, memory: 4096 },
  "node3"  => { name: "#{PROJECT_NAME}-k8s-node3",  ip: "192.168.10.103", cpus: 4, memory: 8192 }
}


  resources:
    requests:
      cpu: "500m"
      memory: "768Mi"
    limits:
      cpu: "1"
      memory: "1536Mi"

  celery:
    workerConcurrency: 3
                             

config:
  core:
    parallelism: 64
    max_active_tasks_per_dag: 32
  celery:
    worker_concurrency: 3
    prefetch_multiplier: 1