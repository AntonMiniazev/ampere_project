PROJECT_NAME = "ampere"
NODES = {
  "master" => { name: "#{PROJECT_NAME}-k8s-master", ip: "192.168.10.100", cpus: 2, memory: 4096 },
  "node1"  => { name: "#{PROJECT_NAME}-k8s-node1",  ip: "192.168.10.101", cpus: 2, memory: 4096 },
  "node2"  => { name: "#{PROJECT_NAME}-k8s-node2",  ip: "192.168.10.102", cpus: 2, memory: 4096 },
  "node3"  => { name: "#{PROJECT_NAME}-k8s-node3",  ip: "192.168.10.103", cpus: 4, memory: 8192 }
}