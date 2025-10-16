#!/usr/bin/env bash

set -e

###############################################################################
# 1) Create three Multipass instances
###############################################################################
NODES=(node0 node1 node2)

echo -e "\033[31m=== Creating multipass instances (node0, node1, node2) ===\033[0m"
for NODE in "${NODES[@]}"; do
  if multipass list | grep -q "^$NODE[[:space:]]"; then
    echo "Instance '$NODE' already exists. Skipping creation..."
  else
    multipass launch -n "$NODE" --cpus 8 --mem 10G --disk 20G 24.04
  fi
done

# Give the VMs some time to boot up
echo -e "\033[31m=== Waiting for instances to be ready... ===\033[0m"
sleep 15

###############################################################################
# 2) Retrieve IP addresses for the newly launched instances
###############################################################################
echo -e "\033[31m=== Retrieving IP addresses ===\033[0m"
IP_NODE0=$(multipass info node0 | grep IPv4 | awk '{print $2}')
IP_NODE1=$(multipass info node1 | grep IPv4 | awk '{print $2}')
IP_NODE2=$(multipass info node2 | grep IPv4 | awk '{print $2}')

echo "node0 IP: $IP_NODE0"
echo "node1 IP: $IP_NODE1"
echo "node2 IP: $IP_NODE2"

###############################################################################
# 3) Install Docker inside each instance
###############################################################################
echo -e "\033[31m=== Installing Docker on each node ===\033[0m"
for i in 0 1 2; do
  multipass exec node$i -- bash -c "sudo rm -f /etc/apt/sources.list.d/docker.list || true"
  multipass exec node$i -- sudo apt update -y
  multipass exec node$i -- sudo apt install -y docker.io
  multipass exec node$i -- sudo systemctl start docker
  multipass exec node$i -- sudo systemctl enable docker
  multipass exec node$i -- docker --version
  multipass exec node$i -- sudo usermod -aG docker ubuntu
  multipass exec node$i -- sudo apt install -y docker-compose
done

###############################################################################
# 4) Launch etcd containers on each node (with custom project names)
###############################################################################
echo -e "\033[31m=== Deploying etcd containers ===\033[0m"

# node0 etcd
multipass exec node0 -- bash -c "\
  mkdir -p /tmp/etcd-data.tmp
  cat <<EOF > etcd-node0.yaml
version: '3.8'
services:
  etcd:
    image: gcr.io/etcd-development/etcd:v3.5.15
    container_name: etcd-gcr-v3.5.15
    command: >
      /usr/local/bin/etcd
      --name s0
      --data-dir /etcd-data
      --listen-client-urls http://0.0.0.0:2379
      --advertise-client-urls http://$IP_NODE0:2379
      --listen-peer-urls http://0.0.0.0:2380
      --initial-advertise-peer-urls http://$IP_NODE0:2380
      --initial-cluster s0=http://$IP_NODE0:2380,s1=http://$IP_NODE1:2380,s2=http://$IP_NODE2:2380
      --initial-cluster-token tkn
      --initial-cluster-state new
      --log-level info
      --logger zap
      --log-outputs stderr
    ports:
      - '2379:2379'
      - '2380:2380'
    volumes:
      - '/tmp/etcd-data.tmp:/etcd-data'
EOF

  # Use a unique project name for node0's etcd
  docker-compose -p etcd-node0 -f etcd-node0.yaml up -d
"

# node1 etcd
multipass exec node1 -- bash -c "\
  mkdir -p /tmp/etcd-data.tmp
  cat <<EOF > etcd-node1.yaml
version: '3.8'
services:
  etcd:
    image: gcr.io/etcd-development/etcd:v3.5.15
    container_name: etcd-gcr-v3.5.15-node1
    command: >
      /usr/local/bin/etcd
      --name s1
      --data-dir /etcd-data
      --listen-client-urls http://0.0.0.0:2379
      --advertise-client-urls http://$IP_NODE1:2379
      --listen-peer-urls http://0.0.0.0:2380
      --initial-advertise-peer-urls http://$IP_NODE1:2380
      --initial-cluster s0=http://$IP_NODE0:2380,s1=http://$IP_NODE1:2380,s2=http://$IP_NODE2:2380
      --initial-cluster-token tkn
      --initial-cluster-state new
      --log-level info
      --logger zap
      --log-outputs stderr
    ports:
      - '2379:2379'
      - '2380:2380'
    volumes:
      - '/tmp/etcd-data.tmp:/etcd-data'
EOF

  # Use a unique project name for node1's etcd
  docker-compose -p etcd-node1 -f etcd-node1.yaml up -d
"

# node2 etcd
multipass exec node2 -- bash -c "\
  mkdir -p /tmp/etcd-data.tmp
  cat <<EOF > etcd-node2.yaml
version: '3.8'
services:
  etcd:
    image: gcr.io/etcd-development/etcd:v3.5.15
    container_name: etcd-gcr-v3.5.15-node2
    command: >
      /usr/local/bin/etcd
      --name s2
      --data-dir /etcd-data
      --listen-client-urls http://0.0.0.0:2379
      --advertise-client-urls http://$IP_NODE2:2379
      --listen-peer-urls http://0.0.0.0:2380
      --initial-advertise-peer-urls http://$IP_NODE2:2380
      --initial-cluster s0=http://$IP_NODE0:2380,s1=http://$IP_NODE1:2380,s2=http://$IP_NODE2:2380
      --initial-cluster-token tkn
      --initial-cluster-state new
      --log-level info
      --logger zap
      --log-outputs stderr
    ports:
      - '2379:2379'
      - '2380:2380'
    volumes:
      - '/tmp/etcd-data.tmp:/etcd-data'
EOF

  # Use a unique project name for node2's etcd
  docker-compose -p etcd-node2 -f etcd-node2.yaml up -d
"

###############################################################################
# 5) Launch Redpanda containers on each node (with custom project names)
###############################################################################
echo -e "\033[31m=== Deploying Redpanda containers ===\033[0m"

########################
# Redpanda on node0
########################
multipass exec node0 -- bash -c "\
cat <<EOF > redpanda-node0.yaml
version: '3.8'
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.10
    container_name: redpanda-node-1
    command:
      - redpanda
      - start
      - --smp 1
      - --reserve-memory 0M
      - --overprovisioned
      - --node-id 1
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://$IP_NODE0:9092,external://$IP_NODE0:19092
      - --rpc-addr 0.0.0.0:33145
      - --advertise-rpc-addr $IP_NODE0:33145
    ports:
      - '19092:19092'
      - '9092:9092'
      - '33145:33145'
      - '9644:9644'
EOF

docker-compose -p redpanda-node0 -f redpanda-node0.yaml up -d
"

########################
# Redpanda on node1
########################
multipass exec node1 -- bash -c "\
cat <<EOF > redpanda-node1.yaml
version: '3.8'
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.10
    container_name: redpanda-node-2
    command:
      - redpanda
      - start
      - --smp 1
      - --reserve-memory 0M
      - --overprovisioned
      - --node-id 2
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://$IP_NODE1:9092,external://$IP_NODE1:19092
      - --rpc-addr 0.0.0.0:33145
      - --advertise-rpc-addr $IP_NODE1:33145
      - --seeds $IP_NODE0:33145
    ports:
      - '19092:19092'
      - '9092:9092'
      - '33145:33145'
      - '9644:9644'
EOF

docker-compose -p redpanda-node1 -f redpanda-node1.yaml up -d
"

########################
# Redpanda on node2
########################
multipass exec node2 -- bash -c "\
cat <<EOF > redpanda-node2.yaml
version: '3.8'
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.10
    container_name: redpanda-node-3
    command:
      - redpanda
      - start
      - --smp 1
      - --reserve-memory 0M
      - --overprovisioned
      - --node-id 3
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://$IP_NODE2:9092,external://$IP_NODE2:19092
      - --rpc-addr 0.0.0.0:33145
      - --advertise-rpc-addr $IP_NODE2:33145
      - --seeds $IP_NODE0:33145
    ports:
      - '19092:19092'
      - '9092:9092'
      - '33145:33145'
      - '9644:9644'
EOF

docker-compose -p redpanda-node2 -f redpanda-node2.yaml up -d
"

###############################################################################
# Done
###############################################################################
echo -e "\033[31m=== All done! ===\033[0m"
echo "Etcd and Redpanda should now be running on node0, node1, and node2."
echo "You can verify by connecting via 'multipass shell nodeX' and using 'docker ps'."
