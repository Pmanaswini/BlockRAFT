#!/usr/bin/env bash

set -e

###############################################################################
# 1) Create seven Multipass instances
###############################################################################
NODES=(node0 node1 node2 node3 node4 node5 node6)

echo -e "\033[31m=== Creating multipass instances (node0..node6) ===\033[0m"
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
IP_NODE3=$(multipass info node3 | grep IPv4 | awk '{print $2}')
IP_NODE4=$(multipass info node4 | grep IPv4 | awk '{print $2}')
IP_NODE5=$(multipass info node5 | grep IPv4 | awk '{print $2}')
IP_NODE6=$(multipass info node6 | grep IPv4 | awk '{print $2}')

echo "node0 IP: $IP_NODE0"
echo "node1 IP: $IP_NODE1"
echo "node2 IP: $IP_NODE2"
echo "node3 IP: $IP_NODE3"
echo "node4 IP: $IP_NODE4"
echo "node5 IP: $IP_NODE5"
echo "node6 IP: $IP_NODE6"

###############################################################################
# 3) Install Docker inside each instance
###############################################################################
echo -e "\033[31m=== Installing Docker on each node ===\033[0m"
for i in {0..6}; do
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
# 4) Launch etcd containers on each node
###############################################################################
echo -e "\033[31m=== Deploying etcd containers ===\033[0m"

# We'll reference all nodes in the cluster
ETCD_CLUSTER="s0=http://$IP_NODE0:2380,s1=http://$IP_NODE1:2380,s2=http://$IP_NODE2:2380,s3=http://$IP_NODE3:2380,s4=http://$IP_NODE4:2380,s5=http://$IP_NODE5:2380,s6=http://$IP_NODE6:2380"

for i in {0..6}; do
  NAME="s$i"
  NODE_IP_VAR="IP_NODE$i"
  NODE_IP="${!NODE_IP_VAR}"

  multipass exec node$i -- bash -c "\
    mkdir -p /tmp/etcd-data.tmp
    cat <<EOF > etcd-node$i.yaml
version: '3.8'
services:
  etcd:
    image: gcr.io/etcd-development/etcd:v3.5.15
    container_name: etcd-gcr-v3.5.15-node$i
    command: >
      /usr/local/bin/etcd
      --name $NAME
      --data-dir /etcd-data
      --listen-client-urls http://0.0.0.0:2379
      --advertise-client-urls http://$NODE_IP:2379
      --listen-peer-urls http://0.0.0.0:2380
      --initial-advertise-peer-urls http://$NODE_IP:2380
      --initial-cluster $ETCD_CLUSTER
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

    docker-compose -p etcd-node$i -f etcd-node$i.yaml up -d
  "
done

###############################################################################
# 5) Launch Redpanda containers on each node
###############################################################################
echo -e "\033[31m=== Deploying Redpanda containers ===\033[0m"

for i in {0..6}; do
  NODE_ID=$((i+1))
  NODE_IP_VAR="IP_NODE$i"
  NODE_IP="${!NODE_IP_VAR}"

  multipass exec node$i -- bash -c "\
cat <<EOF > redpanda-node$i.yaml
version: '3.8'
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.10
    container_name: redpanda-node-$NODE_ID
    command:
      - redpanda
      - start
      - --smp 1
      - --reserve-memory 0M
      - --overprovisioned
      - --node-id $NODE_ID
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://$NODE_IP:9092,external://$NODE_IP:19092
      - --rpc-addr 0.0.0.0:33145
      - --advertise-rpc-addr $NODE_IP:33145"
  if [ $i -ne 0 ]; then
    echo "      - --seeds $IP_NODE0:33145" >> redpanda-node$i.yaml
  fi
  echo "    ports:
      - '19092:19092'
      - '9092:9092'
      - '33145:33145'
      - '9644:9644'
EOF

docker-compose -p redpanda-node$i -f redpanda-node$i.yaml up -d
"
done

###############################################################################
# Done
###############################################################################
echo -e "\033[31m=== All done! ===\033[0m"
echo "Etcd and Redpanda should now be running on node0..node6."
echo "You can verify by connecting via 'multipass shell nodeX' and using 'docker ps'."
