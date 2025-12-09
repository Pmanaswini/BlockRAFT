# Distributed Framework

This project uses CMake as the build system. Follow the steps below to configure, build, and run the project.

---

## Project Structure

```
Distributed_Framework/
├── build/               # Generated build files
│   ├── client           # Compiled client executable
│   ├── RestAPI          # Compiled REST API executable
│   └── other build files...
├── git-hooks/pre-commit # Clang-format checking
├── blocksDB
├── block_producer
├── DAG-module
├── merkelTree
├── Crow/
├── googletest/
├── protos/
│   ├── protoUtils/
│   ├── block.proto
│   ├── component.proto
│   ├── transaction.proto
│   └── matrix.proto
├── client/              # Contains client.cpp
├── RestAPI/             # Contains RestAPI.cpp
├── auto_deploy.sh       # Auto deployment script     
└── CMakeLists.txt
```

---

## Requirements

- CMake version 3.20 or higher
- GCC 11.4.0 compiler or higher
- Google Protocol Buffers

---

## Creating Multipass VMs

```bash
chmod +x autoDeploy.sh
./autoDeploy.sh
```

---

## Copying Files to the VMs

Execute this from your host machine (outside of VMs):

```bash
multipass transfer -r ./ node0:Distributed_Framework
multipass transfer -r ./ node1:Distributed_Framework
multipass transfer -r ./ node2:Distributed_Framework
```

---

## Install Additional Packages (If Not Installed)

Execute this inside each VM:

```bash
multipass shell node0
multipass shell node1
multipass shell node2
```

Then run:

```bash
sudo apt-get update && \
sudo apt-get install -y cmake unzip python3-pip protobuf-compiler libprotobuf-dev \
  libboost-all-dev libssl-dev libcpprest-dev libboost-system-dev \
  libcurl4-openssl-dev librdkafka-dev libasio-dev libgflags-dev \
  nlohmann-json3-dev libgtest-dev librocksdb-dev \
  libgrpc-dev libgrpc++-dev protobuf-compiler-grpc etcd-client libtbb-dev
```
---

## Install rpk for Redpanda

```bash
curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip &&
    mkdir -p ~/.local/bin &&
    export PATH="~/.local/bin:$PATH" &&
    unzip rpk-linux-amd64.zip -d ~/.local/bin/
```

## Install Etcd-cpp-api

```bash
  cd
  git clone --recurse-submodules https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3.git
  sed -i '1i #include <cstdint>' etcd-cpp-apiv3/etcd/v3/Member.hpp
  sed -i '1i #include <cstdint>' etcd-cpp-apiv3/src/v3/Member.cpp 
  cd etcd-cpp-apiv3
  mkdir build && cd build
  cmake ..
  sudo make -j4
  sudo make install
```
---

---

## Build the Project

Run CMake to configure and build the project:

```bash
cd
cd BlockRAFT
mkdir build
cd build
cmake ..
make
```

---

## Running the Executables

```bash
ctest
./node
```

### Test the RestAPI Module

```bash
./RestAPI   # Run the RestAPI
./client    # Run the client
```

### Download rpk on the Host (Refer to Redpanda Documentation)

```bash
rpk topic consume transaction_pool --brokers localhost:19092  # Check submitted transactions
rpk cluster info -X brokers=localhost:19092                   # Check the cluster
```

---

## Deleting Multipass VMs

```bash
multipass stop node0 node1 node2
multipass delete node0 node1 node2
multipass purge
```

