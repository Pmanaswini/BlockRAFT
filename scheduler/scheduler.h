#include <pthread.h>
#include <tbb/concurrent_hash_map.h>

#include <atomic>
#include <cmath>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

#include "../dagModule/DAGmodule.h"
#include "../smartContracts/eCommerce/eCommProcessor.h"
#include "../smartContracts/nft/nftProcessor.h"
#include "../smartContracts/voting/votingProcessor.h"
#include "../smartContracts/wallet/walletProcessor.h"
#include "block.pb.h"
#include "components.pb.h"
#include "matrix.pb.h"
#include "transaction.pb.h"
#include "addressList.pb.h"
using namespace std;

class scheduler {
 public:
  GlobalState& state;
  DAGmodule dag;
  unordered_set<int> processed_components;
  int threadCount;
  vector<transaction::Transaction> transactions;
  eCommProcessor eCommPro;
  WalletProcessor walletPro;
  NFTProcessor nftPro;
  VotingProcessor votePro;
  atomic<int> compCount{0}, flag{true}, completeFlag{false}, updateFlag{false};

  tbb::concurrent_hash_map<std::string, std::string> myMap;
  // Constructor
  scheduler(GlobalState& statePtr)
      : state(statePtr),
        eCommPro(state, myMap),
        walletPro(state, myMap),
        nftPro(state, myMap),
        votePro(state, myMap) {}
  int columnSum(int colIndex) {
    int sum = 0;

    for (const auto& row : dag.adjacencyMatrix) {
      if (colIndex < row.size()) {
        sum += row[colIndex];
      }
    }

    return sum;
  }

void reset() {
  // Clear component tracker
  processed_components.clear();

  // Clear transactions
  transactions.clear();
  dag.dagClean();

  // Reset atomics
  compCount.store(0);
  flag.store(true);
  completeFlag.store(false);
  updateFlag.store(false);

  // Clear the concurrent map
  myMap.clear();  // TBB's concurrent_hash_map supports clear()
}

  // Serializes and stores shared map state to etcd
  string sendData() {
    addressList::AddressValueList protoList;

    // Build the protobuf message from the map
    for (const auto& kv : myMap) {
        addressList::AddressValue* pair = protoList.add_pairs();
        pair->set_address(kv.first);
        pair->set_value(kv.second);
    }

    // Serialize to string
    std::string serializedData;
    if (!protoList.SerializeToString(&serializedData)) {
        std::cerr << "Failed to serialize AddressValueList!" << std::endl;
        return serializedData;
    }
return serializedData;
}
  // Serializes and stores shared map state to etcd
  void dataStore(const std::string& path) {
    addressList::AddressValueList protoList;

    // Build the protobuf message from the map
    for (const auto& kv : myMap) {
        addressList::AddressValue* pair = protoList.add_pairs();
        pair->set_address(kv.first);
        pair->set_value(kv.second);
    }

    // Serialize to string
    std::string serializedData;
    if (!protoList.SerializeToString(&serializedData)) {
        std::cerr << "Failed to serialize AddressValueList!" << std::endl;
        return;
    }

    std::cout << "Path of store DATA is " << path << std::endl;
    std::cout << "Size of serialized proto DATA is " << serializedData.size() << std::endl;

    // Store in etcd (binary-safe)
    etcdClient.put(path, serializedData).get();
}
  // Parses and loads DAG matrix from serialized protobuf input
  void extractDAG(string matrixData) {
    // Deserialize the Protobuf message
    matrix::DirectedGraph graph;
    if (!graph.ParseFromString(matrixData)) {
      cerr << "Failed to parse DirectedGraph protobuf message." << endl;
      return;
    }
    dag.totalTxns = graph.num_nodes();
    dag.completedTxns = graph.num_nodes();
    dag.inDegree = unique_ptr<atomic<int>[]>(new atomic<int>[dag.totalTxns]);
    // Initialize all positions to -1
    for (size_t i = 0; i < dag.totalTxns; ++i) {
      dag.inDegree[i].store(-1, std::memory_order_relaxed);
    }
    dag.adjacencyMatrix.resize(
        dag.totalTxns,
        vector<int>(dag.totalTxns,
                    0));  // resizing the matrix based on the transaction count
    for (int i = 0; i < graph.adjacencymatrix_size(); ++i) {
      if (i >= dag.adjacencyMatrix.size()) {
        cerr << "Row index out of bounds: " << i << endl;
        continue;
      }
      const matrix::DirectedGraph::MatrixRow& row = graph.adjacencymatrix(i);
      for (int j = 0; j < row.edges_size(); ++j) {
        if (j >= dag.adjacencyMatrix[i].size()) {
          cerr << "Column index out of bounds: row " << i << ", col " << j
               << endl;
          continue;
        }
        dag.adjacencyMatrix[i][j] = row.edges(j);
      }
    }
  }
  // Updates in-degree of transactions from a component
  void ProcessIndegree(const components::componentsTable::component component) {
    int col = 0, sum = 0, comp = component.compid();
    for (int j = 0; j < component.transactionlist_size(); ++j) {
      col = component.transactionlist(j).id();
      sum = columnSum(col);
      dag.inDegree[col].store(sum, std::memory_order_relaxed);
      dag.completedTxns--;
    }
  }

  bool CheckForNewComponents(const auto& component) {
    int component_id = component.compid();
    if (processed_components.find(component_id) == processed_components.end()) {
      processed_components.insert(component_id);
      return true;
    }
    return false;
  }
  // Deserialize and process relevant components assigned to this node
  void ExtractComponents(const std::string& serialized_data, int node_id) {
    // Parse the serialized data
    if (!dag.cTable.ParseFromString(serialized_data)) {
      std::cerr << "Failed to parse componentsTable data." << std::endl;
      return;
    }
    for (int i = 0; i < dag.cTable.componentslist_size(); ++i) {
      const auto& component = dag.cTable.componentslist(i);
      if (node_id == component.assignedfollower() &&
          CheckForNewComponents(component)) {
        ProcessIndegree(component);
      }
    }
    updateFlag.store(true);
  }

  void ExtractNewComponents(const std::string& serialized_data, int node_id) {
    // Parse the serialized data
    bool etcdReset = false;
    if (!dag.cTable.ParseFromString(serialized_data)) {
      std::cerr << "Failed to parse componentsTable data." << std::endl;
      return;
    }
    // Iterate through the list of components
    for (int i = 0; i < dag.cTable.componentslist_size(); ++i) {
      const auto& component = dag.cTable.componentslist(i);
      if (node_id == component.assignedfollower() &&
          CheckForNewComponents(component)) {
        ProcessIndegree(component);
        updateFlag.store(true);
      }
    }
  }

  void executeTxns(int PID, const std::string& leader_id,
                   const std::string& term_no, int block_num) {
    std::string path = leader_id + "/" + term_no + "/" +
                       std::to_string(block_num) + "/addresses";

    while ((!completeFlag.load()) && flag.load()) {
      if (dag.completedTxns == dag.totalTxns && updateFlag) {
        // dataStore(leader_id + "/" + term_no + "/" + std::to_string(block_num) +"/"+node_id+"/data");
        std::string comp_key = leader_id + "/" + term_no + "/" +
                               std::to_string(block_num) +
                               "/components/status" + "/" + node_id;
        auto put_response =
            etcdClient.put(comp_key, std::to_string(compCount)).get();
        if (put_response.is_ok()) {
          updateFlag.store(false);
        } else {
          std::cerr << "Failed to store component in etcd: "
                    << put_response.error_message() << std::endl;
        }
      }
      int txnId = dag.selectTxn();
      // cout<<"selected: "<<txnId<<" "<<dag.completedTxns<<" "<<dag.totalTxns<<endl;
      if (txnId != -1) {
        transaction::Transaction txn =
            transactions[txnId];  // Directly use txnId to get the transaction
        transaction::TransactionHeader header;

        if (!header.ParseFromString(txn.header())) {
          cerr << "Failed to parse transaction header." << endl;
          continue;
        }

        if (header.family_name() == "wallet") {
          flag.store(walletPro.ProcessTxn(txn));  // Call wallet processing
        } else if (header.family_name() == "eComm") {
          flag.store(
              eCommPro.ProcessTxn(txn, path));  // Call eCommerce processing
        } else if (header.family_name() == "nft") {
          flag.store(nftPro.ProcessTxn(txn));  // Call eCommerce processing
        } else if (header.family_name() == "voting") {
          flag.store(votePro.ProcessTxn(txn));  // Call eCommerce processing
        } else {
          flag.store(false);  // Use store for atomic flag assignment
        }

        dag.complete(txnId);
        compCount.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }
  void extractBlock(const string& blockData) {
    Block block;

    if (!block.ParseFromString(blockData)) {
      cerr << "Failed to parse block data." << endl;
      return;
    }

    transactions.assign(block.transactions().begin(),
                        block.transactions().end());

    dag.createfollower(block, threadCount);
  }

  // this thread monitors the status from the leader
  void monitorFunc(const std::string& leader_id, const std::string& term_no,
                   int block_num) {
    std::string run_key =
        leader_id + "/" + term_no + "/" + std::to_string(block_num) + "/run";
    while (flag.load()) {
      try {
        etcd::Response response = etcdClient.get(run_key).get();
        if (!response.is_ok()) {
          std::cerr << "Failed to get run status: " << response.error_message()
                    << std::endl;
        }

        std::string status = response.value().as_string();

        if (status == "finish") {
          completeFlag.store(true);
          cout << "leader said finish" << endl;
          return;
        }
        // std::this_thread::sleep_for(std::chrono::milliseconds(10));

      } catch (const std::exception& e) {
        std::cerr << "Exception while polling run key: " << e.what()
                  << std::endl;
      }
    }
  }

  bool scheduleTxns(const std::string& leader_id, const std::string& term_no,
                    int block_num, int thCount) {
    threadCount = thCount;
    thread threads[threadCount], runMonitor;
    flag.store(true);
    completeFlag.store(false);
    cout << "Transactions to execute is " << dag.totalTxns - dag.completedTxns
         << endl;
    if(dag.totalTxns - dag.completedTxns <= 0) {
      cout << "No transactions to execute" << endl;
      std::string comp_key = leader_id + "/" + term_no + "/" +
                               std::to_string(block_num) +
                               "/components/status" + "/" + node_id;
        auto put_response =
            etcdClient.put(comp_key, "0").get();
    }
    cout<<"threads got created"<<endl;
    for (int i = 0; i < threadCount; i++) {
      threads[i] = thread(&scheduler::executeTxns, this, i, leader_id, term_no,
                          block_num);
    }
    runMonitor =
        thread(&scheduler::monitorFunc, this, leader_id, term_no, block_num);
    runMonitor.join();
    for (int i = 0; i < threadCount; i++) {
      threads[i].join();  // Wait for all threads to finish
    }
    
    return flag.load();
    
  }
};
