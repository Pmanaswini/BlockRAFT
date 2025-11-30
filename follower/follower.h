#pragma once
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <chrono>
#include <etcd/Client.hpp>
#include <etcd/KeepAlive.hpp>
#include <etcd/Response.hpp>
#include <etcd/Watcher.hpp>
#include <iostream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../blockProducer/blockProducer.h"
#include "../blocksDB/blocksDB.h"
#include "../dagModule/DAGmodule.h"
#include "../leader/etcdGlobals.h"
#include "../merkleTree/globalState.h"
#include "../scheduler/scheduler.h"

using namespace std;

std::atomic<bool> leaderCrashed{false};

class follower {
 public:
  string Leader;
  GlobalState state;
  GlobalState tmp;
  scheduler Scheduler;

  follower() : tmp("globalState_tmp"), Scheduler(tmp) {
    // Duplicate the state into tmp DB
    state.duplicateState("globalState_tmp");
  }
  atomic<int> stopWatcher;

  // 1. Leader and BLock Management functions :

  string getLeaderID() {
    try {
      etcd::Response response = etcdClient.get("CurrentLeader").get();

      if (!response.is_ok()) {
        BOOST_LOG_TRIVIAL(error)
            << "Error retrieving leader ID: " << response.error_message();

        return "";
      }

      Leader = response.value().as_string();
      return Leader;
    } catch (const std::exception& e) {
      BOOST_LOG_TRIVIAL(error) << "Exception occurred: " << e.what();
      return "";
    }
  }

  std::vector<std::string> getNamespaces(const std::string& leader_id,
                                         const std::string& term_no) {
    std::vector<std::string> namespaces;
    std::string prefix = leader_id + "/" + term_no + "/";

    try {
      etcd::Response response = etcdClient.ls(prefix).get();

      if (!response.is_ok()) {
        BOOST_LOG_TRIVIAL(error)
            << "Error retrieving namespaces: " << response.error_message();
        return namespaces;
      }
      // Store all keys found under the leader's term
      for (const auto& key : response.keys()) {
        namespaces.push_back(key);
      }

    } catch (const std::exception& e) {
      BOOST_LOG_TRIVIAL(error) << "Exception occurred: " << e.what();
    }

    return namespaces;
  }

  // Function to extract the largest block number
  int getLargestBlockNumber(const std::vector<std::string>& namespaces) {
    int max_block_num = -1;

    for (const auto& ns : namespaces) {
      // Example format: s0/40/1/DAG → extract the "1"
      std::stringstream ss(ns);
      std::string segment;
      std::vector<std::string> tokens;

      while (std::getline(ss, segment, '/')) {
        tokens.push_back(segment);
      }

      if (tokens.size() >= 3) {
        try {
          int block_num =
              std::stoi(tokens[2]);  // tokens[2] = "1" in "s0/40/1/DAG"
          max_block_num = std::max(max_block_num, block_num);
        } catch (const std::exception& e) {
          BOOST_LOG_TRIVIAL(error)
              << "Error parsing block number from: " << tokens[2] << " - "
              << e.what();
        }
      }
    }

    return max_block_num;
  }

  // Function to get the run status of a block
  std::string getBlockStatus(const std::string& leader_id,
                             const std::string& term_no, int block_num) {
    std::string block_status_key =
        leader_id + "/" + term_no + "/" + std::to_string(block_num) + "/run";

    try {
      etcd::Response response = etcdClient.get(block_status_key).get();

      if (!response.is_ok()) {
        BOOST_LOG_TRIVIAL(error)
            << "Error retrieving block status: " << response.error_message();
        return "";
      }

      return response.value()
          .as_string();  // Expected values: "running", "finished", etc.
    } catch (const std::exception& e) {
      BOOST_LOG_TRIVIAL(error)
          << "Exception occurred while fetching block status: " << e.what();
      return "";
    }
  }

  // 2. Watcher Functions :
  void watchBlockExecution(const std::string& leader_id,
                           const std::string& term_no, int block_num) {
    std::string block_status_key =
        leader_id + "/" + term_no + "/" + std::to_string(block_num) + "/run";

    etcd::Watcher watcher(
        etcdClient, block_status_key,
        [this, block_num](etcd::Response response) {
          if (response.is_ok() && response.action() == "set") {
            std::string new_status = response.value().as_string();

            if (new_status == "finished") {
            }
          } else {
            BOOST_LOG_TRIVIAL(error) << "Error watching block execution: "
                                     << response.error_message();
          }
        });
    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }

  void watchNextBlock(const std::string& leader_id, const std::string& term_no,
                      int current_block_num) {
    int next_block_num = current_block_num + 1;
    std::string next_block_status_key = leader_id + "/" + term_no + "/" +
                                        std::to_string(next_block_num) + "/run";

    etcd::Watcher watcher(
        etcdClient, next_block_status_key,
        [this, next_block_num](etcd::Response response) {
          if (response.is_ok() && response.action() == "set") {
            std::string new_status = response.value().as_string();

            if (new_status == "running") {
            }
          } else {
            BOOST_LOG_TRIVIAL(error) << "Error watching next block execution: "
                                     << response.error_message();
          }
        });
  }

  void watchComponentChanges(const std::string& leader_id,
                             const std::string& term_no, int block_num) {
    std::string components_key = leader_id + "/" + term_no + "/" +
                                 std::to_string(block_num) + "/components";

    etcd::Watcher watcher(
        etcdClient, components_key, [this, block_num](etcd::Response response) {
          if (response.is_ok() && response.action() == "set") {
            std::string new_components_data = response.value().as_string();

            int nodeNum = stoi(node_id.substr(1));
            Scheduler.ExtractNewComponents(new_components_data, nodeNum);

          } else {
            BOOST_LOG_TRIVIAL(error) << "Error watching component changes: "
                                     << response.error_message();
          }
        });
  }

  // 3. Data Extraction

  // Function to retrieve and extract the DAG from ETCD
  void getDAG(const std::string& leader_id, const std::string& term_no,
              int block_num) {
    std::string dag_key =
        leader_id + "/" + term_no + "/" + std::to_string(block_num) + "/DAG";

    try {
      etcd::Response response = etcdClient.get(dag_key).get();

      if (!response.is_ok()) {
        BOOST_LOG_TRIVIAL(error)
            << "Error retrieving DAG data: " << response.error_message();
        return;
      }

      std::string dag_data = response.value().as_string();

      // Extract the DAG using the scheduler
      Scheduler.extractDAG(dag_data);
      if (Scheduler.dag.totalTxns > 0) {
      } else {
        BOOST_LOG_TRIVIAL(error)
            << "DAG extraction failed or empty for block " << block_num << ".";
      }

    } catch (const std::exception& e) {
      BOOST_LOG_TRIVIAL(error) << "Exception while fetching DAG: " << e.what();
    }
  }

  // Function to retrieve and extract components using scheduler
  void getComponents(const std::string& leader_id, const std::string& term_no,
                     int block_num) {
    std::string components_key = leader_id + "/" + term_no + "/" +
                                 std::to_string(block_num) + "/components";

    try {
      etcd::Response response = etcdClient.get(components_key).get();

      if (!response.is_ok()) {
        BOOST_LOG_TRIVIAL(error)
            << "Error retrieving components: " << response.error_message();
        return;
      }

      std::string serialized_data = response.value().as_string();
      // deserializeAndPrintComponents(serialized_data);

      // Use scheduler's ExtractComponents method
      int nodeNum = stoi(node_id.substr(1));
      Scheduler.ExtractComponents(serialized_data, nodeNum);

    } catch (const std::exception& e) {
      BOOST_LOG_TRIVIAL(error)
          << "Exception while fetching components: " << e.what();
    }
  }

  // Function to retrieve block data
  string readBlock(const std::string& leader_id, const std::string& term_no,
                   int block_num) {
    std::string block_key =
        leader_id + "/" + term_no + "/" + std::to_string(block_num) + "/block";

    try {
      etcd::Response response = etcdClient.get(block_key).get();

      if (!response.is_ok()) {
        BOOST_LOG_TRIVIAL(error)
            << "Error retrieving block: " << response.error_message();
      }

      std::string block_data = response.value().as_string();

      Scheduler.extractBlock(block_data);
      return block_data;

    } catch (const std::exception& e) {
      BOOST_LOG_TRIVIAL(error)
          << "Exception during block retrieval: " << e.what();
    }
    return "";
  }

  // Function to execute transactions from a block
  bool executeTransactions(const std::string& leader_id,
                           const std::string& term_no, int block_num,
                           int thCount) {
    bool success;
    // Step 1: Execute the DAG transactions in parallel

    success = Scheduler.scheduleTxns(leader_id, term_no, block_num, thCount);
    if (success) {
    } else {
      BOOST_LOG_TRIVIAL(error)
          << "Transaction execution failed for block " << block_num << ".";
    }
    return success;
  }

  bool markComponentAsDone(const std::string& leader_id,
                           const std::string& term_no, int block_num,
                           int component_id) {
    std::string key = leader_id + "/" + term_no + "/" +
                      std::to_string(block_num) + "/status/" +
                      std::to_string(component_id);
    try {
      etcd::Response response = etcdClient.put(key, "done").get();
      if (!response.is_ok()) {
        BOOST_LOG_TRIVIAL(error)
            << "Failed to mark component as done: " << response.error_message();
        return false;
      }
      BOOST_LOG_TRIVIAL(info)
          << "Component " << component_id << " marked as done.";
          cout<< "Component " << component_id << " marked as done.";
      return true;
    } catch (const std::exception& e) {
      BOOST_LOG_TRIVIAL(error)
          << "Exception marking component done: " << e.what();
      return false;
    }
  }
  void watchLeaderCrash() {
    std::string watch_key = "CurrentLeader";
    std::string initial_leader = Leader;

    BOOST_LOG_TRIVIAL(info)
        << "Watching for leader crash on key: " << watch_key;

    std::thread([this, watch_key, initial_leader]() {
      etcd::Watcher watcher(
          etcdClient, watch_key,
          [this, initial_leader](etcd::Response response) {
            if (!response.is_ok()) {
              BOOST_LOG_TRIVIAL(error)
                  << "Leader watch failed: " << response.error_message();
              return;
            }

            if (response.action() == "delete") {
              BOOST_LOG_TRIVIAL(info)
                  << "Leader crashed or stepped down! Stopping execution.";
              Scheduler.flag.store(false);
              leaderCrashed.store(true);
              return;
            }

            std::string new_leader = response.value().as_string();
            if (new_leader != initial_leader) {
              BOOST_LOG_TRIVIAL(info)
                  << "Leader changed from " << initial_leader << " to "
                  << new_leader << ". Stopping execution.";
              Scheduler.flag.store(false);
              leaderCrashed.store(true);
              return;
            }
          });

      // Instead of fixed sleep, poll stop flag
      while (!stopWatcher.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }).detach();
  }

  string startBlock(const std::string& leader_id, const std::string& term_no,
                    int block_num, int thCount, int clusterSize) {
    
    stopWatcher.store(false);  // Reset before starting
    // cout << "follower started: "<<leader_id<<" "<<term_no<<" "<<block_num<<endl;
    watchLeaderCrash();  // Start watching
    auto start = std::chrono::high_resolution_clock::now();
    string serializedBlock = readBlock(leader_id, term_no, block_num);
    BOOST_LOG_TRIVIAL(info) << "dag is created";
    // cout << "dag is created:"<<Scheduler.dag.totalTxns;

    if (Scheduler.dag.totalTxns <= 0 || Scheduler.dag.totalTxns > 10000) {
      BOOST_LOG_TRIVIAL(error) << "Error: Invalid DAG transaction count: "
                               << Scheduler.dag.totalTxns;
      stopWatcher.store(true);  // Stop watcher early
      return "";
    }
    try {
      getComponents(leader_id, term_no, block_num);

      auto exeS = std::chrono::high_resolution_clock::now();

      watchComponentChanges(leader_id, term_no, block_num);
      bool flag = executeTransactions(leader_id, term_no, block_num, thCount);
      auto exeE = std::chrono::high_resolution_clock::now();
      etcd::Response response = etcdClient
                                    .get(leader_id + "/" + term_no + "/" +
                                         std::to_string(block_num) + "/commit")
                                    .get();
      if (!flag) {
        BOOST_LOG_TRIVIAL(error) << "Execution failed";
        return serializedBlock;
      }
      if (flag && response.value().as_string() == "1") {
        saveData(leader_id + "/" + term_no + "/" + std::to_string(block_num),
                 clusterSize);

      } else {
        state.resetTree();
      }

      // db.storeBlock("B" + to_string(header.block_num()), serializedBlock);
      stopWatcher.store(true);
      auto end = std::chrono::high_resolution_clock::now();
      auto dura3 =
          std::chrono::duration_cast<std::chrono::milliseconds>(exeS - start)
              .count();
      BOOST_LOG_TRIVIAL(info)
          << "Time for geting DAG and block: " << dura3 << " ms";
      auto dura4 =
          std::chrono::duration_cast<std::chrono::milliseconds>(exeE - exeS)
              .count();
      BOOST_LOG_TRIVIAL(info) << "Time for total execution: " << dura4 << " ms";
      auto dura5 =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - exeE)
              .count();
      BOOST_LOG_TRIVIAL(info)
          << "Time for storing the values: " << dura5 << " ms";
      auto dura6 =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
              .count();
      BOOST_LOG_TRIVIAL(info) << "Time for total block: " << dura6 << " ms";
    } catch (const std::exception& e) {
      BOOST_LOG_TRIVIAL(error)
          << "Exception during block execution: " << e.what();
    }
    return serializedBlock;
  }

  void deserializeAndPrintComponents(const std::string& binaryData) {
    components::componentsTable table;
    if (table.ParseFromString(binaryData)) {
      for (const auto& comp : table.componentslist()) {
        for (const auto& txn : comp.transactionlist()) {
        }
      }
    } else {
      BOOST_LOG_TRIVIAL(error) << "Failed to parse componentsTable proto";
    }
  }

  void deserializeAndPrintDAG(const std::string& binaryData) {
    matrix::DirectedGraph graph;
    if (graph.ParseFromString(binaryData)) {
      for (const auto& row : graph.adjacencymatrix()) {
        for (int edge : row.edges()) {
        }
      }
    } else {
      BOOST_LOG_TRIVIAL(error) << "Failed to parse DirectedGraph proto";
    }
  }

  std::string fetchAndParseKeys(const std::string& path, int i,
                                GlobalState& tmp) {
    std::string etcdKey = path + "/data/s" + std::to_string(i);
    etcd::Response response = etcdClient.get(etcdKey).get();

    if (!response.is_ok()) {
      BOOST_LOG_TRIVIAL(error)
          << "Failed to fetch data from etcd at: " << etcdKey;
      return "";
    }

    std::string data = response.value().as_string();
    std::istringstream ss(data);
    std::string pair;
    std::string updatedKeys;

    while (std::getline(ss, pair, ';')) {
      auto delimiterPos = pair.find('=');
      if (delimiterPos == std::string::npos) continue;

      std::string key = pair.substr(0, delimiterPos);
      std::string value = pair.substr(delimiterPos + 1);
      tmp.insert(key, value);
      // Value can be parsed too if needed, but we're just collecting keys
      updatedKeys += key + " ";
    }

    return updatedKeys;
  }


void saveData(const std::string& path, int clusterSize) {
    std::vector<std::thread> threads;
    GlobalState state;
    std::vector<std::string> results(clusterSize);

    for (int i = 0; i < clusterSize; ++i) {
        threads.emplace_back(
            [&, i]() { results[i] = fetchAndParseKeys(path, i, state); });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::string allUpdatedKeys;
    for (const auto& result : results) {
        allUpdatedKeys += result;
    }
    tmp.updateTree(allUpdatedKeys);
    state.replaceWith("globalState_tmp");
    rocksdb::DestroyDB("globalState_tmp", rocksdb::Options());
    std::filesystem::remove_all("globalState_tmp");
  }
};