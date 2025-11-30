#pragma once
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
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
#include "../leader/testingBlockProducer.h"
#include "../merkleTree/globalState.h"

string etcdPort = "http://127.0.0.1:2379";
using namespace std;

class leader {
 public:
  static std::thread watcherThread;
  static std::unique_ptr<etcd::Watcher> watcher;
  static std::atomic<bool> stopMonitor;
  static atomic<int> componentCount;

  std::vector<int> healthyMemberIds;

  struct EtcdMember {
    std::string ipAddress;
    std::string id;
    bool healthy;
  };
  std::vector<EtcdMember> memberList;
  blocksDB db;
  DAGmodule DAGObj;
  components::componentsTable table;
  int activeFollowers;

  string executeCommand(const std::string& command) {
    char buffer[128];
    std::string result = "";

    // Open pipe to file
    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe) {
      return "popen failed!";
    }

    // Read till end of process:
    while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
      result += buffer;
    }

    // Close the pipe and check if there was an error
    int status = pclose(pipe);
    if (status == -1) {
      return "Error closing the pipe!";
    }

    return result;
  };

  void getEtcdMembers() {
    etcd::Response res = etcdClient.list_member().get();
    for (int i = 0; i < res.members().size(); i++) {
      memberList.push_back(
          {res.members()[i].get_clientURLs()[0], res.members()[i].get_name()});
    }
  }
  int getActiveFollowers() {
    string output, word;
    int count = 0;
    bool health;
    output = executeCommand("etcdctl endpoint health --cluster 2>&1");
    std::istringstream stream(output);
    std::string ip;
    bool nextIsIP = false;

    while (stream >> word) {
      if (word.substr(0, 7) == "http://" &&
          word.substr(7) != node_ip + ":2379") {
        ip = word;
        nextIsIP = true;
      }
      if (nextIsIP && word == "healthy:") {
        // Find the member in the list and mark as healthy
        for (auto& member : memberList) {
          if (member.ipAddress == ip) {
            member.healthy = true;
            break;  // No need to check further
          }
        }
        count++;  // Increment count for a healthy node
        nextIsIP = false;
      }
    }
    return count;
  };

  void monitorComponents(string compKey) {
    int count;
    bool flag;
    while (!stopMonitor.load()) {
      count = getActiveFollowers();

      if (count < activeFollowers && count > 0) {
        activeFollowers = count;
        for (int i = 0; i < table.componentslist_size(); ++i) {
          components::componentsTable::component* comp =
              table.mutable_componentslist(i);
          int follower = comp->assignedfollower();
          if (memberList[follower].healthy == false) {
            int index = i % activeFollowers;
            comp->set_assignedfollower(healthyMemberIds[index]);
            flag = true;
          }
        }
        if (flag) {
          std::string serializedTable;
          if (!table.SerializeToString(&serializedTable)) {
            return;
          }
          etcd::Response response =
              etcdClient.set(compKey, serializedTable).get();
        }
      }
    }
  }

  bool assignFollowers(string compKey) {
    etcd::Response response;
    for (const auto& member : memberList) {
      if (member.healthy) {
        int id = std::stoi(member.id.substr(1));
        healthyMemberIds.push_back(id);
      }
    }
    activeFollowers = healthyMemberIds.size();
    for (int i = 0; i < componentCount.load(std::memory_order_relaxed); ++i) {
      components::componentsTable::component* comp =
          table.mutable_componentslist(i);
      if (activeFollowers > 0) {
        // Assign a healthy follower in a round-robin fashion
        int index = i % activeFollowers;
        comp->set_assignedfollower(healthyMemberIds[index]);
      } else {
        return false;
      }
    }
    std::string serializedTable;
    if (!table.SerializeToString(&serializedTable)) {
      return false;  // Serialization failed
    }
    response = etcdClient.set(compKey, serializedTable).get();
    return true;
  }

  void rtrim(std::string& str) {
    while (!str.empty() && str.back() == '\n') {
      str.pop_back();
    }
  }
  bool checkComponents(const std::string& compStatusPath, int txnCount) {
    while (true) {
      int completed = 0;
      for (int i = 0; i < memberList.size(); ++i) {
        std::string key = compStatusPath + "/s" + std::to_string(i);
        etcd::Response response = etcdClient.get(key).get();
        if (response.is_ok()) {
          int count = stoi(response.value().as_string());
          completed = completed + count;
        } else {
        }
      }

      if (txnCount == completed) {
        BOOST_LOG_TRIVIAL(info) << "All components are done.";
        stopMonitor.store(true);
        return true;
      }
    }
  }

  bool nodeDetails() {
    node_ip = executeCommand("hostname -I | awk '{print $1}'");
    rtrim(node_ip);
    etcd::Response response = etcdClient.list_member().get();
    for (size_t i = 0; i < response.members().size(); ++i) {
      const auto& member = response.members()[i];
      for (const std::string& url : member.get_clientURLs()) {
        if (url.find(node_ip) != std::string::npos) {
          node_id = member.get_name();
          return true;
        }
      }
    }
    return false;
  }

  std::string fetchAndParseKeys(const std::string& path, int i,
                                GlobalState& state) {
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
      state.insert(key, value);
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

    // Update the global state tree
    state.updateTree(allUpdatedKeys);
}

bool leaderProtocol(string raftTerm, int txnCount, int thCount, string mode, int count) {
  etcd::Response response;
  std::string serializedBlock, serializedComp;
  thread componentsMonitor;
  BlockHeader header;
  Block latestBlock;
  TestBlockProducer producer;

  // Timing points
  auto start = std::chrono::high_resolution_clock::now();
  auto blockC = start;
  auto dagS = start;
  auto dagC = start;
  auto compC = start;
  auto exeS = start;
  auto exeE = start;
  auto end = start;

  bool DAG = false;

  // Block generation
  if (count % 2 == 0) {
      BOOST_LOG_TRIVIAL(info) << "Setup File is running (even count)." << count;
      cout << "Setup File is running (even count)." << count;
      latestBlock = producer.produce(db, txnCount, "../leader/setupFile.txt");
  } else {
      BOOST_LOG_TRIVIAL(info) << "Test File is running (odd count)." << count;
      cout<< "Test File is running (odd count)." << count;
      latestBlock = producer.produce(db, txnCount, "../leader/testFile.txt");
  }

  blockC = std::chrono::high_resolution_clock::now();

  if (latestBlock.transactions_size() == 0) {
      BOOST_LOG_TRIVIAL(warning) << "Empty block encountered.";
      return false;
  }

  std::string base_path, blockKey, compKey, runKey, commitKey;

  std::thread t1([&]() {
      if (!latestBlock.SerializeToString(&serializedBlock)) {
          BOOST_LOG_TRIVIAL(error) << "Failed to serialize the block.";
          return;
      }

      std::string headerString = latestBlock.header();
      if (!header.ParseFromString(headerString)) {
          BOOST_LOG_TRIVIAL(error) << "Failed to parse block header.";
          return;
      }

      getEtcdMembers();
      getActiveFollowers();

      base_path = node_id + "/" + raftTerm + "/" + to_string(header.block_num());
      blockKey = base_path + "/block";
      compKey = base_path + "/components";
      runKey = base_path + "/run";
      commitKey = base_path + "/commit";
      cout<<"Base path: "<<base_path<<endl;

      auto etcd_block_start = std::chrono::high_resolution_clock::now();
      response = etcdClient.set(blockKey, serializedBlock).get();
      auto etcd_block_end = std::chrono::high_resolution_clock::now();

      BOOST_LOG_TRIVIAL(info)
          << "ETCD write (block): "
          << std::chrono::duration_cast<std::chrono::milliseconds>(etcd_block_end - etcd_block_start).count()
          << " ms";
          auto etcd_run_wait_start = std::chrono::high_resolution_clock::now();
          response = etcdClient.set(runKey, "wait").get();
          auto etcd_run_wait_end = std::chrono::high_resolution_clock::now();
    
          BOOST_LOG_TRIVIAL(info)
              << "ETCD write (run=wait): "
              << std::chrono::duration_cast<std::chrono::milliseconds>(etcd_run_wait_end - etcd_run_wait_start).count()
              << " ms";
    
          auto etcd_commit0_start = std::chrono::high_resolution_clock::now();
          response = etcdClient.set(commitKey, (mode == "validation") ? "1" : "0").get();
          auto etcd_commit0_end = std::chrono::high_resolution_clock::now();
    
          BOOST_LOG_TRIVIAL(info)
              << "ETCD write (commit=0/1): "
              << std::chrono::duration_cast<std::chrono::milliseconds>(etcd_commit0_end - etcd_commit0_start).count()
              << " ms";
    
  });
  
  std::thread t2([&]() {
      dagS = std::chrono::high_resolution_clock::now();
      DAG = DAGObj.create(latestBlock, thCount);
      txnCount = DAGObj.totalTxns;
      cout<<"Total Txns: "<<DAGObj.totalTxns<<endl;
      dagC = std::chrono::high_resolution_clock::now();

      table = DAGObj.connectedComponents();
      compC = std::chrono::high_resolution_clock::now();

      componentCount.store(table.componentslist_size(), std::memory_order_relaxed);
  });

  t1.join();
  t2.join();

  if (DAG) {
      BOOST_LOG_TRIVIAL(info) << base_path;

      componentsMonitor = std::thread(&leader::monitorComponents, this, compKey);


      if (!assignFollowers(compKey)) return false;

      exeS = std::chrono::high_resolution_clock::now();

      auto etcd_run_start_start = std::chrono::high_resolution_clock::now();
      response = etcdClient.set(runKey, "start").get();
      auto etcd_run_start_end = std::chrono::high_resolution_clock::now();

      BOOST_LOG_TRIVIAL(info)
          << "ETCD write (run=start): "
          << std::chrono::duration_cast<std::chrono::milliseconds>(etcd_run_start_end - etcd_run_start_start).count()
          << " ms";

      checkComponents(compKey + "/status", txnCount);

      auto etcd_run_finish_start = std::chrono::high_resolution_clock::now();
      response = etcdClient.set(runKey, "finish").get();
      auto etcd_run_finish_end = std::chrono::high_resolution_clock::now();

      BOOST_LOG_TRIVIAL(info)
          << "ETCD write (run=finish): "
          << std::chrono::duration_cast<std::chrono::milliseconds>(etcd_run_finish_end - etcd_run_finish_start).count()
          << " ms";

      auto etcd_commit1_start = std::chrono::high_resolution_clock::now();
      response = etcdClient.set(commitKey, "1").get();
      auto etcd_commit1_end = std::chrono::high_resolution_clock::now();

      BOOST_LOG_TRIVIAL(info)
          << "ETCD write (commit=1): "
          << std::chrono::duration_cast<std::chrono::milliseconds>(etcd_commit1_end - etcd_commit1_start).count()
          << " ms";

      exeE = std::chrono::high_resolution_clock::now();

      saveData(base_path, memberList.size());
      db.storeBlock("B" + to_string(header.block_num()), serializedBlock);

      end = std::chrono::high_resolution_clock::now();

      // Correct timing logs
      BOOST_LOG_TRIVIAL(info) << "Time for block production: " 
          << std::chrono::duration_cast<std::chrono::milliseconds>(blockC - start).count() << " ms";
      BOOST_LOG_TRIVIAL(info) << "Time for DAG creation: " 
          << std::chrono::duration_cast<std::chrono::milliseconds>(dagC - dagS).count() << " ms";
      BOOST_LOG_TRIVIAL(info) << "Time for component detectin: " 
          << std::chrono::duration_cast<std::chrono::milliseconds>(compC - dagC).count() << " ms";
      BOOST_LOG_TRIVIAL(info) << "Time for assigning followers: " 
          << std::chrono::duration_cast<std::chrono::milliseconds>(exeS - compC).count() << " ms";
      BOOST_LOG_TRIVIAL(info) << "Time for total execution: " 
          << std::chrono::duration_cast<std::chrono::milliseconds>(exeE - exeS).count() << " ms";
      BOOST_LOG_TRIVIAL(info) << "Time for storing the values: " 
          << std::chrono::duration_cast<std::chrono::milliseconds>(end - exeE).count() << " ms";
      BOOST_LOG_TRIVIAL(info) << "Time for leader protocol: " 
          << std::chrono::duration_cast<std::chrono::milliseconds>(end - blockC).count() << " ms";

      componentsMonitor.join();
      DAGObj.dagClean();
      componentCount.store(0, std::memory_order_relaxed);
      table.Clear();
      healthyMemberIds.clear();
      memberList.clear();

      if (count % 2 == 0) {
          GlobalState state;
          state.resetTree();
      }

      return true;
  }

  return false;
}

};