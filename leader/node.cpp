
#include <cstdlib>
#include <iomanip>
#include <nlohmann/json.hpp>

#ifdef UNIT_TEST
#define MAIN_DISABLED
#endif

#include "../follower/follower.h"
#include "../leader/etcdGlobals.h"
#include "leader.h"

using namespace std;
int clusterSize;
std::thread leader::watcherThread;
std::unique_ptr<etcd::Watcher> leader::watcher;
std::atomic<bool> LLease{false}, leader::stopMonitor{false};
std::atomic<int> leader::componentCount{0};
leader leaderObj;
string raftTerm;

atomic<bool> isLeader = false, etcdHealth = true, redpandaHealth = true;
void initFileLogging() {
  boost::log::core::get()->set_filter(
    boost::log::trivial::severity >= boost::log::trivial::info
);
  boost::log::add_file_log(
      boost::log::keywords::file_name = "../experiment.log",
      boost::log::keywords::auto_flush = true,
      boost::log::keywords::open_mode = std::ios_base::app);
  boost::log::add_common_attributes();
}
string executeCommand(const std::string &command) {
  char buffer[128];
  std::string result = "";

  FILE *pipe = popen(command.c_str(), "r");
  if (!pipe) {
    return "popen failed!";
  }

  while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
    result += buffer;
  }

  int status = pclose(pipe);
  if (status == -1) {
    return "Error closing the pipe!";
  }

  return result;
}

bool isEtcdClusterHealthy(const std::string &output, int memCount) {
  std::istringstream stream(output);
  std::string line;
  int healthyCount = 0;
  std::vector<std::string> unhealthyNodes;

  while (std::getline(stream, line)) {
    if (line.find("is healthy") != std::string::npos) {
      healthyCount++;
    } else if (line.find("is unhealthy") != std::string::npos ||
               line.find("failed to commit proposal") != std::string::npos ||
               line.find("context deadline exceeded") != std::string::npos ||
               line.find("connection refused") != std::string::npos) {
      unhealthyNodes.push_back(line);
    }
  }

  int failedNodes = memCount - healthyCount;
  bool isHealthy = (failedNodes <= std::floor((memCount) / 3));

  if (!isHealthy) {
    // BOOST_LOG_TRIVIAL(error)
    //     << "[ETCD] Cluster health check failed. Healthy nodes: " << healthyCount
    //     << ", Total members: " << memCount;

    for (const auto &node : unhealthyNodes) {
      // BOOST_LOG_TRIVIAL(warning) << "[ETCD] Unhealthy node: " << node;
    }
  }

  return isHealthy;
}

void etcdMonitorFunc() {
  string output;
  int count = 1;
  bool health;
  etcd::Response response = etcdClient.list_member().get();
  int memCount = response.members().size();
  clusterSize = memCount;
  while (true) {
    output = executeCommand("etcdctl endpoint health --cluster 2>&1");
    health = isEtcdClusterHealthy(output, memCount);
    if (!health) {
      etcdHealth.store(false);
      return;
    }
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }
  return;
}

void redpandaMonitorFunc() {
  string output, word;
  while (true) {
    output = executeCommand("rpk cluster health");
    std::istringstream stream(output);
    while (stream >> word) {
      if (word == "false") {
        redpandaHealth.store(false);
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }
}

bool findLeader() {
  vector<string> values;
  string command = "etcdctl endpoint status ";
  string output = executeCommand(command);

  if (output.empty()) {
    etcdHealth.store(false);
    return false;
  }

  stringstream ss(output);
  string item;
  while (getline(ss, item, ',')) {
    item.erase(0, item.find_first_not_of(" \t"));
    item.erase(item.find_last_not_of(" \t") + 1);
    values.push_back(item);
  }

  if (values.size() < 7) {
    etcdHealth.store(false);
    return false;
  }

  etcdHealth.store(true);
  raftTerm = values[6];

  if (values[4] == "true") {
    isLeader.store(true);
    return true;
  } else {
    isLeader.store(false);
    return false;
  }
}

std::string int64ToHex(int64_t leaseID) {
  std::stringstream ss;
  ss << std::hex << leaseID;
  return ss.str();
}

void leaderTTL() {
  LLease.store(true);
  while (etcdHealth.load() && findLeader()) {
    etcd::Response resp = etcdClient.leasegrant(20).get();
    auto leaseID = resp.value().lease();
    etcd::Response response =
        etcdClient.put("CurrentLeader", node_id, leaseID).get();
    std::this_thread::sleep_for(std::chrono::seconds(18));
  }
  LLease.store(false);
}

#ifndef MAIN_DISABLED
int main() {
  initFileLogging();
  etcd::Response response;
  thread etcdMonitor, leaderLease, redpandaMonitor;
  etcdMonitor = thread(&etcdMonitorFunc);
  redpandaMonitor = thread(&redpandaMonitorFunc);
  std::ifstream configFile("../config.json");
  if (!configFile.is_open()) {
    BOOST_LOG_TRIVIAL(error) << "Failed to open config.json";
    return 1;
  }

  nlohmann::json configJson;
  configFile >> configJson;

  int threadCount = configJson["threadCount"];
  int txnCount = configJson["txnCount"];
  int count=0, blocksCount = configJson["blocks"];
  std::string schedulerMode = configJson["scheduler"];
  std::string executionMode = configJson["mode"];
  // executeCommand("etcdctl del \"\" --prefix");

  while (etcdHealth.load() && redpandaHealth.load() && count< blocksCount) {
    if (leaderObj.nodeDetails()) {
      if (findLeader()) {
        if (!LLease.load()) {
          leaderLease = thread(&leaderTTL);
        }
        leaderObj.componentCount.store(0);
        leader::stopMonitor.store(false);

        bool status = leaderObj.leaderProtocol(raftTerm, txnCount, threadCount,
                                               executionMode, count);

        if (status) {
          count++;
        }
      } else {
        // Follower branch:
        follower f;
        std::string leader_id = f.getLeaderID();  // fetch initial leader
        if (leader_id.empty()) {
          // BOOST_LOG_TRIVIAL(warning)
          //     << "Leader ID is empty. Skipping block processing.";
          continue;  // Skip this iteration if no leader is present.
        }

        std::string term_no = raftTerm;

        // 1. Watch for leader crash
        leaderCrashed.store(false);
        f.watchLeaderCrash();

        // 2. Determine latest block
        std::vector<std::string> namespaces =
            f.getNamespaces(leader_id, term_no);
        int block_num = f.getLargestBlockNumber(namespaces);
        BOOST_LOG_TRIVIAL(info) << "Largest Block: " << block_num;
        string startKey =
            leader_id + "/" + term_no + "/" + to_string(block_num) + "/run";
        etcd::Response response = etcdClient.get(startKey).get();

        if (block_num >= 0 && response.value().as_string() == "start") {
          string serializedBlock = f.startBlock(leader_id, term_no, block_num,
                                                threadCount, clusterSize);

          if (leaderCrashed.load()) {
            // BOOST_LOG_TRIVIAL(warning)
            //     << "Breaking DAG execution due to leader crash.";
            continue;  // restart the main loop
          }

          if (executionMode == "validation") {
            leaderObj.db.storeBlock("B" + to_string(block_num),
                                    serializedBlock);
          }
        } else {
          // BOOST_LOG_TRIVIAL(info) << "No blocks found to execute as follower.";
        }
      }
      BOOST_LOG_TRIVIAL(info) << "The block was successfully executed.";
    }
  }
  leaderObj.db.destroyDB();
  etcdMonitor.join();
  redpandaMonitor.join();
  if (findLeader()) {
    leaderLease.join();
  }
  return 0;
}
#endif