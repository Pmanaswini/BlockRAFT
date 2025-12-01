#pragma once
#include <etcd/Client.hpp>
#include <string>
#include <iostream>
#include <unistd.h>
#include <arpa/inet.h>
#include <arpa/inet.h>
#include <errno.h>
#include <thread>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include "addressList.pb.h"
inline std::string node_ip, node_id;
inline etcd::Client etcdClient("http://127.0.0.1:2379");
inline etcd::Client etcdClientWallet("http://127.0.0.1:2379");

atomic<int> dataCounter;
ssize_t send_all(int fd, const char* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = send(fd, buf + total, len - total, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        total += static_cast<size_t>(n);
    }
    return static_cast<ssize_t>(total);
}

// Helper: recv exactly n bytes (loops until all received or EOF/error)
ssize_t recv_all(int fd, char* buf, size_t len) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = recv(fd, buf + total, len - total, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) { // peer closed
            break;
        }
        total += static_cast<size_t>(n);
    }
    return static_cast<ssize_t>(total);
}

// Try to read a 4-byte big-endian length then payload.
// Returns true on success. If returns false, caller may fallback to read_until_close.
bool read_length_prefixed_message(int fd, std::string &out) {
    uint32_t len_be = 0;
    ssize_t n = recv_all(fd, reinterpret_cast<char*>(&len_be), sizeof(len_be));
    if (n <= 0) return false;
    if (static_cast<size_t>(n) < sizeof(len_be)) return false;
    uint32_t len = ntohl(len_be);

    // Optional safety: reject absurdly large sizes (adjust max as needed)
    const uint32_t MAX_ALLOWED = 50u * 1024u * 1024u; // 50 MB
    if (len > MAX_ALLOWED) {
        std::cerr << "Rejected message length " << len << " (exceeds max)\n";
        return false;
    }

    out.resize(len);
    ssize_t m = recv_all(fd, &out[0], len);
    if (m < 0 || static_cast<uint32_t>(m) != len) return false;
    return true;
}

// Fallback: read until peer closes connection (used for older plain sends)
bool read_until_close(int fd, std::string &out) {
    const size_t CHUNK = 64 * 1024;
    std::vector<char> buf(CHUNK);
    out.clear();
    while (true) {
        ssize_t n = recv(fd, buf.data(), buf.size(), 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) break; // closed
        out.append(buf.data(), static_cast<size_t>(n));
    }
    return true;
}

std::string MergeSerializedAddressLists(vector<std::string>& serializedLists)
{
    addressList::AddressValueList finalList;

    for (size_t i = 0; i < serializedLists.size(); ++i) {
        const std::string& blob = serializedLists[i];

        addressList::AddressValueList temp;
        if (!temp.ParseFromString(blob)) {
            std::cerr << "Failed to parse protobuf element at index " << i << std::endl;
            return "";   // Abort: one or more blobs is invalid
        }

        // Append all AddressValue entries
        for (const auto& entry : temp.pairs()) {
            *finalList.add_pairs() = entry;
        }
    }

    // Serialize final combined result
    std::string output;
    if (!finalList.SerializeToString(&output)) {
        std::cerr << "Failed to serialize merged AddressValueList\n";
        return "";
    }

    return output;
}

void server_loop(int port, int thread_id,int clusterSize, std::vector<std::string>* sharedStrings) {
    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Thread " << thread_id << " - socket() failed: " << strerror(errno) << "\n";
        return;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "Thread " << thread_id << " - bind(" << port << ") failed: "
                  << strerror(errno) << "\n";
        close(server_fd);
        return;
    }

    if (listen(server_fd, 10) < 0) {
        std::cerr << "Thread " << thread_id << " - listen() failed: " << strerror(errno) << "\n";
        close(server_fd);
        return;
    }

    std::cout << "Thread " << thread_id << " listening on port " << port << "...\n";

    // Process exactly one client/message then terminate
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (client_fd < 0) {
        std::cerr << "Thread " << thread_id << " - accept() failed: "
                  << strerror(errno) << "\n";
        close(server_fd);
        return;
    }

    // Read message: try length-prefixed first; if that fails, fallback to reading until close
    std::string received;
    bool ok = read_length_prefixed_message(client_fd, received);
    if (!ok) {
        // Attempt fallback (old clients that send plain data then close)
        if (!read_until_close(client_fd, received)) {
            std::cerr << "Thread " << thread_id << " - failed to read message\n";
            close(client_fd);
            close(server_fd);
            return;
        }
    }

    std::cout << "Thread " << thread_id << " - Received on port "
              << port << " (" << received.size() << " bytes): ";
              (*sharedStrings)[thread_id] =received;
              dataCounter++;
    if (received.size() <= 200) {
        std::cout << received << "\n";
    } else {
        std::cout << "<" << received.size() << " bytes>\n";
    }
    while(dataCounter<clusterSize-1){}
    // Prepare reply "goodmrg" using length-prefixed framing
    
    const std::string reply = MergeSerializedAddressLists(*sharedStrings);
    // Example large reply: 300,000 bytes
    // std::string reply(300000, 'Z');   // Or any data you want

    uint32_t reply_len = static_cast<uint32_t>(reply.size());
    uint32_t reply_be = htonl(reply_len);

    if (send_all(client_fd, reinterpret_cast<const char*>(&reply_be), sizeof(reply_be)) < 0) {
        std::cerr << "Thread " << thread_id << " - send header failed: " << strerror(errno) << "\n";
        close(client_fd);
        close(server_fd);
        return;
    }
    if (send_all(client_fd, reply.data(), reply.size()) < 0) {
        std::cerr << "Thread " << thread_id << " - send payload failed: " << strerror(errno) << "\n";
        close(client_fd);
        close(server_fd);
        return;
    }

    // Close client and server sockets to terminate this server thread
    close(client_fd);
    close(server_fd);

    std::cout << "Thread " << thread_id << " - replied and terminating.\n";
}


void server(int id, int clusterSize) {
    
    std::vector<std::thread> threads;
    std::vector<std::string> sharedStrings(clusterSize);
    threads.reserve(clusterSize-1);

    // Start all server threads
    for (int i = 0; i < clusterSize; ++i) {
        if(id!=i){
        int port = 8080 + i;
        threads.emplace_back(server_loop, port, i,clusterSize,&sharedStrings);}
    }

    std::cout << "Started " << clusterSize << " server threads on ports "
              << 8080 << " .. " << (8080 + clusterSize - 1) << "\n";

    // JOIN all threads so main never exits
    for (auto& t : threads) {
        t.join();
    }
}


// Client: sends one length-prefixed message (handles large payloads) and reads the length-prefixed reply.
// If you need to talk to an old server that doesn't use length prefix, use the older plain send() version.
void client(int port,string payload) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return; }

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    // IP of node0 - keep your existing IP
    if (inet_pton(AF_INET, "10.227.214.187", &serv_addr.sin_addr) != 1) {
        std::cerr << "inet_pton failed\n";
        close(sock);
        return;
    }

    if (connect(sock, (sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "connect failed: " << strerror(errno) << "\n";
        close(sock);
        return;
    }

    // Example payload: replace with your big payload (300000 bytes)
    // std::string payload = "hello";
    // std::string payload(300000, 'A'); // uncomment to test 300k of 'A'

    // Send length prefix header (4 bytes big-endian)
    uint32_t len = static_cast<uint32_t>(payload.size());
    uint32_t be = htonl(len);
    if (send_all(sock, reinterpret_cast<const char*>(&be), sizeof(be)) < 0) {
        std::cerr << "send header failed: " << strerror(errno) << "\n";
        close(sock);
        return;
    }

    // Send payload (looped)
    if (!payload.empty()) {
        if (send_all(sock, payload.data(), payload.size()) < 0) {
            std::cerr << "send payload failed: " << strerror(errno) << "\n";
            close(sock);
            return;
        }
    }

    // Read length-prefixed reply from server
    uint32_t reply_be = 0;
    ssize_t n = recv_all(sock, reinterpret_cast<char*>(&reply_be), sizeof(reply_be));
    if (n <= 0 || static_cast<size_t>(n) < sizeof(reply_be)) {
        std::cerr << "failed to read reply header\n";
        close(sock);
        return;
    }
    uint32_t reply_len = ntohl(reply_be);
    std::string reply;
    reply.resize(reply_len);
    ssize_t m = recv_all(sock, &reply[0], reply_len);
    if (m < 0 || static_cast<uint32_t>(m) != reply_len) {
        std::cerr << "failed to read full reply\n";
        close(sock);
        return;
    }

    std::cout << "Client received reply (" << reply_len << " bytes): " << "\n";

    close(sock);
}