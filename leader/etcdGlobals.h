#pragma once
#include <etcd/Client.hpp>
#include <string>
#include <iostream>
#include <unistd.h>
#include <arpa/inet.h>

inline std::string node_ip, node_id;
inline etcd::Client etcdClient("http://127.0.0.1:2379");
inline etcd::Client etcdClientWallet("http://127.0.0.1:2379");


void server_loop(int port, int thread_id) {
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
    int msgCount=1;
    while (msgCount) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            std::cerr << "Thread " << thread_id << " - accept() failed: "
                      << strerror(errno) << "\n";
            continue;
        }

        char buffer[1024];
        ssize_t n = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (n > 0) {
            buffer[n] = '\0';
            std::cout << "Thread " << thread_id << " - Received on port "
                      << port << ": " << buffer << "\n";
        }

        close(client_fd);
        msgCount--;
    }

    close(server_fd);
}


void server(int id, int clusterSize) {
    
    std::vector<std::thread> threads;
    threads.reserve(clusterSize-1);

    // Start all server threads
    for (int i = 0; i < clusterSize; ++i) {
        if(id!=i){
        int port = 8080 + i;
        threads.emplace_back(server_loop, port, i);}
    }

    std::cout << "Started " << clusterSize << " server threads on ports "
              << 8080 << " .. " << (8080 + clusterSize - 1) << "\n";

    // JOIN all threads so main never exits
    for (auto& t : threads) {
        t.join();
    }
}


void client(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serv_addr{};

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    // IP of node0
    inet_pton(AF_INET, "10.227.214.187", &serv_addr.sin_addr);

    connect(sock, (sockaddr*)&serv_addr, sizeof(serv_addr));

    const char* msg = "hello";
    send(sock, msg, strlen(msg), 0);

    close(sock);
}
