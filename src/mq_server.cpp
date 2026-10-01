#include "mq_server.hpp"
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

using namespace std;

namespace messaging {

    void ThreadSafeQueue::push(string_view message) {
        lock_guard<mutex> lock(queue_mutex);
        queue_data.push(string(message));
        cond_var.notify_one();
    }

    string ThreadSafeQueue::pop() {
        unique_lock<mutex> lock(queue_mutex);
        cond_var.wait(lock, [this]() { return !queue_data.empty(); });
        
        string msg = move(queue_data.front());
        queue_data.pop();
        return msg;
    }

    bool ThreadSafeQueue::empty() const {
        lock_guard<mutex> lock(queue_mutex);
        return queue_data.empty();
    }

    MessageBroker::MessageBroker(int broker_port) : port(broker_port) {}
    MessageBroker::~MessageBroker() {
        if (server_fd != -1) close(server_fd);
    }

    ThreadSafeQueue& MessageBroker::get_or_create_topic(const string& topic_name) {
        lock_guard<mutex> lock(topics_mutex);
        return topics[topic_name];
    }

    bool MessageBroker::start() {
        server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd == -1) return false;

        int opt = 1;
        setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(port);

        if (bind(server_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) return false;
        if (listen(server_fd, 10) < 0) return false;

        cout << "broker listening on port " << port << endl;
        return true;
    }

    void MessageBroker::run() {
        while (true) {
            int client_socket = accept(server_fd, nullptr, nullptr);
            if (client_socket >= 0) {
                jthread(&MessageBroker::handle_client, this, client_socket).detach(); // Spin off processing thread
            }
        }
    }

    void MessageBroker::handle_client(int client_socket) {
        vector<char> buffer(4096, 0);
        ssize_t bytes_read = read(client_socket, buffer.data(), buffer.size() - 1);

        if (bytes_read > 0) {
            string raw_command(buffer.data(), bytes_read);
            stringstream ss(raw_command);
            string action, topic_name, payload;

            ss >> action >> topic_name;
            getline(ss, payload); 

            if (!payload.empty() && payload[0] == ' ') payload.erase(0, 1);

            if (action == "PUB") {
                auto& target_queue = get_or_create_topic(topic_name);
                target_queue.push(payload);
                string ack = "[ACK]: Message published to " + topic_name + "\n";
                write(client_socket, ack.data(), ack.length());
                cout << "[V]* Published to '" << topic_name << "': " << payload << endl;
            } 
            else if (action == "SUB") {
                auto& target_queue = get_or_create_topic(topic_name);
                cout << "[V]* Consumer connected to channel. Streaming messages on topic '" << topic_name << "'" << endl;
                
                while (true) {
                    string message = target_queue.pop(); 
                    string out = "MSG: " + message + "\n";
                    
                    if (write(client_socket, out.data(), out.length()) < 0) {
                        cout << "[V]* Consumer disconnected from topic '" << topic_name << "'." << endl;
                        break;
                    }
                }
            }
            else {
                string err = "ERR: Unknown action protocol. Use PUB or SUB.\n";
                write(client_socket, err.data(), err.length());
            }
        }
        close(client_socket);
    }
}
