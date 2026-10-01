#pragma once

#include <string>
#include <string_view>
#include <queue>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
using namespace std;
namespace messaging {
    class ThreadSafeQueue {
        private:
            queue<string> queue_data;
            mutable mutex queue_mutex;
            condition_variable cond_var;

        public:
            void push(string_view message);
            string pop();
            bool empty() const;
    };

    class MessageBroker {
        private:
            int server_fd{-1};
            int port;
            unordered_map<string, ThreadSafeQueue> topics;
            mutex topics_mutex;

            void handle_client(int client_socket);
            ThreadSafeQueue& get_or_create_topic(const string& topic_name);

        public:
            explicit MessageBroker(int broker_port);
            ~MessageBroker();

            MessageBroker(const MessageBroker&) = delete;
            MessageBroker& operator=(const MessageBroker&) = delete;

            bool start();
            void run();
    };
}
