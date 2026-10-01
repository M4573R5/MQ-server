#include "mq_server.hpp"
#include <iostream>
using namespace std;

int main() {
    const int PORT = 9099;
    messaging::MessageBroker broker(PORT);

    if (!broker.start()) {
        cerr << "Failed to start broker on port "<< PORT;
        return 1;
    }

    broker.run();
    return 0;
}
