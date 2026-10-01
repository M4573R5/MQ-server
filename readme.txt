g++ -std=c++20 -Iinclude src/main.cpp src/mq_server.cpp -pthread -o mq-server

>> ./mq-server 

Test on terminal:
Terminal 1:
>>  

Terminal 2:
>> echo "PUB <topc_name> BROWN_SUGAR" | nc localhost 9099
