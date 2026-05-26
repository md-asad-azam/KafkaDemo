#include <iostream>
#include <string>
#include <rdkafkacpp.h>

int main() {
    std::string brokers = "localhost:29092,localhost:39092,localhost:49092";
    std::string topic   = "test-topic";
    std::string keys[] = {"key1", "key2", "key3"};

    std::string errstr;
    int idx=0;

    // Config
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("acks", "all", errstr); // wait for all ISR replicas

    // Producer
    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return 1;
    }
    delete conf;

    // Produce messages
    std::cout<<"The producer is running, enter your messages: \n";
    while (true) {
        std::string key = keys[idx++%3];
        std::string message;
        std::getline(std::cin, message);

        RdKafka::ErrorCode err = producer->produce(
            topic,
            RdKafka::Topic::PARTITION_UA,       // partition
            RdKafka::Producer::RK_MSG_COPY,     // copy payload
            const_cast<char *>(message.c_str()),
            message.size(),
            const_cast<char *>(key.c_str()),
            key.size(),                         // no key
            0,                                  // no timestamp
            nullptr                             // no headers
        );

        if (err != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Produce failed: " << RdKafka::err2str(err) << std::endl;
        } else {
            std::cout << "Produced: " << message << std::endl;
        }

        producer->poll(0); // serve delivery callbacks
    }

    // Wait for all messages to be delivered
    producer->flush(10000);
    delete producer;

    return 0;
}