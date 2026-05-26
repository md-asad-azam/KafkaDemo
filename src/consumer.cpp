#include <iostream>
#include <string>
#include <csignal>
#include <rdkafkacpp.h>

static volatile bool running = true;

void sigint_handler(int) {
    running = false;
}

int main() {
    std::string brokers  = "localhost:29092,localhost:39092,localhost:49092";
    std::string topic    = "test-topic";
    std::string group_id = "my-consumer-group";
    std::string errstr;

    signal(SIGINT, sigint_handler);

    // Config
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("group.id",          group_id, errstr);
    conf->set("auto.offset.reset", "earliest", errstr); // read from beginning

    // Consumer
    RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!consumer) {
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        return 1;
    }
    delete conf;

    // Subscribe
    consumer->subscribe({topic});
    std::cout << "Consuming from topic: " << topic << std::endl;

    // Poll loop
    while (running) {
        RdKafka::Message *msg = consumer->consume(1000); // 1s timeout

        switch (msg->err()) {
            case RdKafka::ERR_NO_ERROR:
                std::cout
                    << "Received [partition " << msg->partition()
                    << " offset "             << msg->offset()
                    << " key " << msg->key()
                    << " Broker: " << msg->broker_id()<< "]: "
                    << std::string(static_cast<const char *>(msg->payload()), msg->len())
                    << std::endl;
                break;

            case RdKafka::ERR__TIMED_OUT:
                break; // normal, just no messages

            case RdKafka::ERR__PARTITION_EOF:
                std::cout << "Reached end of partition" << std::endl;
                break;

            default:
                std::cerr << "Consumer error: " << msg->errstr() << std::endl;
                running = false;
                break;
        }

        delete msg;
    }

    consumer->close();
    delete consumer;

    return 0;
}