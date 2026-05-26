cmake_minimum_required(VERSION 3.20)
project(Kafka_Demo)

set(CMAKE_CXX_STANDARD 20)

set(RDKAFKA_BUILD_TESTS    OFF CACHE BOOL "" FORCE)
set(RDKAFKA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(RDKAFKA_BUILD_STATIC   ON  CACHE BOOL "" FORCE)

add_subdirectory(external/librdkafka)

# Producer
add_executable(producer src/producer.cpp)
target_include_directories(producer PRIVATE external/librdkafka/src-cpp)
target_link_libraries(producer PRIVATE rdkafka++ rdkafka)

# Consumer
add_executable(consumer src/consumer.cpp)
target_include_directories(consumer PRIVATE external/librdkafka/src-cpp)
target_link_libraries(consumer PRIVATE rdkafka++ rdkafka)