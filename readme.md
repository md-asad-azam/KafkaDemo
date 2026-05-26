# Docker Compose Explained

- `CLUSTER_ID` - Every service shares the same cluster ID. This is how all 6 nodes know they belong to the same cluster. You generate this once with kafka-storage random-uuid and never change it.
- `KAFKA_PROCESS_ROLES` - tells that this node acts as a broker or controller or both.
  `KAFKA_PROCESS_ROLES: broker` | `KAFKA_PROCESS_ROLES: controller` | `KAFKA_PROCESS_ROLES: broker, controller`
- The controller do not store any message it only does the metadata/election.
- `KAFKA_CONTROLLER_QUORUM_VOTERS` - Each container needs to know all the eligible quorum voters.
- `KAFKA_DEFAULT_REPLICATION_FACTOR` - Here 3 means that each message will be replicated on 3 nodes(1 Leader and 2 replica).
- `KAFKA_MIN_INSYNC_REPLICAS` This works together with replication factor. It answers: "how many replicas must confirm a write before we tell the producer it succeeded?

# Kafka_Demo

A C++ Kafka producer/consumer using **librdkafka**, vendored from source, built with CMake + MSVC on Windows.

---

## Project Structure

```
Kafka_Demo/
├── CMakeLists.txt
├── src/
│   ├── producer.cpp
│   └── consumer.cpp
└── external/
    └── librdkafka/        ← although its added as a git module, I am gonna push the whole source code of the lib on my github
```

---

## Kafka Cluster

3-broker KRaft cluster (no Zookeeper) running via Docker Compose.

| Service     | Host Port |
|-------------|-----------|
| broker-1    | 29092     |
| broker-2    | 39092     |
| broker-3    | 49092     |
| Kafka UI    | 8080      |

**Start the cluster:**
```bash
docker compose up -d
```

**Stop the cluster:**
```bash
docker compose down
```

**Nuke everything including volumes (fresh start):**
```bash
docker compose down -v
```

Kafka UI: http://localhost:8080

---

## Building

**Toolchain: MSVC (Visual Studio Build Tools 2022)**
Do NOT use MinGW — librdkafka doesn't play well with it on Windows.

In CLion: `Settings → Build → Toolchains` → Visual Studio must be at the top.

```bash
# From project root
cmake -B cmake-build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build cmake-build-debug -j6
```

To fully reset the build (do this when CMake acts weird):
```bash
rm -rf cmake-build-debug
# then reconfigure in CLion or re-run cmake
```

---

## CMakeLists.txt — Key Points

```cmake
set(RDKAFKA_BUILD_STATIC   ON  CACHE BOOL "" FORCE)   # static = no DLL hunting at runtime
set(RDKAFKA_BUILD_TESTS    OFF CACHE BOOL "" FORCE)   # don't build their tests
set(RDKAFKA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)   # don't build their examples
```

Each executable lists its source file **explicitly**. Never use `file(GLOB)` — it will pull
in all `.cpp` files and cause `main already defined` linker errors.

```cmake
add_executable(producer src/producer.cpp)   # only producer.cpp
add_executable(consumer src/consumer.cpp)   # only consumer.cpp
```

Link targets are `rdkafka++` and `rdkafka` (not `rdkafka++_static` — that name doesn't exist).

---

## Vendoring a Dependency

```bash
git clone --depth 1 --branch v2.4.0 https://github.com/confluentinc/librdkafka external/librdkafka
rm -rf external/librdkafka/.git
git add external/librdkafka
```

The `.git` folder is removed so it's plain source in your repo — no submodule pointer, no network dependency at build time.

To update: delete the folder, clone the new tag, remove `.git`, commit.

---

## Producer

- Brokers: `localhost:29092,localhost:39092,localhost:49092`
- `acks=all` — waits for all in-sync replicas to confirm. Safe default.
- Use `PARTITION_UA` — lets rdkafka pick the partition by hashing the key. Same key always lands on the same partition.
- Use `std::getline(std::cin, message)` NOT `std::cin >> message` — the latter splits on spaces and sends each word as a separate message.
- Call `producer->poll(0)` inside the loop to serve internal callbacks.
- Call `producer->flush(10000)` before exit — waits up to 10s for in-flight messages to deliver.

---

## Consumer

- `group.id` — identifies the consumer group. Multiple consumers with the same group.id share partitions (each partition goes to one consumer). Different group.id = independent read cursor.
- `auto.offset.reset=earliest` — on first run reads from the beginning of the topic. Change to `latest` to only get new messages.
- `consume(1000)` — polls with a 1 second timeout. Returns a message or a timeout error — both are normal.
- Always `delete msg` after handling it.
- `ERR__TIMED_OUT` is not an error — it just means no messages arrived in the timeout window.
- Ctrl+C is handled via `SIGINT` → sets `running = false` → exits the loop cleanly → `consumer->close()`.

---

## Topics & Partitions

- Default partition count: **3** (set via `KAFKA_NUM_PARTITIONS: 3` in docker-compose).
- Partitions are **0-indexed**: 0, 1, 2. Never hardcode partition `3` — it doesn't exist.
- Topics are auto-created on first produce. To create manually:
```bash
docker exec -it broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic my-topic --partitions 3 --replication-factor 3
```

- List topics:
```bash
docker exec -it broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

---

## Common Errors & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `0xC0000135` at runtime | DLL not found | Use static build: `RDKAFKA_BUILD_STATIC ON` |
| `LNK1104: cannot open rdkafka++_static.lib` | Wrong target name | Link `rdkafka++` not `rdkafka++_static` |
| `LNK2005: main already defined` | CLion globbing both .cpp into one target | List source files explicitly in CMakeLists.txt |
| `Local: Unknown partition` | Hardcoded partition index out of range | Use `PARTITION_UA` |
| Messages split on spaces | Using `std::cin >>` | Use `std::getline(std::cin, message)` |
| CMake cache stale after config change | Old cache values override new ones | Delete `cmake-build-debug` entirely and reconfigure |