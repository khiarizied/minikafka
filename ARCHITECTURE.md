# MiniKafka Architecture

This document outlines the architecture of the MiniKafka project, a simplified message broker inspired by Apache Kafka. It covers the overall design, data storage format, client-server protocol, and key workflows.

## 1. Core Components

The system is composed of three main parts:

- **The Broker (`MiniKafkaWithSegmentsFixed.java`):** A single server process that listens for client connections, manages topics and partitions, and stores message data.
- **The Group Coordinator (`GroupCoordinator.java`):** A component within the Broker responsible for managing consumer groups and their committed offsets. It uses an internal topic for durable storage, mimicking real Kafka behavior.
- **The Client (`MiniKafkaClientWithOffset.java`):** A library that applications can use to produce messages to and consume messages from the broker.

---

## 2. Producer Workflow

When a client produces a message, the following sequence occurs:

1.  **Connection:** The client opens a new, short-lived TCP connection to the broker for each message it sends.
2.  **Protocol Command:** The client sends a `PRODUCE` command with the message details.
    - Format: `TOPIC <topic> <key> <payload_length>\n<payload_bytes>`
3.  **Partitioning:** The broker receives the command and determines the target partition by taking the hash of the message `key` and running it modulo the total number of partitions for the topic. This ensures messages with the same key always land in the same partition.
4.  **Storage:** The broker appends the message to the active segment file of the target partition. It assigns a new, unique, monotonically increasing offset (Record ID) to the message.
5.  **Response:** The broker sends a confirmation back to the client, indicating the offset the message was written to.
    - Format: `+OFFSET <offset>\n`

---

## 3. Consumer Workflow & Group Management

Consuming messages is a more stateful process involving coordination with the `GroupCoordinator`.

1.  **Connection:** The client opens a persistent TCP connection to the broker for consuming.
2.  **Consume Request:** The client sends a request specifying the topic, consumer group, and which partition it wants to read from.
    - Format: `topic:group:partition[:toOffset]`
3.  **Offset Fetching:** The broker's session handler receives the request and asks the `GroupCoordinator` for the last committed offset for that specific `group`, `topic`, and `partition`.
4.  **Message Streaming:**
    - The broker begins reading the partition's log files starting from the fetched offset.
    - For each message found, it sends it to the client.
    - Format: `+MSG <offset> <payload_length> <partition> <key>\n<payload_bytes>`
5.  **Acknowledgement (ACK):** After successfully processing a message, the client sends an `ACK` command back to the broker.
    - Format: `ACK\n`
6.  **Offset Commit:**
    - Upon receiving the `ACK`, the broker's session handler instructs the `GroupCoordinator` to commit the new offset (which is `message_offset + 1`).
    - The `GroupCoordinator` then **produces a message to the internal `__consumer_offsets` topic**. The key of this message is a string like `"my-group:my-topic:0"`, and the payload is the new offset number. This provides a durable, replicated log of consumer progress.
7.  **Loop:** The process repeats from step 4, with the broker sending the next available message. If no new messages are available, it sends `+EMPTY\n` and the client waits before retrying.

---

## 4. On-Disk Data Structure

### Directory Layout

Data is stored in a hierarchical directory structure:

```
<base_data_directory>/
└── logs/
    ├── my-topic/
    │   ├── 0/
    │   │   ├── 0.log
    │   │   └── 15000.log
    │   └── 1/
    │       └── 0.log
    │
    └── __consumer_offsets/
        ├── 0/
        │   └── 0.log
        └── ... (other partitions)
```

- **Topics:** Each topic gets its own directory.
- **Partitions:** Inside a topic directory, each partition has its own subdirectory.
- **Segments:** Inside a partition directory, the data is stored in `.log` files called segments. A new segment is created when the current one reaches `maxSegmentSizeBytes`. Segment files are named after the first offset they contain (e.g., `0.log`, `15000.log`).

### Message Record Format

Each `.log` file is a binary sequence of records. Every record is self-contained and prefixed with its total length, allowing for efficient traversal.

The structure of a single record is:

| Field                   | Data Type | Size (bytes) | Description                                                |
| ----------------------- | --------- | ------------ | ---------------------------------------------------------- |
| **Total Record Length** | `int`     | 4            | The total size in bytes of the fields that follow.         |
| **CRC32 of Payload**    | `int`     | 4            | A checksum of the payload to detect corruption.            |
| **Offset/Record ID**    | `long`    | 8            | The unique, sequential ID of the message in its partition. |
| **Timestamp**           | `long`    | 8            | The time the message was written by the broker.            |
| **Key Length**          | `int`     | 4            | The length of the message key in bytes.                    |
| **Key**                 | `byte[]`  | Variable     | The message key.                                           |
| **Payload Length**      | `int`     | 4            | The length of the message payload in bytes.                |
| **Payload**             | `byte[]`  | Variable     | The actual message content.                                |

---

## 5. Client-Server Network Protocol

Communication occurs over a simple, newline-delimited TCP protocol.

### Produce a Message

- **Client -> Server:** `TOPIC <topic> <key> <payload_length>\n<payload_bytes>`
- **Server -> Client (Success):** `+OFFSET <offset>\n`
- **Server -> Client (Error):** `-ERR <error_message>\n`

### Consume Messages

- **Client -> Server:** `topic:group:partition[:toOffset]\n`
  - `partition` can be `-1` to consume from all partitions.
  - `toOffset` is an optional upper bound for consumption.
- **Server -> Client (Message):** `+MSG <offset> <payload_length> <partition> <key>\n<payload_bytes>`
- **Server -> Client (No Messages):** `+EMPTY\n`
- **Server -> Client (End Reached):** `+END <offset>\n` (Used when `toOffset` is specified).

### Acknowledge a Message

- **Client -> Server:** `ACK\n`
- **Server -> Client (Success):** `+OK\n`

### Set Consumer Offset Manually

- **Client -> Server:** `SET_OFFSET <topic>:<group> <offset|BEGIN>\n`
- **Server -> Client (Success):** `+OK\n`
