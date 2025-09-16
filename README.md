# MiniKafka

This is a simple single-node TCP log server that implements some Kafka-like features, including partitioning, gzip compression, and pluggable partition listeners.

## Features

- Partitioned storage
- Gzip compression
- Pluggable partition listeners
- TCP-based client-server communication
- Consumer group offset management

## Build and Run

### Build with Maven

```bash
mvn clean package
```

### Run the Server

```bash
java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.MiniKafka <port> <data-dir> [partitions=4]
```

For example:

```bash
java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.MiniKafka 9092 ./data 4
```

## Usage Examples

### Produce Messages

```java
MiniKafkaClient client = new MiniKafkaClient("localhost", 9092);
long offset = client.produce("my-topic", "key1", "message content".getBytes());
```

### Consume Messages

```java
MiniKafkaClient client = new MiniKafkaClient("localhost", 9092);
// Add listener
client.consume("group1", "my-topic", (partition, offset, key, payload) -> {
    System.out.println("Received message: " + new String(payload));
});
```

## Protocol Description

### Produce Message

Client sends: `TOPIC<topic><key|payload>`

Server responds: `+OFFSET <offset>`

### Consume Message

Client sends: `<topic>:<group>`

Server responds:
- If there is a message: `+MSG <offset> <length>` followed by the message content and a newline
- If there is no message: `+EMPTY`

### Acknowledge Message

Client sends: `ACK`

Server responds: `+OK`

## Annotation-Based Listener System

MiniKafka also supports an annotation-based listener system that allows you to easily create message consumers by annotating methods with @KafkaListener.

### Example Usage

```java
public class MyListener {
    @KafkaListener(topic = "test-topic", groupId = "example-group")
    public void handleTestMessage(int partition, long offset, String key, byte[] payload) {
        System.out.println("Received test message: " + new String(payload, StandardCharsets.UTF_8));
    }
    
    @KafkaListener(topic = "orders", groupId = "order-processors")
    public void handleOrder(int partition, long offset, String key, byte[] payload) {
        System.out.println("Processing order: " + new String(payload, StandardCharsets.UTF_8));
        // Process order logic here
    }
}
```

### Running an Annotated Listener

```bash
java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.AnnotatedListenerExample localhost 9982
```

You can also create your own listener class and use the KafkaListenerProcessor to register it:

```java
public static void main(String[] args) throws IOException {
    MiniKafkaClient client = new MiniKafkaClient("localhost", 9982);
    KafkaListenerProcessor processor = new KafkaListenerProcessor(client);
    
    MyListener listener = new MyListener();
    processor.process(listener);
    
    // Keep running
    try {
        Thread.sleep(Long.MAX_VALUE);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    } finally {
        client.close();
    }
}
```

## Algorithm Descriptions

### Message Storage Algorithm

MiniKafka uses a partitioned storage system to organize messages:

1. **Partition Allocation**: When a topic is created, messages are distributed across partitions using a simple round-robin algorithm. Each partition is a separate directory on disk.

2. **Message Writing**: When a producer sends a message:
   - The message is assigned to a partition based on a hash of the key (if provided) or round-robin
   - The message is appended to the partition file with the format: `[offset][key-length][key][payload-length][payload]`
   - The offset is monotonically increasing within each partition
   - After writing, the message may be GZIP compressed if compression is enabled

3. **Offset Management**: Each partition maintains its own offset counter, which starts at 0 and increments with each message. Offsets are persisted to disk to survive server restarts.

### Partition File Format

MiniKafka stores messages in partition files using a binary format that allows efficient reading and writing:

1. **File Structure**: Each partition is stored as a single file that grows as messages are appended. The file is named with the pattern "topic-partition.log" (e.g., "orders-0.log").

2. **Message Record Format**: Each message in the partition file follows this structure:
   ```
   +------------------+--------------------+
   | Length (4 bytes) | Payload (variable) |
   +------------------+--------------------+
   ```
   - **Length**: A 4-byte integer indicating the length of the compressed payload in bytes
   - **Payload**: The compressed message content (key and payload combined)

3. **Writing Process**: When a new message is written:
   - The message key and payload are combined and compressed using GZIP
   - The file pointer is moved to the end of the file
   - The length of the compressed data is written (4 bytes)
   - The compressed data is written
   - The file position at the start of the record is used as the offset

4. **Reading Process**: When reading messages from a specific offset:
   - The file is opened and the file pointer is positioned to the specified offset
   - The length of the compressed data is read (4 bytes)
   - The compressed data is read based on the length
   - The data is decompressed to extract the original key and payload

Note: The current implementation includes:
- Segmentation of partition files (each partition is stored in multiple segment files)
- Automatic segment rolling based on size limits
- Segment cleanup based on count limits
- Persistent segment metadata

The implementation does not include:
- Separate storage for keys and payloads in the file format
- Index files for faster offset lookups
- Explicit offset fields in the file format (offsets are derived from file position)

### Message Consumption Algorithm

MiniKafka implements a consumer group model for message consumption:

1. **Consumer Registration**: When a consumer connects, it specifies a topic and consumer group ID. The server tracks which consumers belong to which groups.

2. **Partition Assignment**: Within a consumer group, partitions are assigned to consumers. Each partition is consumed by only one consumer in a group, but can be consumed by multiple consumers in different groups.

3. **Offset Tracking**: Each consumer group maintains its own offset for each partition. This allows different groups to consume messages independently.

4. **Message Delivery**: When a consumer requests messages:
   - The server checks the consumer's last committed offset for the partition
   - Messages with offsets greater than the last committed offset are returned
   - The consumer acknowledges receipt by sending an ACK
   - The server updates the consumer's offset only after receiving an ACK

### GZIP Compression Algorithm

MiniKafka supports optional GZIP compression for message payloads:

1. **Compression Decision**: Compression is applied based on message size and configuration settings.

2. **Compression Process**: Before writing to disk, the payload is compressed using GZIP. A compression flag is added to the message metadata to indicate whether the payload is compressed.

3. **Decompression Process**: When reading messages, the server checks the compression flag. If set, the payload is decompressed before being sent to the consumer.

### TCP Communication Protocol

MiniKafka uses a simple text-based protocol over TCP:

1. **Connection Handling**: The server uses non-blocking I/O with a selector to handle multiple client connections efficiently.

2. **Message Production**: 
   - Client sends: `TOPIC<topic><key|payload>`
   - Server responds: `+OFFSET <offset>`
   - The server parses the topic, key, and payload, then writes the message to the appropriate partition

3. **Message Consumption**:
   - Client sends: `<topic>:<group>`
   - Server responds with messages or `+EMPTY` if no new messages are available
   - Each message is prefixed with `+MSG <offset> <length>`

4. **Acknowledgment**:
   - Client sends: `ACK` after processing a message
   - Server responds: `+OK` and updates the consumer's offset

### Annotation-Based Listener System

The annotation-based listener system provides a convenient way to create message consumers:

1. **Method Scanning**: The KafkaListenerProcessor scans classes for methods annotated with @KafkaListener.

2. **Validation**: Each annotated method is validated to ensure it has the correct signature: `(int partition, long offset, String key, byte[] payload)`.

3. **Registration**: For each valid method, the processor registers a consumer with the MiniKafkaClient for the specified topic and group.

4. **Message Dispatch**: When a message is received, the processor invokes the appropriate method with the message details.

## Using MiniKafka with Segmentation

MiniKafkaWithSegments is an enhanced version of MiniKafka that supports log segmentation, which helps manage large log files by splitting them into smaller segments.

### Running MiniKafkaWithSegments

```bash
java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.MiniKafkaWithSegments <port> <data-dir> [partitionCount] [maxSegmentSizeMB] [maxSegmentCount]
```

For example:

```bash
java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.MiniKafkaWithSegments 9092 ./data 4 100 10
```

This will start a server with:
- Port 9092
- Data directory ./data
- 4 partitions per topic
- Maximum segment size of 100MB
- Maximum of 10 segments per partition

### Using MiniKafkaClientWithSegments

```bash
java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.MiniKafkaClientWithSegments <host> <port> [topic] [group]
```

For example:

```bash
java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.MiniKafkaClientWithSegments localhost 9092 test-topic g1
```

### Segment File Structure

With segmentation enabled, the log files are organized as follows:

```
data/
├── logs/
│   ├── topic1/
│   │   ├── 0/
│   │   │   ├── 0.log
│   │   │   ├── 1048576.log
│   │   │   └── 2097152.log
│   │   ├── 1/
│   │   │   ├── 0.log
│   │   │   └── 1048576.log
│   │   └── ...
│   └── topic2/
│       └── ...
├── offsets.json
└── segments.json
```

Each segment file is named with its base offset (the offset of the first message in the segment). When a segment reaches the maximum size, a new segment is created with the current offset as its base offset.

## Testing

To test the producer-consumer functionality:

1. Start the server:
   ```bash
   java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.MiniKafka 9982 "./data" 4
   ```

2. Start a producer in a new terminal:
   ```bash
   java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.MessageProducer localhost 9982 test-topic
   ```

3. Start a consumer in another new terminal:
   ```bash
   java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.MiniKafkaClient localhost 9982 test-topic g1
   ```

4. Or start an annotated listener:
   ```bash
   java -cp target/minikafka-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.AnnotatedListenerExample localhost 9982
   ```
