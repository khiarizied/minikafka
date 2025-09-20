package com.example;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Manages consumer groups and offsets by storing them in an internal topic,
 * similar to Kafka's __consumer_offsets.
 */
public class GroupCoordinator {

    public static final String OFFSETS_TOPIC = "__consumer_offsets";

    private final MiniKafkaWithSegmentsFixed server;
    private final Map<String, Long> offsets = new ConcurrentHashMap<>(); // Key: "group:topic:partition", Value: offset

    public GroupCoordinator(MiniKafkaWithSegmentsFixed server) {
        this.server = server;
    }

    /**
     * Loads all offsets from the internal topic into memory.
     * This is a blocking operation that should be called at startup.
     */
    public void loadOffsetsFromTopic() {
        System.out.println("Loading offsets from internal topic: " + OFFSETS_TOPIC);
        final CountDownLatch latch = new CountDownLatch(1);

        // Use a special group to consume the entire offsets topic
        String internalGroupName = "group-coordinator-loader-" + System.currentTimeMillis();
        
        // Create a temporary, local client to consume the topic
        MiniKafkaClientWithOffset tempClient = new MiniKafkaClientWithOffset("localhost", server.getPort());

        tempClient.consume(internalGroupName, OFFSETS_TOPIC, 0, -1, -1, (partition, offset, key, payload) -> {
            try {
                String offsetValue = new String(payload, StandardCharsets.UTF_8);
                long committedOffset = Long.parseLong(offsetValue);
                // The key is "group:topic:partition"
                offsets.put(key, committedOffset);
            } catch (Exception e) {
                System.err.println("Failed to parse offset message. Key: " + key + ", Payload: " + new String(payload));
            }
        });

        // This is a simplification. In a real system, we'd need a more robust way
        // to know when we've reached the end of the topic. For now, we'll just wait
        // a few seconds to allow the topic to be consumed.
        try {
            System.out.println("Waiting for offset topic to be consumed...");
            Thread.sleep(2000); // Allow time for consumption
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            tempClient.close();
            System.out.println("Finished loading " + offsets.size() + " offsets.");
        }
    }

    /**
     * Commits an offset by producing a message to the internal offsets topic.
     */
    public void commitOffset(String group, String topic, int partition, long offset) {
        String key = group + ":" + topic + ":" + partition;
        String payload = String.valueOf(offset);
        
        // Store in memory immediately
        offsets.put(key, offset);

        // Produce to the internal topic asynchronously
        server.produceInternal(OFFSETS_TOPIC, key, payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Fetches the last committed offset for a given group, topic, and partition.
     */
    public long getOffset(String group, String topic, int partition) {
        String key = group + ":" + topic + ":" + partition;
        return offsets.getOrDefault(key, 0L);
    }

    /**
     * Shuts down the coordinator. (No-op for now as there's no background thread).
     */
    public void shutdown() {
        System.out.println("GroupCoordinator shut down.");
        // No resources to release in this version
    }
}
