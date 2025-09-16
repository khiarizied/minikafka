package com.example;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Processes classes with @KafkaListener annotated methods and registers them
 * with the MiniKafkaClient for message consumption.
 */
public class KafkaListenerProcessor {
    private final MiniKafkaClient client;
    private final Map<String, Method> topicToMethodMap = new HashMap<>();
    private final Map<String, Object> topicToInstanceMap = new HashMap<>();

    public KafkaListenerProcessor(MiniKafkaClient client) {
        this.client = client;
    }

    /**
     * Scans an object for @KafkaListener annotated methods and registers them.
     * @param listener The object to scan
     */
    public void process(Object listener) {
        Class<?> clazz = listener.getClass();

        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(KafkaListener.class)) {
                KafkaListener annotation = method.getAnnotation(KafkaListener.class);
                String topic = annotation.topic();
                String groupId = annotation.groupId();

                // Validate method signature
                if (method.getParameterCount() != 4 || 
                    method.getParameterTypes()[0] != int.class || // partition
                    method.getParameterTypes()[1] != long.class || // offset
                    method.getParameterTypes()[2] != String.class || // key
                    method.getParameterTypes()[3] != byte[].class) { // payload
                    throw new IllegalArgumentException("Method " + method.getName() + 
                        " must have signature (int partition, long offset, String key, byte[] payload)");
                }

                // Make method accessible
                method.setAccessible(true);

                // Store mapping
                topicToMethodMap.put(topic + ":" + groupId, method);
                topicToInstanceMap.put(topic + ":" + groupId, listener);

                // Register consumer with MiniKafkaClient
                client.consume(groupId, topic, (partition, offset, key, payload) -> {
                    try {
                        Method m = topicToMethodMap.get(topic + ":" + groupId);
                        Object instance = topicToInstanceMap.get(topic + ":" + groupId);
                        m.invoke(instance, partition, offset, key, payload);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                System.out.println("Registered listener for topic: " + topic + 
                                  " with group: " + groupId + 
                                  " -> " + clazz.getName() + "." + method.getName());
            }
        }
    }

    /**
     * Example usage of the KafkaListener annotation system.
     */
    public static class ExampleListener {
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

    public static void main(String[] args) throws IOException {
        // Create client
        MiniKafkaClient client = new MiniKafkaClient("localhost", 9982);

        // Create processor
        KafkaListenerProcessor processor = new KafkaListenerProcessor(client);

        // Create and register listener
        ExampleListener listener = new ExampleListener();
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
}
