package com.example;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Enhanced MiniKafka client with offset control support.
 */
public class MiniKafkaClientWithOffset implements AutoCloseable {

    private final String host;
    private final int port;
    private final ExecutorService listenerPool = Executors.newCachedThreadPool();

    public interface Handler {
        void onMessage(int partition, long offset, String key, byte[] payload);
    }

    public MiniKafkaClientWithOffset(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /*
     * ----------------------------------------------------------
     * PRODUCE
     * ----------------------------------------------------------
     */
    public long produce(String topic, String key, byte[] payload) throws IOException {
        try (Socket s = new Socket(host, port);
                DataOutputStream out = new DataOutputStream(s.getOutputStream());
                DataInputStream in = new DataInputStream(s.getInputStream())) {

            // New binary-safe protocol: TOPIC <topic> <key> <payload_length>\n<payload_bytes>
            String header = "TOPIC " + topic + " " + (key == null ? "" : key) + " " + payload.length + "\n";
            out.writeBytes(header);
            out.write(payload);
            out.flush();

            String resp = readLine(in);
            if (resp == null || !resp.startsWith("+OFFSET "))
                throw new IOException("Server error: " + resp);
            return Long.parseLong(resp.split(" ")[1]);
        }
    }

    /*
     * ----------------------------------------------------------
     * CONSUME (long-lived listener)
     * ----------------------------------------------------------
     */
    public void consume(String group, String topic, Handler handler) {
        consume(group, topic, -1, -1, -1, handler);
    }

    public void consume(String group, String topic, long fromOffset, long toOffset, int partition, Handler handler) {
        listenerPool.submit(() -> {
            boolean offsetSet = false;
            while (!Thread.currentThread().isInterrupted()) {
                try (Socket s = new Socket(host, port);
                        DataOutputStream out = new DataOutputStream(s.getOutputStream());
                        DataInputStream in = new DataInputStream(s.getInputStream())) {

                    if (!offsetSet) {
                        if (fromOffset >= 0) {
                            out.writeBytes("SET_OFFSET " + topic + ":" + group + " " + fromOffset + "\n");
                            out.flush();
                            String resp = readLine(in);
                            if (resp == null || !resp.startsWith("+OK")) {
                                throw new IOException("Failed to set offset: " + resp);
                            }
                        } else if (fromOffset == -1) {
                            out.writeBytes("SET_OFFSET " + topic + ":" + group + " BEGIN\n");
                            out.flush();
                            String resp = readLine(in);
                            if (resp == null || !resp.startsWith("+OK")) {
                                throw new IOException("Failed to set offset to beginning: " + resp);
                            }
                        }
                        offsetSet = true;
                    }

                    // New format: TOPIC:GROUP:PARTITION[:toOffset]
                    String consumeRequest = topic + ":" + group + ":" + partition;
                    if (toOffset >= 0) {
                        consumeRequest += ":" + toOffset;
                    }
                    out.writeBytes(consumeRequest + '\n');
                    out.flush();

                    String resp = readLine(in);
                    if (resp == null)
                        break;
                    if (resp.startsWith("+EMPTY") || resp.startsWith("+END ")) {
                        if (resp.startsWith("+END ")) {
                            System.out.println("Reached end offset " + resp.split(" ")[1] + ", stopping consumption");
                            break; // leave loop – no error
                        }
                        Thread.sleep(200);
                        continue;
                    }
                    if (!resp.startsWith("+MSG ")) {
                        throw new IOException("Unexpected response: " + resp);
                    }

                    String[] meta = resp.split(" ");
                    long offset = Long.parseLong(meta[1]);
                    int len = Integer.parseInt(meta[2]);
                    int receivedPartition = Integer.parseInt(meta[3]);

                    if (toOffset >= 0 && offset > toOffset) {
                        System.out.println("Reached end offset " + toOffset + ", stopping consumption");
                        break;
                    }

                    byte[] payload = new byte[len];
                    in.readFully(payload);
                    in.read(); // consume trailing \n

                    handler.onMessage(receivedPartition, offset, "", payload);

                    out.writeBytes("ACK\n");
                    out.flush();
                    String ackResp = readLine(in);
                    if (ackResp == null || !"+OK".equals(ackResp))
                        throw new IOException("Ack failed: " + ackResp);

                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.err.println("Listener error, will retry: " + e.getMessage());
                    try {
                        Thread.sleep(1_000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
    }

    private String readLine(DataInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1 && b != '\n')
            sb.append((char) b);
        return b == -1 ? null : sb.toString();
    }

    // ✅ Safe payload formatter — shows JSON if readable, hex if binary
    private String formatPayload(byte[] payload) {
        if (payload == null || payload.length == 0)
            return "<empty>";

        boolean isText = true;
        for (byte b : payload) {
            if (b < 32 && b != '\t' && b != '\n' && b != '\r') {
                isText = false;
                break;
            }
            if (b > 126) {
                isText = false;
                break;
            }
        }

        if (isText) {
            try {
                return new String(payload, StandardCharsets.UTF_8);
            } catch (Exception e) {
                // Fallback to hex if UTF-8 decode fails
            }
        }

        // Return hex dump for binary data
        StringBuilder hex = new StringBuilder();
        for (byte b : payload) {
            hex.append(String.format("%02X ", b));
        }
        return "<binary (" + payload.length + " bytes)> " + hex.toString().trim();
    }

    @Override
    public void close() {
        listenerPool.shutdownNow();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
    }

    private byte[] compress(byte[] raw) {
        // Compression removed - return data as-is
        return raw;
    }

    private byte[] decompress(byte[] data) {
        // Compression removed - return data as-is  
        return data;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(
                    "Usage: java MiniKafkaClientWithOffset <host> <port> <topic> [group] [partition] [fromOffset] [toOffset]");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];

        // Produce mode
        if (args.length == 4 && "produce".equals(args[3])) {
            try (MiniKafkaClientWithOffset cli = new MiniKafkaClientWithOffset(host, port)) {
                for (int i = 0; i < 30; i++) {
                    String key = "key" + i;
                    String payload = "message " + i;
                    long off = cli.produce(topic, key, payload.getBytes(StandardCharsets.UTF_8));
                    System.out.println("Sent to offset " + off);
                }
            }
            return;
        }

        // Consume mode
        String group = args.length > 3 ? args[3] : "g1";
        int partition = args.length > 4 ? Integer.parseInt(args[4]) : -1; // -1 for all
        long fromOffset = args.length > 5 ? Long.parseLong(args[5]) : -1;
        long toOffset = args.length > 6 ? Long.parseLong(args[6]) : -1;

        try (MiniKafkaClientWithOffset cli = new MiniKafkaClientWithOffset(host, port)) {
            cli.consume(group, topic, fromOffset, toOffset, partition, (p, offset, key, payload) -> {
                String safeDisplay;
                try {
                    safeDisplay = new String(payload, StandardCharsets.UTF_8);
                } catch (Exception e) {
                    safeDisplay = "<binary (" + payload.length + " bytes)> " + bytesToHex(payload);
                }
                System.out
                        .println("Received message at offset " + offset + " (partition " + p + "): " + safeDisplay);
            });

            // Keep alive
            Thread.sleep(Long.MAX_VALUE);
        }
    }
}