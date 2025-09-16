package com.example;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.*;

/**
 * MiniKafka with segmentation support.
 * Tiny single-node TCP log server with
 * - partitions
 * - gzip compression
 * - pluggable per-partition listeners
 * - log segmentation
 */
public class MiniKafkaWithSegments {

    private final Path base;
    private final int partitionCount;
    private final ServerSocket server;
    private final Map<String, RandomAccessFile[]> topicFiles = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Long>> offsets = new ConcurrentHashMap<>(); // topic:partition -> group ->
                                                                                      // offset
    private final Path offsetFile;
    private final Map<String, List<PartitionListener>> listeners = new ConcurrentHashMap<>();

    // Segmentation settings
    private final long maxSegmentSizeBytes;
    private final int maxSegmentCount;
    private final Map<String, Map<Integer, List<Long>>> topicPartitionSegments = new ConcurrentHashMap<>(); // topic -> partition -> list of base offsets

    public interface PartitionListener {
        void onMessage(int partition, long offset, String key, byte[] payload);
    }

    public MiniKafkaWithSegments(int port, Path base, int partitions, long maxSegmentSizeBytes, int maxSegmentCount) throws IOException {
        this.base = base;
        this.partitionCount = partitions;
        this.maxSegmentSizeBytes = maxSegmentSizeBytes;
        this.maxSegmentCount = maxSegmentCount;
        this.offsetFile = base.resolve("offsets.json");
        Files.createDirectories(base.resolve("logs"));
        loadOffsets();
        loadSegments();
        server = new ServerSocket(port);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    /* --------------- public listener API --------------- */
    public synchronized void addListener(String topic, PartitionListener l) {
        listeners.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(l);
    }

    /* --------------- server main loop --------------- */
    public void serve() throws Exception {
        System.out.println("Listening on " + server.getLocalPort() + " with " + partitionCount + " partitions");
        System.out.println("Segmentation: max size=" + maxSegmentSizeBytes + " bytes, max count=" + maxSegmentCount);
        while (true) {
            Socket s = server.accept();
            new Thread(new Session(s)).start();
        }
    }

    /* --------------- persistence helpers --------------- */
    private RandomAccessFile[] topicFiles(String topic) {
        return topicFiles.computeIfAbsent(topic, t -> {
            try {
                RandomAccessFile[] fa = new RandomAccessFile[partitionCount];
                for (int p = 0; p < partitionCount; p++) {
                    // Get the latest segment for this partition
                    List<Long> segments = getOrCreateSegments(topic, p);
                    long baseOffset = segments.get(segments.size() - 1);
                    Path segmentFile = getSegmentFile(topic, p, baseOffset);
                    fa[p] = new RandomAccessFile(segmentFile.toFile(), "rw");
                }
                return fa;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private List<Long> getOrCreateSegments(String topic, int partition) {
        String topicKey = topic;
        return topicPartitionSegments.computeIfAbsent(topicKey, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(partition, p -> {
                    try {
                        // List existing segment files
                        List<Long> segments = new ArrayList<>();
                        Path partitionDir = base.resolve("logs").resolve(topic).resolve(String.valueOf(p));
                        if (Files.exists(partitionDir)) {
                            try (DirectoryStream<Path> stream = Files.newDirectoryStream(partitionDir, "*.log")) {
                                for (Path file : stream) {
                                    String fileName = file.getFileName().toString();
                                    if (fileName.endsWith(".log")) {
                                        String offsetStr = fileName.substring(0, fileName.length() - 4);
                                        try {
                                            long offset = Long.parseLong(offsetStr);
                                            segments.add(offset);
                                        } catch (NumberFormatException e) {
                                            // Skip invalid files
                                        }
                                    }
                                }
                            }
                        }

                        // Sort segments by offset
                        Collections.sort(segments);

                        // If no segments exist, create one with offset 0
                        if (segments.isEmpty()) {
                            segments.add(0L);
                            ensureSegmentExists(topic, partition, 0L);
                        }

                        return segments;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private Path getSegmentFile(String topic, int partition, long baseOffset) {
        return base.resolve("logs").resolve(topic).resolve(String.valueOf(partition))
                .resolve(baseOffset + ".log");
    }

    private void ensureSegmentExists(String topic, int partition, long baseOffset) throws IOException {
        Path segmentFile = getSegmentFile(topic, partition, baseOffset);
        Files.createDirectories(segmentFile.getParent());
        if (!Files.exists(segmentFile)) {
            Files.createFile(segmentFile);
        }
    }

    private void rollSegment(String topic, int partition, long newBaseOffset) throws IOException {
        List<Long> segments = getOrCreateSegments(topic, partition);

        // Add new segment
        segments.add(newBaseOffset);
        ensureSegmentExists(topic, partition, newBaseOffset);

        // Remove old segments if we exceed maxSegmentCount
        while (segments.size() > maxSegmentCount) {
            long oldBaseOffset = segments.remove(0);
            Path oldSegmentFile = getSegmentFile(topic, partition, oldBaseOffset);
            Files.deleteIfExists(oldSegmentFile);
        }

        // Update the active file for this partition
        RandomAccessFile[] files = topicFiles(topic);
        if (files != null && files[partition] != null) {
            files[partition].close();
        }
        files[partition] = new RandomAccessFile(getSegmentFile(topic, partition, newBaseOffset).toFile(), "rw");
    }

    @SuppressWarnings("unchecked")
    private void loadOffsets() {
        if (!Files.exists(offsetFile))
            return;
        try (Reader r = Files.newBufferedReader(offsetFile)) {
            Map<String, Map<String, Double>> raw = new com.google.gson.Gson().fromJson(r, Map.class);
            raw.forEach((tp, gm) -> gm.forEach((g, off) -> offsets.computeIfAbsent(tp, k -> new ConcurrentHashMap<>())
                    .put(g, off.longValue())));
        } catch (Exception e) {
            System.err.println("offset load failed: " + e);
        }
    }

    @SuppressWarnings("unchecked")
    private void loadSegments() {
        Path segmentsFile = base.resolve("segments.json");
        if (!Files.exists(segmentsFile))
            return;
        try (Reader r = Files.newBufferedReader(segmentsFile)) {
            Map<String, Map<String, List<Double>>> raw = new com.google.gson.Gson().fromJson(r, Map.class);
            raw.forEach((topic, pm) -> {
                Map<Integer, List<Long>> partitionMap = new ConcurrentHashMap<>();
                pm.forEach((pStr, segments) -> {
                    int partition = Integer.parseInt(pStr);
                    List<Long> segmentOffsets = new ArrayList<>();
                    segments.forEach(offset -> segmentOffsets.add(offset.longValue()));
                    partitionMap.put(partition, segmentOffsets);
                });
                topicPartitionSegments.put(topic, partitionMap);
            });
        } catch (Exception e) {
            System.err.println("segments load failed: " + e);
        }
    }

    private void saveOffsets() {
        try (Writer w = Files.newBufferedWriter(offsetFile)) {
            new com.google.gson.Gson().toJson(offsets, w);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveSegments() {
        try (Writer w = Files.newBufferedWriter(base.resolve("segments.json"))) {
            new com.google.gson.Gson().toJson(topicPartitionSegments, w);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void shutdown() {
        try {
            server.close();
            topicFiles.values().forEach(fa -> {
                for (RandomAccessFile f : fa)
                    try {
                        f.close();
                    } catch (Exception ignore) {
                    }
            });
            saveOffsets();
            saveSegments();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* --------------- per-client session --------------- */
    private class Session implements Runnable {
        private final Socket sock;
        private final DataInputStream in;
        private final DataOutputStream out;

        private String consumeTopic, consumeGroup;
        private int consumePartition;
        private long deliverOffset;
        private byte[] deliverBuf;

        Session(Socket s) throws IOException {
            this.sock = s;
            this.in = new DataInputStream(s.getInputStream());
            this.out = new DataOutputStream(s.getOutputStream());
        }

        public void run() {
            try {
                String line;
                while ((line = readLine()) != null) {
                    if (line.startsWith("TOPIC")) {
                        handleProduce(line.substring(5));
                    } else if (line.contains(":")) {
                        handleConsume(line);
                    } else if (line.equals("ACK")) {
                        handleAck();
                    } else {
                        write("-ERR unknown command\n");
                    }
                }
            } catch (Exception e) {
                System.err.println("client error: " + e);
            } finally {
                try {
                    sock.close();
                } catch (IOException ignore) {
                }
            }
        }

        private String readLine() throws IOException {
            StringBuilder sb = new StringBuilder();
            int b;
            while ((b = in.read()) != -1 && b != '\n')
                sb.append((char) b);
            return b == -1 ? null : sb.toString();
        }

        private void write(String s) throws IOException {
            out.writeBytes(s);
            out.flush();
        }

        /* ---------- PRODUCE ---------- */
        private void handleProduce(String topicKeyPayload) throws IOException {
            int k = topicKeyPayload.indexOf('<');
            if (k < 0) {
                write("-ERR bad format, expected TOPIC<key|payload>\n");
                return;
            }
            String topic = topicKeyPayload.substring(0, k);
            String keyAndPay = topicKeyPayload.substring(k + 1, topicKeyPayload.length() - 1);
            int sep = keyAndPay.indexOf('|');
            String key = sep < 0 ? "" : keyAndPay.substring(0, sep);
            byte[] payloadRaw = (sep < 0 ? keyAndPay : keyAndPay.substring(sep + 1)).getBytes(StandardCharsets.UTF_8);

            int partition = Math.abs(key.hashCode()) % partitionCount;
            RandomAccessFile file = topicFiles(topic)[partition];

            byte[] compressed;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
                gzip.write(payloadRaw);
                gzip.finish();
                compressed = baos.toByteArray();
            }

            long offset;
            synchronized (file) {
                offset = file.length();
                file.seek(offset);
                file.writeInt(compressed.length);
                file.write(compressed);

                // Check if we need to roll to a new segment
                if (file.length() >= maxSegmentSizeBytes) {
                    rollSegment(topic, partition, offset + 4 + compressed.length);
                }
            }
            write("+OFFSET " + offset + '\n');

            /* notify listeners */
            byte[] decompressed = decompress(compressed);
            List<PartitionListener> list = listeners.get(topic);
            if (list != null) {
                for (PartitionListener l : list)
                    l.onMessage(partition, offset, key, decompressed);
            }
        }

        /* ---------- CONSUME ---------- */
        private void handleConsume(String req) throws IOException {
            String[] parts = req.split(":");
            if (parts.length != 2) {
                write("-ERR bad format, expected TOPIC:GROUP\n");
                return;
            }
            consumeTopic = parts[0];
            consumeGroup = parts[1];

            // Find the partition and segment that contains the next message for this group
            for (int p = 0; p < partitionCount; p++) {
                String tpKey = consumeTopic + ":" + p;
                long nextOffset = offsets.computeIfAbsent(tpKey, k -> new ConcurrentHashMap<>())
                        .getOrDefault(consumeGroup, 0L);

                // Get all segments for this partition
                List<Long> segments = getOrCreateSegments(consumeTopic, p);

                // Find the segment that contains the next offset
                for (int i = 0; i < segments.size(); i++) {
                    long baseOffset = segments.get(i);
                    long nextBaseOffset = (i + 1 < segments.size()) ? segments.get(i + 1) : Long.MAX_VALUE;

                    // If the next offset is in this segment
                    if (nextOffset >= baseOffset && nextOffset < nextBaseOffset) {
                        Path segmentFile = getSegmentFile(consumeTopic, p, baseOffset);

                        try (RandomAccessFile f = new RandomAccessFile(segmentFile.toFile(), "r")) {
                            // Position to the offset within the segment
                            long segmentOffset = nextOffset - baseOffset;
                            if (segmentOffset >= f.length()) {
                                continue; // This segment doesn't have the offset
                            }

                            f.seek(segmentOffset);
                            int len = f.readInt();
                            byte[] compressed = new byte[len];
                            f.readFully(compressed);
                            deliverBuf = decompress(compressed);
                            deliverOffset = nextOffset;
                            consumePartition = p;

                            write("+MSG " + deliverOffset + ' ' + compressed.length + '\n');
                            out.write(compressed);
                            out.write('\n');
                            out.flush();
                            return;
                        } catch (EOFException e) {
                            // End of segment, continue to next segment
                            continue;
                        }
                    }
                }
            }
            write("+EMPTY\n");
        }

        private void handleAck() throws IOException {
            if (consumeTopic == null) {
                write("-ERR no outstanding msg\n");
                return;
            }
            String tpKey = consumeTopic + ":" + consumePartition;
            offsets.get(tpKey).put(consumeGroup, deliverOffset + 4 + compress(deliverBuf).length);
            saveOffsets();
            write("+OK\n");
            consumeTopic = null;
        }
    }

    /* --------------- compression helpers --------------- */
    private static byte[] compress(byte[] raw) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(raw);
            gzip.finish();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] decompress(byte[] compressed) {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed));
                ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buf = new byte[4096];
            int n;
            while ((n = gzip.read(buf)) != -1)
                baos.write(buf, 0, n);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /* --------------- client helper (listener) --------------- */
    public static class Client {
        private final String host;
        private final int port;
        private final MiniKafkaWithSegments serverLocal; // null when remote

        private Client(String host, int port, MiniKafkaWithSegments serverLocal) {
            this.host = host;
            this.port = port;
            this.serverLocal = serverLocal;
        }

        public void addListener(String topic, PartitionListener l) {
            if (serverLocal != null) {
                serverLocal.addListener(topic, l);
                return;
            }
            throw new UnsupportedOperationException("remote listener not implemented over TCP yet");
        }

        public long produce(String topic, String key, byte[] payload) throws IOException {
            try (Socket s = new Socket(host, port);
                    DataOutputStream out = new DataOutputStream(s.getOutputStream());
                    DataInputStream in = new DataInputStream(s.getInputStream())) {
                out.writeBytes("TOPIC" + topic + '<' + key + '|' + new String(payload, StandardCharsets.UTF_8) + ">\n");
                out.flush();
                String resp = new Scanner(in).nextLine();
                return Long.parseLong(resp.split(" ")[1]);
            }
        }
    }

    public static Client connect(String host, int port) {
        return new Client(host, port, null);
    }

    public static Client connectLocal(MiniKafkaWithSegments server) {
        return new Client(null, 0, server);
    }

    /* --------------- main --------------- */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: java MiniKafkaWithSegments <port> <data-dir> [partitionCount] [maxSegmentSizeMB] [maxSegmentCount]");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        Path dir = Paths.get(args[1]);
        int parts = args.length > 2 ? Integer.parseInt(args[2]) : 4;
        long maxSegmentSizeMB = args.length > 3 ? Long.parseLong(args[3]) : 100; // Default 100MB
        int maxSegmentCount = args.length > 4 ? Integer.parseInt(args[4]) : 10; // Default 10 segments

        new MiniKafkaWithSegments(port, dir, parts, maxSegmentSizeMB * 1024 * 1024, maxSegmentCount).serve();
    }
}
