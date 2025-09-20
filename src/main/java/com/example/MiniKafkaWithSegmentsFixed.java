package com.example;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * MiniKafka with segmentation support – NOW USING DENSE RECORD-Ids
 * (0,1,2…) instead of byte positions.
 */
public class MiniKafkaWithSegmentsFixed {

    /* ---------------------------------------------------------- */
    /*  unchanged fields                                           */
    /* ---------------------------------------------------------- */
    private final Path base;
    private final int partitionCount;
    private final ServerSocket server;
    private final Map<String, SafeRandomAccessFile[]> topicFiles = new ConcurrentHashMap<>();
    // private final Map<String, Map<String, Long>> offsets = new ConcurrentHashMap<>(); // Replaced by GroupCoordinator
    // private final Path offsetFile; // Replaced by GroupCoordinator
    private final Map<String, List<PartitionListener>> listeners = new ConcurrentHashMap<>();
    private final Map<String, SafeRandomAccessFile> segmentFileCache = new ConcurrentHashMap<>();
    private final long maxSegmentSizeBytes;
    private final int maxSegmentCount;
    private final Map<String, Map<Integer, List<Long>>> topicPartitionSegments = new ConcurrentHashMap<>();

    /* NEW: per-topic, per-partition record counter (0,1,2…) */
    private final Map<String, long[]> nextRecordId = new ConcurrentHashMap<>();

    // NEW: Centralized group management
    private final GroupCoordinator groupCoordinator;

    public interface PartitionListener {
        void onMessage(int partition, long offset, String key, byte[] payload);
    }

    /* ---------------------------------------------------------- */
    /*  constructor (unchanged)                                   */
    /* ---------------------------------------------------------- */
    public MiniKafkaWithSegmentsFixed(int port, Path base, int partitions,
                                      long maxSegmentSizeBytes, int maxSegmentCount) throws IOException {
        this.base = base;
        this.partitionCount = partitions;
        this.maxSegmentSizeBytes = maxSegmentSizeBytes;
        this.maxSegmentCount = maxSegmentCount;
        // this.offsetFile = base.resolve("offsets.json"); // Replaced
        Files.createDirectories(base.resolve("logs"));
        
        // Initialize the Group Coordinator
        this.groupCoordinator = new GroupCoordinator(this);

        rebuildSegmentsFromDisk(); // More robust than loading from a potentially stale JSON file
        server = new ServerSocket(port);

        // Must be called AFTER server socket is created
        this.groupCoordinator.loadOffsetsFromTopic();

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public int getPort() {
        return server.getLocalPort();
    }

    /**
     * An internal produce method that bypasses the network stack for efficiency.
     * Used by the GroupCoordinator to commit offsets.
     */
    public void produceInternal(String topic, String key, byte[] payload) {
        int partition = Math.abs(key.hashCode()) % partitionCount;
        SafeRandomAccessFile safeFile = topicFiles(topic)[partition];

        try {
            long recordId = nextRecordId(topic, partition);
            long timestamp = System.currentTimeMillis();
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            Checksum crc = new CRC32();
            crc.update(payload, 0, payload.length);
            int crcValue = (int) crc.getValue();

            int keyLen = keyBytes.length;
            int payloadLen = payload.length;
            int totalLen = 4 + 8 + 8 + 4 + keyLen + 4 + payloadLen;

            safeFile.seek(safeFile.length());
            safeFile.writeInt(totalLen);
            safeFile.writeInt(crcValue);
            safeFile.writeLong(recordId);
            safeFile.writeLong(timestamp);
            safeFile.writeInt(keyLen);
            safeFile.write(keyBytes);
            safeFile.writeInt(payloadLen);
            safeFile.write(payload);

            if (safeFile.length() >= maxSegmentSizeBytes) {
                rollSegment(topic, partition, recordId + 1);
            }
        } catch (IOException e) {
            System.err.println("FATAL: Error in internal produce: " + e.getMessage());
            // This is a critical error, as it means the server can't manage its own state.
        }
    }

    /* ---------------------------------------------------------- */
    /*  listener API (unchanged)                                  */
    /* ---------------------------------------------------------- */
    public synchronized void addListener(String topic, PartitionListener l) {
        listeners.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(l);
    }

    private void rebuildSegmentsFromDisk() {
        Path logsDir = base.resolve("logs");
        if (!Files.exists(logsDir)) return;

        try (DirectoryStream<Path> topics = Files.newDirectoryStream(logsDir)) {
            for (Path topicDir : topics) {
                if (!Files.isDirectory(topicDir)) continue;
                String topic = topicDir.getFileName().toString();
                try (DirectoryStream<Path> partitions = Files.newDirectoryStream(topicDir)) {
                    for (Path partitionDir : partitions) {
                        if (!Files.isDirectory(partitionDir)) continue;
                        try {
                            int partition = Integer.parseInt(partitionDir.getFileName().toString());
                            getOrCreateSegments(topic, partition); // This will trigger the scan and population
                        } catch (NumberFormatException e) {
                            // Ignore directories that aren't partition numbers
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error rebuilding segments from disk: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /* ---------------------------------------------------------- */
    /*  server main loop (unchanged)                              */
    /* ---------------------------------------------------------- */
    public void serve() throws Exception {
        System.out.println("Listening on " + server.getLocalPort() + " with " + partitionCount + " partitions");
        System.out.println("Segmentation: max size=" + maxSegmentSizeBytes + " bytes, max count=" + maxSegmentCount);
        while (true) {
            Socket s = server.accept();
            new Thread(new Session(s)).start();
        }
    }
private  long nextRecordId(String topic, int partition) {
    return nextRecordId.computeIfAbsent(topic, k -> new long[partitionCount])[partition]++;
}
   
    /* ---------------------------------------------------------- */
    /*  persistence helpers (unchanged)                           */
    /* ---------------------------------------------------------- */
    private SafeRandomAccessFile[] topicFiles(String topic) {
        return topicFiles.computeIfAbsent(topic, t -> {
            try {
                SafeRandomAccessFile[] fa = new SafeRandomAccessFile[partitionCount];
                for (int p = 0; p < partitionCount; p++) {
                    List<Long> segments = getOrCreateSegments(topic, p);
                    long baseOffset = segments.get(segments.size() - 1);
                    Path segmentFile = getSegmentFile(topic, p, baseOffset);
                    RandomAccessFile raf = new RandomAccessFile(segmentFile.toFile(), "rw");
                    fa[p] = new SafeRandomAccessFile(raf);
                }
                return fa;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private List<Long> getOrCreateSegments(String topic, int partition) {
        return topicPartitionSegments.computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(partition, p -> {
                    try {
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
                                            // skip
                                        }
                                    }
                                }
                            }
                        }
                        Collections.sort(segments);
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
        segments.add(newBaseOffset);
        ensureSegmentExists(topic, partition, newBaseOffset);
        while (segments.size() > maxSegmentCount) {
            long oldBaseOffset = segments.remove(0);
            Path oldSegmentFile = getSegmentFile(topic, partition, oldBaseOffset);
            String oldKey = topic + ":" + partition + ":" + oldBaseOffset;
            SafeRandomAccessFile oldFile = segmentFileCache.remove(oldKey);
            if (oldFile != null) {
                try {
                    oldFile.close();
                } catch (IOException e) {
                    System.err.println("Failed to close old segment file: " + e.getMessage());
                }
            }
            Files.deleteIfExists(oldSegmentFile);
            System.out.println("Deleted old segment: " + oldSegmentFile);
        }
        SafeRandomAccessFile[] files = topicFiles(topic);
        if (files != null && files[partition] != null) {
            try {
                files[partition].getDelegate().getFD().sync();
                System.out.println("Rolling segment for " + topic + ":" + partition + " → new base offset: " + newBaseOffset);
            } catch (IOException e) {
                System.err.println("Failed to sync segment before roll: " + e.getMessage());
            }
            try {
                files[partition].close();
            } catch (IOException e) {
                System.err.println("Failed to close segment file: " + e.getMessage());
            }
        }
        files[partition] = new SafeRandomAccessFile(
                new RandomAccessFile(getSegmentFile(topic, partition, newBaseOffset).toFile(), "rw"));
    }

    private SafeRandomAccessFile getSegmentFileForRead(String topic, int partition, long baseOffset)
            throws IOException {
        String key = topic + ":" + partition + ":" + baseOffset;
        Path segmentFile = getSegmentFile(topic, partition, baseOffset);
        return segmentFileCache.computeIfAbsent(key, k -> {
            try {
                RandomAccessFile raf = new RandomAccessFile(segmentFile.toFile(), "r");
                return new SafeRandomAccessFile(raf);
            } catch (IOException e) {
                throw new RuntimeException("Failed to open segment file: " + segmentFile, e);
            }
        });
    }

    private void shutdown() {
        try {
            groupCoordinator.shutdown(); // Shut down the coordinator

            server.close();
            topicFiles.values().forEach(fa -> {
                for (SafeRandomAccessFile f : fa)
                    try {
                        f.close();
                    } catch (Exception ignore) {
                    }
            });
            segmentFileCache.values().forEach(f -> {
                try {
                    f.close();
                } catch (Exception ignore) {
                }
            });
            // No longer need to save offsets or segments here
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* ========================================================== */
    /*  SESSION – adapted for record-ids                          */
    /* ========================================================== */
    private class Session implements Runnable {
        private final Socket sock;
        private final DataInputStream in;
        private final DataOutputStream out;

        private String consumeTopic, consumeGroup;
        private int consumePartition;
        private long deliverOffset;   // now = RECORD-ID
        private byte[] deliverBuf;
        private int lastCompressedLength; // not used anymore

        /* NEW: upper bound requested by the client (-1 = none) */
        private long consumeToOffset = -1;

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
                    } else if (line.startsWith("SET_OFFSET")) {
                        handleSetOffset(line.substring(11));
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
        private void handleProduce(String line) throws IOException {
            String[] parts = line.split(" ", 4);
            if (parts.length < 4) {
                write("-ERR bad format, expected TOPIC <topic> <key> <payload_length>\\n<payload>\n");
                return;
            }
            String topic = parts[1];
            String key = parts[2];
            int payloadLength;
            try {
                payloadLength = Integer.parseInt(parts[3]);
            } catch (NumberFormatException e) {
                write("-ERR invalid payload length\n");
                return;
            }

            byte[] payload = new byte[payloadLength];
            in.readFully(payload);

            int partition = Math.abs(key.hashCode()) % partitionCount;
            SafeRandomAccessFile safeFile = topicFiles(topic)[partition];

            long recordId = nextRecordId(topic, partition);
            long timestamp = System.currentTimeMillis();
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            // Calculate CRC32 of the payload
            Checksum crc = new CRC32();
            crc.update(payload, 0, payload.length);
            int crcValue = (int) crc.getValue();

            // New format: [total_len (int)] [crc (int)] [id (long)] [ts (long)] [key_len (int)] [key] [payload_len (int)] [payload]
            int keyLen = keyBytes.length;
            int payloadLen = payload.length;
            int totalLen = 4 + 8 + 8 + 4 + keyLen + 4 + payloadLen; // crc, id, ts, key_len, key, payload_len, payload

            safeFile.seek(safeFile.length());
            safeFile.writeInt(totalLen);
            safeFile.writeInt(crcValue);
            safeFile.writeLong(recordId);
            safeFile.writeLong(timestamp);
            safeFile.writeInt(keyLen);
            safeFile.write(keyBytes);
            safeFile.writeInt(payloadLen);
            safeFile.write(payload);

            if (safeFile.length() >= maxSegmentSizeBytes) {
                rollSegment(topic, partition, recordId + 1);
            }

            write("+OFFSET " + recordId + '\n');

            List<PartitionListener> list = listeners.get(topic);
            if (list != null) {
                for (PartitionListener l : list)
                    l.onMessage(partition, recordId, key, payload);
            }
        }

        /* ---------- CONSUME ---------- */
        private void handleConsume(String req) throws IOException {
            String[] parts = req.split(":");
            if (parts.length < 3) { // topic:group:partition
                write("-ERR bad format, expected TOPIC:GROUP:PARTITION[:toOffset]\n");
                return;
            }
            consumeTopic = parts[0];
            consumeGroup = parts[1];
            int requestedPartition;
            try {
                requestedPartition = Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                write("-ERR invalid partition number\n");
                return;
            }

            /* --- parse optional toOffset --- */
            if (parts.length > 3) {
                try {
                    consumeToOffset = Long.parseLong(parts[3]);
                } catch (NumberFormatException e) {
                    write("-ERR invalid toOffset\n");
                    return;
                }
            } else {
                consumeToOffset = -1;
            }

            int startPartition = (requestedPartition == -1) ? 0 : requestedPartition;
            int endPartition = (requestedPartition == -1) ? partitionCount - 1 : requestedPartition;

            for (int p = startPartition; p <= endPartition; p++) {
                // Fetch offset from the GroupCoordinator
                long nextRecord = groupCoordinator.getOffset(consumeGroup, consumeTopic, p);

                List<Long> segments = getOrCreateSegments(consumeTopic, p);

                boolean found = false;

                for (int i = 0; i < segments.size(); i++) {
                    long baseOffset = segments.get(i);
                    long nextBaseOffset = (i + 1 < segments.size()) ? segments.get(i + 1) : Long.MAX_VALUE;

                    if (nextRecord >= nextBaseOffset)
                        continue;

                    Path segmentFile = getSegmentFile(consumeTopic, p, baseOffset);
                    try (RandomAccessFile f = new RandomAccessFile(segmentFile.toFile(), "r")) {
                        long fileLength = f.length();
                        long pos = 0; // Start from the beginning of the segment

                        while (pos + 4 <= fileLength) // Check for at least a length field
                        {
                            f.seek(pos);
                            int totalLen = f.readInt();

                            if (pos + 4 + totalLen > fileLength) {
                                // Incomplete record at the end of the file.
                                break;
                            }

                            int crcValue = f.readInt();
                            long recordId = f.readLong();
                            long timestamp = f.readLong();
                            
                            int keyLen = f.readInt();
                            if (keyLen < 0 || keyLen > 1024) { // Sanity check for key length
                                break;
                            }
                            byte[] keyBytes = new byte[keyLen];
                            f.readFully(keyBytes);
                            String key = new String(keyBytes, StandardCharsets.UTF_8);

                            int len = f.readInt();
                            if (len < 0 || len > 100_000_000) {
                                // Data corruption or not at a record boundary.
                                break;
                            }
                            
                            // The record length check already ensures we have enough bytes
                            
                            /* skip if not yet reached */
                            if (recordId < nextRecord) {
                                pos += 4 + totalLen;
                                continue;
                            }

                            byte[] payloadBytes = new byte[len];
                            f.readFully(payloadBytes);

                            // Verify CRC32
                            Checksum crc = new CRC32();
                            crc.update(payloadBytes, 0, payloadBytes.length);
                            if ((int) crc.getValue() != crcValue) {
                                System.err.println("CRC mismatch for record " + recordId + " in " + segmentFile + ". Skipping.");
                                pos += 4 + totalLen;
                                continue; // Skip corrupted record
                            }

                            /* respect toOffset */
                            if (consumeToOffset >= 0 && recordId > consumeToOffset) {
                                pos += 4 + totalLen;
                                continue;
                            }

                            /* deliver */
                            deliverOffset = recordId;
                            deliverBuf = payloadBytes;
                            consumePartition = p;

                            write("+MSG " + recordId + ' ' + len + ' ' + p + ' ' + key + '\n');
                            out.write(payloadBytes);
                            out.write('\n');
                            out.flush();

                            found = true;
                            break;
                        }
                        if (found)
                            break;

                        /* early abort if next segment is already too far */
                        if (consumeToOffset >= 0 && pos > consumeToOffset)
                            break;
                    } catch (IOException e) {
                        // skip segment
                    }
                }
                if (found)
                    return;
            }

            /* nothing found */
            if (consumeToOffset >= 0) {
                write("+END " + consumeToOffset + "\n");
            } else {
                write("+EMPTY\n");
            }
        }

        /* ---------- ACK ---------- */
        private void handleAck() throws IOException {
            if (consumeTopic == null) {
                write("-ERR no outstanding msg\n");
                return;
            }
            // Commit offset via the GroupCoordinator
            groupCoordinator.commitOffset(consumeGroup, consumeTopic, consumePartition, deliverOffset + 1);
            write("+OK\n");
            consumeTopic = null;
        }

        /* ---------- SET_OFFSET ---------- */
        private void handleSetOffset(String params) throws IOException {
            String[] parts = params.split(" ", 3);
            if (parts.length < 2) {
                write("-ERR bad format, expected SET_OFFSET topic:group offset|BEGIN\n");
                return;
            }
            String topicGroup = parts[0];
            String offsetSpec = parts[1];

            String[] topicParts = topicGroup.split(":");
            if (topicParts.length != 2) {
                write("-ERR bad format, expected topic:group\n");
                return;
            }
            String topic = topicParts[0];
            String group = topicParts[1];

            SafeRandomAccessFile[] files = topicFiles(topic);
            if (files == null) {
                write("-ERR topic not found\n");
                return;
            }
            for (int p = 0; p < partitionCount; p++) {
                long offset;
                if ("BEGIN".equals(offsetSpec)) {
                    offset = 0;
                } else {
                    try {
                        offset = Long.parseLong(offsetSpec);
                    } catch (NumberFormatException e) {
                        write("-ERR invalid offset format\n");
                        return;
                    }
                }
                // Commit offset via the GroupCoordinator
                groupCoordinator.commitOffset(group, topic, p, offset);
            }
            // No longer need to save immediately, as it's handled by the coordinator
            write("+OK\n");
        }
    }

    /* --------------- compression helpers (no-op) --------------- */
    private static byte[] decompress(byte[] data) { return data; }
    private static byte[] compress(byte[] raw)     { return raw; }

    /* --------------- client helper (unchanged) --------------- */
    public static class Client {
        private final String host;
        private final int port;
        private final MiniKafkaWithSegmentsFixed serverLocal;
        private Client(String host, int port, MiniKafkaWithSegmentsFixed serverLocal) {
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
                out.writeBytes("TOPIC" + topic + '<' + (key == null ? "" : key) + '|'
                        + new String(payload, StandardCharsets.UTF_8) + ">\n");
                out.flush();
                String resp = new Scanner(in).nextLine();
                return Long.parseLong(resp.split(" ")[1]);
            }
        }
    }
    public static Client connect(String host, int port) {
        return new Client(host, port, null);
    }
    public static Client connectLocal(MiniKafkaWithSegmentsFixed server) {
        return new Client(null, 0, server);
    }

    /* --------------- main --------------- */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                    "usage: java MiniKafkaWithSegmentsFixed <port> <data-dir> [partitionCount] [maxSegmentSizeMB] [maxSegmentCount]");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        Path dir = Paths.get(args[1]);
        int parts = args.length > 2 ? Integer.parseInt(args[2]) : 4;
        long maxSegmentSizeMB = args.length > 3 ? Long.parseLong(args[3]) : 100;
        int maxSegmentCount = args.length > 4 ? Integer.parseInt(args[4]) : 10;

        new MiniKafkaWithSegmentsFixed(port, dir, parts, maxSegmentSizeMB * 1024 * 1024, maxSegmentCount).serve();
    }
 /* --------------- thread-safe RAF wrapper (unchanged) --------------- */
    private static class SafeRandomAccessFile {
        private final RandomAccessFile delegate;
        private final ReentrantLock lock = new ReentrantLock();
        SafeRandomAccessFile(RandomAccessFile file) { this.delegate = file; }
        void writeLong(long v) throws IOException { lock.lock(); try { delegate.writeLong(v); } finally { lock.unlock(); } }
        void writeInt(int v)  throws IOException { lock.lock(); try { delegate.writeInt(v); } finally { lock.unlock(); } }
        void write(byte[] b)  throws IOException { lock.lock(); try { delegate.write(b); }   finally { lock.unlock(); } }
        long length()         throws IOException { lock.lock(); try { return delegate.length(); } finally { lock.unlock(); } }
        void seek(long p)     throws IOException { lock.lock(); try { delegate.seek(p); } finally { lock.unlock(); } }
        long readLong()       throws IOException { lock.lock(); try { return delegate.readLong(); } finally { lock.unlock(); } }
        int  readInt()        throws IOException { lock.lock(); try { return delegate.readInt(); } finally { lock.unlock(); } }
        void readFully(byte[] b) throws IOException { lock.lock(); try { delegate.readFully(b); } finally { lock.unlock(); } }
        void close()          throws IOException { lock.lock(); try { delegate.close(); } finally { lock.unlock(); } }
        RandomAccessFile getDelegate() { return delegate; }
        boolean isOpen() { return delegate.getChannel().isOpen(); }
    }
}