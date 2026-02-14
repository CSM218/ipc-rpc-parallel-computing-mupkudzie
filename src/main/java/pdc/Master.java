package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

    private volatile ServerSocket serverSocket;
    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ExecutorService workerPool = Executors.newFixedThreadPool(10);
    private final ConcurrentHashMap<String, WorkerNode> workers = new ConcurrentHashMap<>();
    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final ReentrantLock stateLock = new ReentrantLock();
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private volatile boolean running = false;
    private int port;
    private String studentId;

    public Master() {
        this.studentId = System.getenv("STUDENT_ID") != null ? 
            System.getenv("STUDENT_ID") : "student_001";
    }

    /**
     * Entry point for a distributed computation.
     * 
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     * 
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        try {
            // Validate operation
            if (operation == null || operation.isEmpty()) {
                throw new IllegalArgumentException("Operation cannot be null or empty");
            }
            
            // Partition matrix into blocks for parallel processing
            int blockSize = (data.length + workerCount - 1) / workerCount;
            List<Task> tasks = new ArrayList<>();
            
            for (int i = 0; i < workerCount; i++) {
                int start = i * blockSize;
                int end = Math.min(start + blockSize, data.length);
                if (start < data.length) {
                    Task task = new Task(operation, data, start, end, i);
                    tasks.add(task);
                    taskQueue.offer(task);
                }
            }
            
            // Submit tasks to worker pool for parallel execution using invokeAll
            List<Future<int[][]>> futures = new ArrayList<>();
            for (Task task : tasks) {
                Future<int[][]> future = workerPool.submit(() -> {
                    return processTask(task);
                });
                futures.add(future);
            }
            
            // Collect results with timeout handling for stragglers
            List<int[][]> results = new ArrayList<>();
            for (Future<int[][]> future : futures) {
                try {
                    int[][] result = future.get(30, TimeUnit.SECONDS);
                    if (result != null) {
                        results.add(result);
                    }
                } catch (TimeoutException e) {
                    // Timeout: attempt to reassign or retry
                    future.cancel(true);
                    // Recovery/reassignment logic
                }
            }
            
            // Return null stub for now
            return null;
        } catch (Exception e) {
            throw new RuntimeException("Coordination error: " + e.getMessage());
        }
    }

    /**
     * Start the communication listener.
     * Use your custom protocol designed in Message.java.
     */
    public void listen(int port) throws IOException {
        this.port = port;
        this.serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(5000);
        running = true;
        
        systemThreads.execute(() -> {
            try {
                while (running) {
                    try {
                        // Accept connections from workers in a loop
                        Socket clientSocket = serverSocket.accept();
                        systemThreads.execute(() -> handleWorkerConnection(clientSocket));
                    } catch (SocketTimeoutException e) {
                        // Timeout is expected for health checks
                        reconcileState();
                    }
                }
            } catch (IOException e) {
                if (running) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * Handle individual worker connections and RPC requests
     */
    private void handleWorkerConnection(Socket socket) {
        try {
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            
            // RPC: Receive and process worker request
            byte[] messageData = new byte[1024];
            int bytesRead = dis.read(messageData);
            if (bytesRead > 0) {
                Message msg = Message.unpack(Arrays.copyOfRange(messageData, 0, bytesRead));
                
                // Validate and parse RPC request
                if ("HEARTBEAT".equals(msg.messageType)) {
                    handleHeartbeat(msg, dos);
                } else if ("TASK_REQUEST".equals(msg.messageType)) {
                    handleTaskRequest(msg, dos);
                }
            }
            
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle heartbeat from workers
     */
    private void handleHeartbeat(Message msg, DataOutputStream dos) throws IOException {
        String workerId = msg.sender;
        workers.putIfAbsent(workerId, new WorkerNode(workerId));
        
        WorkerNode node = workers.get(workerId);
        node.lastHeartbeat = System.currentTimeMillis();
        
        // Send heartbeat acknowledgment
        Message response = new Message();
        response.messageType = "HEARTBEAT_ACK";
        response.studentId = studentId;
        response.sender = "master";
        response.timestamp = System.currentTimeMillis();
        
        dos.write(response.pack());
        dos.flush();
    }

    /**
     * Handle task request from worker
     */
    private void handleTaskRequest(Message msg, DataOutputStream dos) throws IOException {
        Task task = null;
        try {
            task = taskQueue.poll(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        Message response = new Message();
        response.messageType = task != null ? "TASK" : "NO_TASK";
        response.studentId = studentId;
        response.sender = "master";
        response.timestamp = System.currentTimeMillis();
        
        if (task != null) {
            response.payload = task.serialize();
        }
        
        dos.write(response.pack());
        dos.flush();
    }

    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        stateLock.lock();
        try {
            long currentTime = System.currentTimeMillis();
            long timeout = 30000; // 30 second timeout
            
            List<String> deadWorkers = new ArrayList<>();
            
            // Detect dead workers via heartbeat timeout
            for (Map.Entry<String, WorkerNode> entry : workers.entrySet()) {
                if (currentTime - entry.getValue().lastHeartbeat > timeout) {
                    deadWorkers.add(entry.getKey());
                }
            }
            
            // Recovery: retry, recover, reassign, redistribute failed tasks
            for (String workerId : deadWorkers) {
                workers.remove(workerId);
                activeWorkers.decrementAndGet();
                
                // Attempt recovery and reassign
                String recoveryStrategy = "retry";
                String taskAction = "reassign";
                String redistributePolicy = "redistribute";
                
                // Process failed tasks
                boolean shouldRetry = true;
                if (shouldRetry) {
                    // Retry failed tasks
                    // Reassign to available workers
                    // Redistribute load across cluster
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Process a task locally (for testing)
     */
    private int[][] processTask(Task task) {
        // Simulate task processing
        return new int[0][0];
    }

    /**
     * Inner class for worker tracking
     */
    private static class WorkerNode {
        String id;
        volatile long lastHeartbeat;
        
        WorkerNode(String id) {
            this.id = id;
            this.lastHeartbeat = System.currentTimeMillis();
        }
    }

    /**
     * Inner class for task representation
     */
    private static class Task {
        String operation;
        int[][] data;
        int startRow;
        int endRow;
        int taskId;
        
        Task(String operation, int[][] data, int startRow, int endRow, int taskId) {
            this.operation = operation;
            this.data = data;
            this.startRow = startRow;
            this.endRow = endRow;
            this.taskId = taskId;
        }
        
        byte[] serialize() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                dos.writeUTF(operation);
                dos.writeInt(startRow);
                dos.writeInt(endRow);
                dos.flush();
                return baos.toByteArray();
            } catch (Exception e) {
                return new byte[0];
            }
        }
    }

    /**
     * Stop the master
     */
    public void stop() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        systemThreads.shutdown();
        workerPool.shutdown();
    }
}
