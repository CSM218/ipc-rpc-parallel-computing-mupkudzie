package pdc;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private String workerId;
    private String masterHost;
    private int masterPort;
    private Socket masterSocket;
    private final ExecutorService threadPool = Executors.newFixedThreadPool(4);
    private volatile boolean connected = false;

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        this.masterHost = masterHost;
        this.masterPort = port;
        
        try {
            // Get student ID from environment
            String studentId = System.getenv("STUDENT_ID") != null ? 
                System.getenv("STUDENT_ID") : "worker_" + System.nanoTime();
            this.workerId = studentId;
            
            // Socket connection to master
            masterSocket = new Socket(masterHost, port);
            connected = true;
            
            // Send handshake RPC request
            sendHandshakeRequest();
            
            // Start background thread for receiving tasks
            threadPool.execute(this::receiveTasksLoop);
            
            // Start heartbeat thread
            threadPool.execute(this::heartbeatLoop);
            
        } catch (IOException e) {
            e.printStackTrace();
            connected = false;
        }
    }

    /**
     * Send RPC handshake request to master
     */
    private void sendHandshakeRequest() throws IOException {
        Message handshake = new Message();
        handshake.messageType = "HANDSHAKE";
        handshake.studentId = workerId;
        handshake.sender = workerId;
        handshake.timestamp = System.currentTimeMillis();
        
        DataOutputStream dos = new DataOutputStream(masterSocket.getOutputStream());
        dos.write(handshake.pack());
        dos.flush();
    }

    /**
     * Receive and process tasks in a loop
     */
    private void receiveTasksLoop() {
        try {
            DataInputStream dis = new DataInputStream(masterSocket.getInputStream());
            
            while (connected) {
                try {
                    byte[] buffer = new byte[4096];
                    int bytesRead = dis.read(buffer);
                    
                    if (bytesRead > 0) {
                        Message msg = Message.unpack(java.util.Arrays.copyOfRange(buffer, 0, bytesRead));
                        
                        if ("TASK".equals(msg.messageType)) {
                            // RPC: execute received task
                            threadPool.execute(() -> execute());
                        }
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send periodic heartbeat to master for health checks
     */
    private void heartbeatLoop() {
        try {
            while (connected) {
                Thread.sleep(10000); // Heartbeat every 10 seconds
                
                Message heartbeat = new Message();
                heartbeat.messageType = "HEARTBEAT";
                heartbeat.studentId = workerId;
                heartbeat.sender = workerId;
                heartbeat.timestamp = System.currentTimeMillis();
                
                DataOutputStream dos = new DataOutputStream(masterSocket.getOutputStream());
                dos.write(heartbeat.pack());
                dos.flush();
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        try {
            // Simulate task execution
            Thread.sleep(100);
            
            // Send task completion message back to master
            Message completion = new Message();
            completion.messageType = "TASK_COMPLETE";
            completion.studentId = workerId;
            completion.sender = workerId;
            completion.timestamp = System.currentTimeMillis();
            
            if (connected && masterSocket != null) {
                DataOutputStream dos = new DataOutputStream(masterSocket.getOutputStream());
                dos.write(completion.pack());
                dos.flush();
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gracefully disconnect from cluster
     */
    public void leaveCluster() {
        connected = false;
        threadPool.shutdown();
        try {
            if (!threadPool.isShutdown()) {
                threadPool.shutdownNow();
            }
            if (masterSocket != null && !masterSocket.isClosed()) {
                masterSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Main method for testing
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: Worker <master_host> <master_port>");
            return;
        }
        
        String masterHost = args[0];
        int masterPort = Integer.parseInt(args[1]);
        
        Worker worker = new Worker();
        worker.joinCluster(masterHost, masterPort);
        
        // Keep worker running
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            worker.leaveCluster();
        }
    }
}
