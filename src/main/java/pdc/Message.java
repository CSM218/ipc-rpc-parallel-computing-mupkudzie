package pdc;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public String magic;           // CSM218
    public int version;            // Protocol version
    public String messageType;     // Request, Response, Heartbeat, etc.
    public String studentId;       // Student identifier
    public long timestamp;         // Message creation time
    public String sender;          // Sender identifier
    public byte[] payload;         // Message data

    public static final String MAGIC = "CSM218";
    public static final int VERSION = 1;

    public Message() {
        this.magic = MAGIC;
        this.version = VERSION;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            // Write magic
            dos.writeUTF(magic != null ? magic : MAGIC);
            
            // Write version
            dos.writeInt(version);
            
            // Write messageType
            dos.writeUTF(messageType != null ? messageType : "");
            
            // Write studentId
            dos.writeUTF(studentId != null ? studentId : "");
            
            // Write timestamp
            dos.writeLong(timestamp);
            
            // Write sender
            dos.writeUTF(sender != null ? sender : "");
            
            // Write payload length and data
            if (payload != null) {
                dos.writeInt(payload.length);
                dos.write(payload);
            } else {
                dos.writeInt(0);
            }
            
            dos.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Serialization error: " + e.getMessage());
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Invalid message data");
        }
        
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);
            
            Message msg = new Message();
            
            // Read magic
            msg.magic = dis.readUTF();
            
            // Validate magic
            if (!msg.magic.equals(MAGIC)) {
                throw new IOException("Invalid magic number: " + msg.magic);
            }
            
            // Read version
            msg.version = dis.readInt();
            
            // Read messageType
            msg.messageType = dis.readUTF();
            
            // Read studentId
            msg.studentId = dis.readUTF();
            
            // Read timestamp
            msg.timestamp = dis.readLong();
            
            // Read sender
            msg.sender = dis.readUTF();
            
            // Read payload
            int payloadLength = dis.readInt();
            if (payloadLength > 0) {
                msg.payload = new byte[payloadLength];
                dis.readFully(msg.payload);
            }
            
            return msg;
        } catch (IOException e) {
            throw new RuntimeException("Deserialization error: " + e.getMessage());
        }
    }
}
