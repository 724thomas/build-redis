package replication;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 연결된 레플리카의 상태 정보를 담는 데이터 클래스
 */
public class ReplicaInfo {
    private final Socket socket;
    private final OutputStream outputStream;
    private final String address;
    private final AtomicLong ackOffset = new AtomicLong(0);
    
    public ReplicaInfo(Socket socket) throws IOException {
        this.socket = socket;
        this.outputStream = socket.getOutputStream();
        this.address = socket.getRemoteSocketAddress().toString();
    }
    
    public Socket getSocket() {
        return socket;
    }
    
    public OutputStream getOutputStream() {
        return outputStream;
    }
    
    public String getAddress() {
        return address;
    }
    
    public long getAckOffset() {
        return ackOffset.get();
    }
    
    public void setAckOffset(long offset) {
        this.ackOffset.set(offset);
    }
} 