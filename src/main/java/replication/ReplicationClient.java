package replication;

import config.ServerConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Redis 복제 클라이언트
 * 마스터와의 연결 및 핸드셰이크를 담당합니다.
 */
public class ReplicationClient {
    
    private final ServerConfig config;
    
    public ReplicationClient(ServerConfig config) {
        this.config = config;
    }
    
    /**
     * 마스터와의 복제 핸드셰이크를 시작합니다.
     */
    public void startHandshake() {
        new Thread(this::performHandshake).start();
    }
    
    /**
     * 마스터와의 핸드셰이크를 수행합니다.
     */
    private void performHandshake() {
        try {
            System.out.println("Connecting to master " + config.getMasterHost() + ":" + config.getMasterPort());
            
            Socket masterSocket = new Socket(config.getMasterHost(), config.getMasterPort());
            OutputStream outputStream = masterSocket.getOutputStream();
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
            
            System.out.println("Connected to master. Starting handshake...");
            
            // Stage 18: PING 명령어 전송
            sendPing(outputStream);
            String pingResponse = readResponse(inputStream);
            System.out.println("Master responded to PING: " + pingResponse);
            
            // Stage 19: REPLCONF listening-port 전송
            sendReplconfListeningPort(outputStream);
            String replconfPortResponse = readResponse(inputStream);
            System.out.println("Master responded to REPLCONF listening-port: " + replconfPortResponse);
            
            // Stage 19: REPLCONF capa psync2 전송
            sendReplconfCapabilities(outputStream);
            String replconfCapaResponse = readResponse(inputStream);
            System.out.println("Master responded to REPLCONF capa: " + replconfCapaResponse);
            
            // Stage 20: PSYNC ? -1 전송
            sendPsync(outputStream);
            String psyncResponse = readResponse(inputStream);
            System.out.println("Master responded to PSYNC: " + psyncResponse);
            
            // 연결 유지 (후속 stage에서 RDB 파일 수신 등 처리)
            
        } catch (IOException e) {
            System.err.println("Failed to connect to master: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 마스터에게 PING 명령어를 전송합니다.
     */
    private void sendPing(OutputStream outputStream) throws IOException {
        String command = "*1\r\n$4\r\nPING\r\n";
        outputStream.write(command.getBytes());
        outputStream.flush();
        System.out.println("Sent PING to master: " + command.replace("\r\n", "\\r\\n"));
    }
    
    /**
     * 마스터에게 REPLCONF listening-port 명령어를 전송합니다.
     */
    private void sendReplconfListeningPort(OutputStream outputStream) throws IOException {
        String portStr = String.valueOf(config.getPort());
        String command = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + portStr.length() + "\r\n" + portStr + "\r\n";
        outputStream.write(command.getBytes());
        outputStream.flush();
        System.out.println("Sent REPLCONF listening-port to master: " + command.replace("\r\n", "\\r\\n"));
    }
    
    /**
     * 마스터에게 REPLCONF capa psync2 명령어를 전송합니다.
     */
    private void sendReplconfCapabilities(OutputStream outputStream) throws IOException {
        String command = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        outputStream.write(command.getBytes());
        outputStream.flush();
        System.out.println("Sent REPLCONF capa to master: " + command.replace("\r\n", "\\r\\n"));
    }
    
    /**
     * 마스터에게 PSYNC ? -1 명령어를 전송합니다.
     * Stage 20: 첫 연결이므로 replication ID는 ?, offset은 -1로 전송
     */
    private void sendPsync(OutputStream outputStream) throws IOException {
        String command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        outputStream.write(command.getBytes());
        outputStream.flush();
        System.out.println("Sent PSYNC to master: " + command.replace("\r\n", "\\r\\n"));
    }
    
    /**
     * 마스터로부터 응답을 읽습니다.
     */
    private String readResponse(BufferedReader inputStream) throws IOException {
        String line = inputStream.readLine();
        
        if (line == null) {
            throw new IOException("Unexpected end of stream from master");
        }
        
        // RESP Simple String (+OK, +PONG, +FULLRESYNC) 처리
        if (line.startsWith("+")) {
            return line;
        }
        
        // 다른 RESP 타입도 처리 가능하도록 확장
        return line;
    }
} 