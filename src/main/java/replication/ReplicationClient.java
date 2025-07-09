package replication;

import config.ServerConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Redis replication client
 * replica가 master와의 handshake 처리를 담당
 */
@Slf4j
@RequiredArgsConstructor
public class ReplicationClient {
    
    private final ServerConfig config;
    
    /**
     * master와의 replication handshake를 시작합니다.
     */
    public void startHandshake() {
        new Thread(() -> {
            try {
                log.info("Connecting to master {}:{}", config.getMasterHost(), config.getMasterPort());
                
                try (Socket masterSocket = new Socket(config.getMasterHost(), config.getMasterPort());
                     OutputStream outputStream = masterSocket.getOutputStream();
                     BufferedReader inputStream = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()))) {
                    
                    log.info("Connected to master. Starting handshake...");
                    performHandshake(outputStream, inputStream);
                    log.info("Replication handshake completed successfully");
                    
                    // 연결 유지 (후속 stage에서 추가 로직 구현 예정)
                    keepConnection(masterSocket, inputStream);
                }
                
            } catch (IOException e) {
                log.error("Failed to connect to master: {}", e.getMessage(), e);
            }
        }).start();
    }
    
    /**
     * 3단계 handshake를 수행합니다.
     */
    private void performHandshake(OutputStream outputStream, BufferedReader inputStream) throws IOException {
        // Stage 18: PING 명령어 전송
        sendPingCommand(outputStream);
        String pingResponse = readRespResponse(inputStream);
        log.debug("Master responded to PING: {}", pingResponse);
        
        // Stage 19: REPLCONF listening-port 전송
        sendReplconfListeningPort(outputStream);
        String replconfPortResponse = readRespResponse(inputStream);
        log.debug("Master responded to REPLCONF listening-port: {}", replconfPortResponse);
        
        // Stage 19: REPLCONF capa psync2 전송
        sendReplconfCapabilities(outputStream);
        String replconfCapaResponse = readRespResponse(inputStream);
        log.debug("Master responded to REPLCONF capa: {}", replconfCapaResponse);
        
        // Stage 20: PSYNC ? -1 전송
        sendPsyncCommand(outputStream);
        String psyncResponse = readRespResponse(inputStream);
        log.debug("Master responded to PSYNC: {}", psyncResponse);
    }
    
    /**
     * master에게 PING 명령어를 RESP Array 형식으로 전송합니다.
     */
    private void sendPingCommand(OutputStream outputStream) throws IOException {
        String command = "*1\r\n$4\r\nPING\r\n";
        outputStream.write(command.getBytes());
        outputStream.flush();
        log.debug("Sent PING to master: {}", command.replace("\r\n", "\\r\\n"));
    }
    
    /**
     * master에게 REPLCONF listening-port 명령어를 전송합니다.
     */
    private void sendReplconfListeningPort(OutputStream outputStream) throws IOException {
        String portStr = String.valueOf(config.getPort());
        String command = String.format("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", 
                                      portStr.length(), portStr);
        outputStream.write(command.getBytes());
        outputStream.flush();
        log.debug("Sent REPLCONF listening-port to master: {}", command.replace("\r\n", "\\r\\n"));
    }
    
    /**
     * master에게 REPLCONF capa psync2 명령어를 전송합니다.
     */
    private void sendReplconfCapabilities(OutputStream outputStream) throws IOException {
        String command = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        outputStream.write(command.getBytes());
        outputStream.flush();
        log.debug("Sent REPLCONF capa to master: {}", command.replace("\r\n", "\\r\\n"));
    }
    
    /**
     * master에게 PSYNC ? -1 명령어를 전송합니다.
     */
    private void sendPsyncCommand(OutputStream outputStream) throws IOException {
        String command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        outputStream.write(command.getBytes());
        outputStream.flush();
        log.debug("Sent PSYNC to master: {}", command.replace("\r\n", "\\r\\n"));
    }
    
    /**
     * master로부터 RESP 응답을 읽습니다.
     */
    private String readRespResponse(BufferedReader inputStream) throws IOException {
        StringBuilder response = new StringBuilder();
        String line = inputStream.readLine();
        
        if (line == null) {
            throw new IOException("Unexpected end of stream from master");
        }
        
        response.append(line);
        
        // RESP Simple String (+PONG, +OK, +FULLRESYNC) 처리
        if (line.startsWith("+")) {
            return response.toString();
        }
        
        // RESP Error (-ERR) 처리
        if (line.startsWith("-")) {
            return response.toString();
        }
        
        // RESP Integer (:) 처리
        if (line.startsWith(":")) {
            return response.toString();
        }
        
        // RESP Bulk String ($) 처리
        if (line.startsWith("$")) {
            int length = Integer.parseInt(line.substring(1));
            if (length >= 0) {
                char[] buffer = new char[length];
                inputStream.read(buffer, 0, length);
                response.append("\r\n").append(new String(buffer));
                inputStream.readLine(); // trailing \r\n 읽기
            }
            return response.toString();
        }
        
        return response.toString();
    }
    
    /**
     * 연결을 유지하고 master로부터의 추가 명령을 처리합니다.
     */
    private void keepConnection(Socket masterSocket, BufferedReader inputStream) {
        // 후속 stage에서 RDB 파일 수신 및 command propagation 처리 예정
        log.info("Keeping connection alive for future stages...");
    }
} 