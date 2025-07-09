package server;

import command.CommandProcessor;
import config.ServerConfig;
import lombok.extern.slf4j.Slf4j;
import protocol.RespProtocol;
import rdb.RdbLoader;
import replication.ReplicationClient;
import storage.StorageManager;
import streams.StreamsManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;

/**
 * Redis 서버의 메인 클래스
 * 서버 시작, 클라이언트 연결 처리를 담당
 */
@Slf4j
public class RedisServer {
    
    private final ServerConfig config;
    private final StorageManager storageManager;
    private final StreamsManager streamsManager;
    private final CommandProcessor commandProcessor;
    private final RdbLoader rdbLoader;
    private final ReplicationClient replicationClient;
    
    public RedisServer(ServerConfig config) {
        this.config = config;
        this.storageManager = new StorageManager();
        this.streamsManager = new StreamsManager();
        this.commandProcessor = new CommandProcessor(config, storageManager, streamsManager);
        this.rdbLoader = new RdbLoader(config, storageManager);
        this.replicationClient = new ReplicationClient(config);
    }
    
    /**
     * 서버를 시작합니다.
     */
    public void start() {
        // RDB 파일에서 데이터 로드
        rdbLoader.loadRdbFile();
        
        log.info("Starting Redis server on port {}", config.getPort());
        log.info("RDB directory: {}", config.getRdbDir());
        log.info("RDB filename: {}", config.getRdbFilename());
        
        // replica 모드인 경우 master에게 연결
        if (config.isReplica()) {
            log.info("Starting in replica mode - connecting to master {}:{}", 
                     config.getMasterHost(), config.getMasterPort());
            replicationClient.startHandshake();
        } else {
            log.info("Starting in master mode");
        }
        
        try (ServerSocket serverSocket = createServerSocket(config.getPort())) {
            log.info("Redis server started. Waiting for connections...");
            
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    log.info("Client connected: {}", clientSocket.getRemoteSocketAddress());
                    
                    // 클라이언트 연결을 별도의 스레드로 처리
                    new Thread(() -> handleClient(clientSocket)).start();
                } catch (IOException e) {
                    log.error("Error handling client connection: {}", e.getMessage());
                }
            }
        } catch (IOException e) {
            log.error("Failed to start Redis server: {}", e.getMessage());
        }
    }
    
    /**
     * 서버 소켓을 생성하고 설정합니다.
     */
    private ServerSocket createServerSocket(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        // 서버 재시작 시 'Address already in use' 에러 방지
        serverSocket.setReuseAddress(true);
        return serverSocket;
    }
    
    /**
     * 클라이언트 연결을 처리하고 Redis 명령어에 응답합니다.
     */
    private void handleClient(Socket clientSocket) {
        String clientAddress = clientSocket.getRemoteSocketAddress().toString();
        
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             OutputStream outputStream = clientSocket.getOutputStream()) {
            
            String line;
            while ((line = reader.readLine()) != null) {
                log.debug("Received from {}: {}", clientAddress, line);
                
                try {
                    // RESP 프로토콜 파싱
                    if (line.startsWith("*")) {
                        // 배열 명령어 처리
                        int arrayLength = Integer.parseInt(line.substring(1));
                        List<String> commands = RespProtocol.parseRespArray(reader, arrayLength);
                        
                        if (!commands.isEmpty()) {
                            String command = commands.get(0).toUpperCase();
                            String response = commandProcessor.processCommand(command, commands);
                            sendResponse(outputStream, response);
                            log.debug("Sent to {}: {}", clientAddress, response.trim());
                        }
                    } else if (line.equals("PING")) {
                        // 단순 텍스트 PING 처리 (이전 호환성)
                        sendResponse(outputStream, RespProtocol.PONG_RESPONSE);
                        log.debug("Sent to {}: PONG", clientAddress);
                    }
                } catch (NumberFormatException e) {
                    log.error("Invalid command format from client {}: {}", clientAddress, e.getMessage());
                    sendResponse(outputStream, RespProtocol.createErrorResponse("invalid command format"));
                } catch (Exception e) {
                    log.error("Error processing command from client {}: {}", clientAddress, e.getMessage());
                    sendResponse(outputStream, RespProtocol.createErrorResponse("internal server error"));
                }
            }
            
        } catch (SocketException e) {
            log.info("Client disconnected: {}", clientAddress);
        } catch (IOException e) {
            log.error("Error handling client {}: {}", clientAddress, e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                log.error("Error closing client socket {}: {}", clientAddress, e.getMessage());
            }
        }
        
        log.info("Client connection closed: {}", clientAddress);
    }
    
    /**
     * 클라이언트에게 응답을 전송합니다.
     */
    private void sendResponse(OutputStream outputStream, String response) throws IOException {
        outputStream.write(response.getBytes());
        outputStream.flush();
    }
} 