package server;

import command.CommandProcessor;
import config.ServerConfig;
import protocol.RespProtocol;
import rdb.RdbLoader;
import replication.ReplicationClient;
import replication.ReplicationManager;
import storage.StorageManager;
import streams.StreamsManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Redis 서버의 메인 클래스
 * 서버 시작, 클라이언트 연결 처리를 담당
 */
public class RedisServer {
    
    private final ServerConfig config;
    private final StorageManager storageManager;
    private final StreamsManager streamsManager;
    private final CommandProcessor commandProcessor;
    private final RdbLoader rdbLoader;
    private final ReplicationClient replicationClient;
    private final ReplicationManager replicationManager;
    
    public RedisServer(ServerConfig config) {
        this.config = config;
        this.storageManager = new StorageManager();
        this.streamsManager = new StreamsManager();
        this.replicationManager = new ReplicationManager(config);
        this.commandProcessor = new CommandProcessor(config, storageManager, streamsManager, replicationManager);
        this.rdbLoader = new RdbLoader(config, storageManager);
        this.replicationClient = new ReplicationClient(config, commandProcessor);
    }
    
    /**
     * 서버를 시작합니다.
     */
    public void start() {
        // RDB 파일에서 데이터 로드
        rdbLoader.loadRdbFile();
        
        System.out.println("Starting Redis server on port " + config.getPort());
        System.out.println("RDB directory: " + config.getRdbDir());
        System.out.println("RDB filename: " + config.getRdbFilename());
        
        // replica 모드인 경우 master에게 연결
        if (config.isReplica()) {
            replicationClient.startHandshake();
        }
        
        try (ServerSocket serverSocket = createServerSocket(config.getPort())) {
            System.out.println("Redis server started. Waiting for connections...");
            
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());
                    
                    // 클라이언트 연결을 별도의 스레드로 처리
                    new Thread(() -> handleClient(clientSocket)).start();
                } catch (IOException e) {
                    System.err.println("Error handling client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to start Redis server: " + e.getMessage());
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
                System.out.println("Received from " + clientAddress + ": " + line);
                
                try {
                    // RESP 프로토콜 파싱
                    if (line.startsWith("*")) {
                        // 배열 명령어 처리
                        int arrayLength = Integer.parseInt(line.substring(1));
                        List<String> commands = RespProtocol.parseRespArray(reader, arrayLength);
                        
                        if (!commands.isEmpty()) {
                            String command = commands.get(0).toUpperCase();
                            
                            // 레플리카로부터의 ACK 처리
                            if (command.equals("REPLCONF") && commands.size() >= 3 && "ACK".equalsIgnoreCase(commands.get(1))) {
                                replicationManager.processAck(clientSocket, Long.parseLong(commands.get(2)));
                                continue; // ACK는 응답 없음
                            }
                            
                            String response = commandProcessor.processCommand(command, commands);
                            sendResponse(outputStream, response);
                            System.out.println("Sent to " + clientAddress + ": " + response.trim());
                            
                            // PSYNC에 대한 특별 처리: RDB 파일 전송 및 레플리카 등록
                            if (command.equals("PSYNC") && response.startsWith("+FULLRESYNC")) {
                                byte[] rdbFileBytes = RespProtocol.EMPTY_RDB_BYTES;
                                String rdbFilePrefix = "$" + rdbFileBytes.length + "\r\n";
                                
                                outputStream.write(rdbFilePrefix.getBytes());
                                outputStream.write(rdbFileBytes);
                                outputStream.flush();
                                System.out.println("Sent empty RDB file to " + clientAddress);
                                
                                // 이 클라이언트를 레플리카로 등록
                                replicationManager.addReplica(clientSocket);
                            }
                            
                            // 쓰기 명령어를 레플리카에 전파
                            List<String> writeCommands = Arrays.asList("SET", "XADD", "INCR");
                            if (writeCommands.contains(command)) {
                                replicationManager.propagateCommand(commands);
                            }
                        }
                    } else if (line.equals("PING")) {
                        // 단순 텍스트 PING 처리 (이전 호환성)
                        sendResponse(outputStream, RespProtocol.PONG_RESPONSE);
                        System.out.println("Sent: PONG");
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid command format from client " + clientAddress + ": " + e.getMessage());
                    sendResponse(outputStream, RespProtocol.createErrorResponse("invalid command format"));
                } catch (Exception e) {
                    System.err.println("Error processing command from client " + clientAddress + ": " + e.getMessage());
                    sendResponse(outputStream, RespProtocol.createErrorResponse("internal server error"));
                }
            }
            
        } catch (SocketException e) {
            System.out.println("Client disconnected: " + clientAddress);
        } catch (IOException e) {
            System.err.println("Error handling client " + clientAddress + ": " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket " + clientAddress + ": " + e.getMessage());
            }
        }
        
        System.out.println("Client connection closed: " + clientAddress);
    }
    
    /**
     * 클라이언트에게 응답을 전송합니다.
     */
    private void sendResponse(OutputStream outputStream, String response) throws IOException {
        outputStream.write(response.getBytes());
        outputStream.flush();
    }
    
    // propagateCommand 메서드는 ReplicationManager로 이동했으므로 삭제
} 