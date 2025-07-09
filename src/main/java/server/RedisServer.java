package server;

import command.CommandProcessor;
import config.ServerConfig;
import lombok.RequiredArgsConstructor;
import protocol.RespProtocol;
import rdb.RdbLoader;
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
public class RedisServer {
    
    private final ServerConfig config;
    private final StorageManager storageManager;
    private final StreamsManager streamsManager;
    private final CommandProcessor commandProcessor;
    private final RdbLoader rdbLoader;
    
    public RedisServer(ServerConfig config) {
        this.config = config;
        this.storageManager = new StorageManager();
        this.streamsManager = new StreamsManager();
        this.commandProcessor = new CommandProcessor(config, storageManager, streamsManager);
        this.rdbLoader = new RdbLoader(config, storageManager);
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
            startReplicationHandshake();
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
     * master와의 replication handshake를 시작합니다.
     */
    private void startReplicationHandshake() {
        new Thread(() -> {
            try {
                System.out.println("Connecting to master " + config.getMasterHost() + ":" + config.getMasterPort());
                
                Socket masterSocket = new Socket(config.getMasterHost(), config.getMasterPort());
                OutputStream outputStream = masterSocket.getOutputStream();
                BufferedReader inputStream = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
                
                System.out.println("Connected to master. Starting handshake...");
                
                // Stage 18: PING 명령어를 RESP Array 형식으로 전송
                sendPingToMaster(outputStream);
                
                // master의 RESP 응답 읽기 (+PONG\r\n 예상)
                String response = readRespResponse(inputStream);
                System.out.println("Master responded to PING: " + response);
                
                // 연결 유지 (후속 stage에서 추가 handshake 진행)
                // TODO: Stage 19, 20에서 REPLCONF와 PSYNC 구현
                
            } catch (IOException e) {
                System.err.println("Failed to connect to master: " + e.getMessage());
                e.printStackTrace();
            }
        }).start();
    }
    
    /**
     * master에게 PING 명령어를 RESP Array 형식으로 전송합니다.
     */
    private void sendPingToMaster(OutputStream outputStream) throws IOException {
        // PING 명령어를 RESP Array로 인코딩: *1\r\n$4\r\nPING\r\n
        String pingCommand = "*1\r\n$4\r\nPING\r\n";
        outputStream.write(pingCommand.getBytes());
        outputStream.flush();
        System.out.println("Sent PING to master: " + pingCommand.replace("\r\n", "\\r\\n"));
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
        
        // RESP Simple String (+PONG) 처리
        if (line.startsWith("+")) {
            return response.toString();
        }
        
        // 다른 RESP 타입도 처리 가능하도록 확장
        // 현재는 Simple String만 처리
        return response.toString();
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
                System.out.println("Received: " + line);
                
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
                            System.out.println("Sent: " + response.trim());
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
} 