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
import java.util.ArrayList;
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
                    ClientHandler clientHandler = new ClientHandler(clientSocket, commandProcessor, replicationManager);
                    new Thread(clientHandler).start();
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
} 