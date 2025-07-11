package server;

import command.CommandHandler;
import config.ServerConfig;
import rdb.RdbLoader;
import replication.ReplicationClient;
import service.ReplicationService;
import service.StorageService;
import service.StreamsService;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Redis 서버의 메인 클래스
 * 서버 시작, 클라이언트 연결 처리를 담당
 */
public class RedisServer {
    
    private final ServerConfig config;
    private final StorageService storageService;
    private final StreamsService streamsService;
    private final ReplicationService replicationService;
    private final CommandHandler commandHandler;
    private final RdbLoader rdbLoader;
    private final ReplicationClient replicationClient;
    
    public RedisServer(ServerConfig config) {
        this.config = config;
        this.storageService = new StorageService();
        this.streamsService = new StreamsService();
        this.replicationService = new ReplicationService(config);
        this.commandHandler = new CommandHandler(config, storageService, replicationService, streamsService);
        this.rdbLoader = new RdbLoader(config, storageService);
        this.replicationClient = new ReplicationClient(config, commandHandler); 
    }
    
    /**
     * 서버를 시작합니다.
     */
    public void start() {
        rdbLoader.loadRdbFile();
        
        System.out.println("Redis server listening on port " + config.getPort());
        
        if (config.isReplica()) {
            replicationClient.startHandshake();
        }
        
        try (ServerSocket serverSocket = new ServerSocket(config.getPort())) {
            serverSocket.setReuseAddress(true);
            
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ClientHandler clientHandler = new ClientHandler(clientSocket, commandHandler, replicationService);
                    new Thread(clientHandler).start();
                } catch (IOException e) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Could not listen on port " + config.getPort() + ": " + e.getMessage());
        }
    }
} 