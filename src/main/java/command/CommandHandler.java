package command;

import config.ServerConfig;
import protocol.RespProtocol;
import service.ReplicationService;
import service.StorageService;
import service.StreamsService;

import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 모든 Redis 명령어를 관리하고, 요청에 맞는 명령어를 찾아 실행하는 핸들러 클래스
 */
public class CommandHandler {
    
    private final Map<String, Command> commandMap = new HashMap<>();
    private final ServerConfig serverConfig;
    private final ReplicationService replicationService;
    private final StreamsService streamsService;
    private final StorageService storageService;

    public CommandHandler(ServerConfig serverConfig, StorageService storageService, ReplicationService replicationService, StreamsService streamsService) {
        this.serverConfig = serverConfig;
        this.replicationService = replicationService;
        this.streamsService = streamsService;
        this.storageService = storageService;

        // Stateless Commands
        commandMap.put("ping", new PingCommand());
        commandMap.put("echo", new EchoCommand());
        commandMap.put("set", new SetCommand(storageService, replicationService));
        commandMap.put("get", new GetCommand(storageService));
        commandMap.put("info", new InfoCommand(serverConfig, replicationService));
        commandMap.put("psync", new PsyncCommand(serverConfig));
        commandMap.put("wait", new WaitCommand(replicationService));
        commandMap.put("config", new ConfigCommand(serverConfig));
        commandMap.put("keys", new KeysCommand(storageService));
        commandMap.put("type", new TypeCommand(storageService, streamsService));
        commandMap.put("xadd", new XaddCommand(streamsService, replicationService));
        commandMap.put("xrange", new XrangeCommand(streamsService));
        commandMap.put("xread", new XreadCommand(streamsService));
        commandMap.put("incr", new IncrCommand(storageService, replicationService));

        // Stateful commands (like REPLCONF) are handled separately.
    }
    
    public String handleCommand(String commandName, List<String> args, Socket clientSocket) {
        String lowerCaseCommand = commandName.toLowerCase();
        
        // Handle stateful commands separately
        if ("replconf".equals(lowerCaseCommand)) {
            Command command = new ReplconfCommand(replicationService, clientSocket);
            return command.execute(args);
        }

        Command command = commandMap.get(lowerCaseCommand);
        if (command == null) {
            return RespProtocol.createErrorResponse("unknown command '" + commandName + "'");
        }
        
        try {
            return command.execute(args);
        } catch (Exception e) {
            return RespProtocol.createErrorResponse("ERR " + e.getMessage());
        }
    }
} 