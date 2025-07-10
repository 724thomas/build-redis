package command;

import config.ServerConfig;
import protocol.RespProtocol;
import replication.ReplicationManager;
import storage.StorageManager;
import streams.StreamsManager;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Redis 명령어 처리를 담당하는 클래스
 */
public class CommandProcessor {
    
    private final ServerConfig config;
    private final StorageManager storageManager;
    private final StreamsManager streamsManager;
    private final ReplicationManager replicationManager;
    
    public CommandProcessor(ServerConfig config, StorageManager storageManager, StreamsManager streamsManager, ReplicationManager replicationManager) {
        this.config = config;
        this.storageManager = storageManager;
        this.streamsManager = streamsManager;
        this.replicationManager = replicationManager;
    }
    
    /**
     * Redis 명령어를 처리하고 응답을 생성합니다.
     */
    public String processCommand(String command, List<String> args) {
        switch (command.toUpperCase()) {
            case "PING":
                return handlePingCommand();
            case "ECHO":
                return handleEchoCommand(args);
            case "SET":
                handleSetCommand(args); // 응답은 OK로 고정, 전파는 호출자(RedisServer)가 담당
                return RespProtocol.OK_RESPONSE;
            case "GET":
                return handleGetCommand(args);
            case "TYPE":
                return handleTypeCommand(args);
            case "CONFIG":
                return handleConfigCommand(args);
            case "KEYS":
                return handleKeysCommand(args);
            case "INFO":
                return handleInfoCommand(args);
            case "REPLCONF":
                return handleReplconfCommand(args);
            case "PSYNC":
                return handlePsyncCommand(args);
            case "XADD":
                return handleXAddCommand(args);
            case "XRANGE":
                return handleXRangeCommand(args);
            case "XREAD":
                return handleXReadCommand(args);
            case "WAIT":
                return handleWaitCommand(args);
            default:
                return RespProtocol.createErrorResponse("unknown command '" + command + "'");
        }
    }

    private String handleWaitCommand(List<String> args) {
        if (args.size() < 3) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'wait' command");
        }
        try {
            int numReplicas = Integer.parseInt(args.get(1));
            long timeout = Long.parseLong(args.get(2));
            
            int ackReplicas = replicationManager.waitForReplicas(numReplicas, timeout);
            return RespProtocol.createInteger(ackReplicas);
            
        } catch (NumberFormatException e) {
            return RespProtocol.createErrorResponse("value is not an integer or out of range");
        }
    }
    
    private String handleTypeCommand(List<String> args) {
        if (args.size() < 2) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'type' command");
        }
        String key = args.get(1);

        if (streamsManager.exists(key)) {
            return RespProtocol.createSimpleString("stream");
        }

        if (storageManager.exists(key)) {
            return RespProtocol.createSimpleString("string");
        }
        
        return RespProtocol.createSimpleString("none");
    }

    /**
     * PING 명령어 처리
     */
    private String handlePingCommand() {
        return RespProtocol.PONG_RESPONSE;
    }
    
    /**
     * ECHO 명령어 처리
     */
    private String handleEchoCommand(List<String> args) {
        if (args.size() >= 2) {
            String value = args.get(1);
            return RespProtocol.createBulkString(value);
        }
        return RespProtocol.createBulkString(null); // null bulk string
    }
    
    /**
     * SET 명령어를 처리합니다. PX 옵션을 지원합니다.
     * 형식: SET key value [PX milliseconds]
     */
    private void handleSetCommand(List<String> args) {
        if (args.size() < 3) {
            // 에러 응답 대신, 예외를 던지거나 처리 방식을 변경할 수 있음
            // 여기서는 간단히 처리를 중단. 응답은 호출자에서 OK로 보냄.
            return;
        }
        
        String key = args.get(1);
        String value = args.get(2);
        
        // PX 옵션 확인
        if (args.size() >= 5 && "PX".equalsIgnoreCase(args.get(3))) {
            try {
                long expireInMs = Long.parseLong(args.get(4));
                long expiryTime = System.currentTimeMillis() + expireInMs;
                
                storageManager.setWithExpiry(key, value, expiryTime);
            } catch (NumberFormatException e) {
                // 에러 처리
            }
        } else {
            // 일반 SET (만료 시간 없음)
            storageManager.set(key, value);
        }
    }
    
    /**
     * GET 명령어를 처리합니다. 만료된 키는 자동으로 삭제합니다.
     */
    private String handleGetCommand(List<String> args) {
        if (args.size() < 2) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'GET' command");
        }
        
        String key = args.get(1);
        String value = storageManager.get(key);
        return RespProtocol.createBulkString(value);
    }
    
    /**
     * CONFIG 명령어를 처리합니다.
     */
    private String handleConfigCommand(List<String> args) {
        if (args.size() < 3) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'CONFIG' command");
        }
        
        String subCommand = args.get(1).toUpperCase();
        if (!"GET".equals(subCommand)) {
            return RespProtocol.createErrorResponse("unknown subcommand '" + args.get(1) + "'");
        }
        
        String parameter = args.get(2);
        
        switch (parameter.toLowerCase()) {
            case "dir":
                return RespProtocol.createRespArray(new String[]{"dir", config.getRdbDir()});
            case "dbfilename":
                return RespProtocol.createRespArray(new String[]{"dbfilename", config.getRdbFilename()});
            default:
                return RespProtocol.createEmptyArray(); // empty array for unknown parameters
        }
    }
    
    /**
     * KEYS 명령어를 처리합니다. 패턴 "*"만 지원합니다.
     */
    private String handleKeysCommand(List<String> args) {
        if (args.size() < 2) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'KEYS' command");
        }
        
        String pattern = args.get(1);
        if (!"*".equals(pattern)) {
            return RespProtocol.createErrorResponse("pattern not supported");
        }
        
        // 모든 키를 RESP 배열로 반환
        Set<String> keys = storageManager.getAllKeys();
        List<String> keyList = new ArrayList<>(keys);
        
        return RespProtocol.createRespArray(keyList.toArray(new String[0]));
    }
    
    /**
     * INFO 명령어를 처리합니다.
     */
    private String handleInfoCommand(List<String> args) {
        if (args.size() < 2) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'INFO' command");
        }
        
        String section = args.get(1).toLowerCase();
        
        switch (section) {
            case "replication":
                return buildReplicationInfo();
            default:
                return RespProtocol.createErrorResponse("unknown INFO section '" + section + "'");
        }
    }
    
    /**
     * Redis 복제 정보를 생성합니다.
     */
    private String buildReplicationInfo() {
        StringBuilder replicationInfo = new StringBuilder();
        replicationInfo.append("# Replication\r\n");
        
        if (config.isReplica()) {
            replicationInfo.append("role:slave\r\n");
            replicationInfo.append("master_host:").append(config.getMasterHost()).append("\r\n");
            replicationInfo.append("master_port:").append(config.getMasterPort()).append("\r\n");
            replicationInfo.append("master_link_status:up\r\n");
            replicationInfo.append("master_last_io_seconds_ago:0\r\n");
            replicationInfo.append("master_sync_in_progress:0\r\n");
            replicationInfo.append("slave_repl_offset:0\r\n");
            replicationInfo.append("slave_priority:100\r\n");
            replicationInfo.append("slave_read_only:1\r\n");
        } else {
            replicationInfo.append("role:master\r\n");
            replicationInfo.append("connected_slaves:").append(replicationManager.getReplicaCount()).append("\r\n");
            replicationInfo.append("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n");
            replicationInfo.append("master_repl_offset:").append(replicationManager.getMasterReplOffset()).append("\r\n");
            replicationInfo.append("second_repl_offset:-1\r\n");
            replicationInfo.append("repl_backlog_active:0\r\n");
            replicationInfo.append("repl_backlog_size:1048576\r\n");
            replicationInfo.append("repl_backlog_first_byte_offset:0\r\n");
            replicationInfo.append("repl_backlog_histlen:0\r\n");
        }
        
        return RespProtocol.createBulkString(replicationInfo.toString());
    }
    

    
    /**
     * REPLCONF 명령어를 처리합니다.
     * Stage 21: master가 replica로부터 REPLCONF를 받을 때 +OK 응답
     */
    private String handleReplconfCommand(List<String> args) {
        if (args.size() < 2) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'REPLCONF' command");
        }
        
        // Stage 27: master로부터 REPLCONF GETACK *를 받을 때 ACK 응답
        if ("GETACK".equalsIgnoreCase(args.get(1))) {
            // 이 로직은 레플리카 측에서 실행되어야 함. 현재 CommandProcessor는 마스터/레플리카 구분이 없음.
            // 레플리카 클라이언트(ReplicationClient)에서 직접 처리하거나,
            // 이 CommandProcessor가 자신이 레플리카 모드인지 알아야 함.
            // 지금은 0으로 응답. 실제 오프셋 추적은 ReplicationClient에서 필요.
            return RespProtocol.createRespArray(new String[]{"REPLCONF", "ACK", "0"});
        }
        
        // REPLCONF의 모든 subcommand에 대해 OK 응답 (기존 로직 유지)
        // listening-port, capa 등의 인수는 현재 단계에서는 무시
        return RespProtocol.OK_RESPONSE;
    }
    
    /**
     * PSYNC 명령어를 처리합니다.
     * Stage 22: master가 replica로부터 PSYNC ? -1을 받을 때 FULLRESYNC 응답
     */
    private String handlePsyncCommand(List<String> args) {
        if (args.size() < 3) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'PSYNC' command");
        }
        
        String replId = args.get(1);
        String offset = args.get(2);
        
        // 첫 연결이므로 ? -1이어야 함
        if ("?".equals(replId) && "-1".equals(offset)) {
            // FULLRESYNC <REPL_ID> 0 응답
            String masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
            String response = "+FULLRESYNC " + masterReplId + " 0\r\n";
            return response;
        }
        
        return RespProtocol.createErrorResponse("unsupported PSYNC parameters");
    }
    
    /**
     * Redis Streams XADD 명령어를 처리합니다.
     */
    private String handleXAddCommand(List<String> args) {
        // XADD <key> <id> <field> <value> [...field value...]
        if (args.size() < 5 || (args.size() - 3) % 2 != 0) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'XADD' command");
        }
        
        String streamKey = args.get(1);
        String entryId = args.get(2);
        List<String> fieldValues = args.subList(3, args.size());
        
        return streamsManager.addEntry(streamKey, entryId, fieldValues);
    }
    
    private String handleXRangeCommand(List<String> args) {
        if (args.size() < 4) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'XRANGE' command");
        }
        
        String key = args.get(1);
        String start = args.get(2);
        String end = args.get(3);
        
        return streamsManager.xrange(key, start, end);
    }
    
    /**
     * Redis Streams XREAD 명령어를 처리합니다.
     */
    private String handleXReadCommand(List<String> args) {
        if (args.size() < 4 || !"STREAMS".equalsIgnoreCase(args.get(1))) {
            return RespProtocol.createErrorResponse("wrong arguments for 'XREAD' command");
        }
        
        // XREAD streams streamKey lastId
        String streamKey = args.get(2);
        String lastId = args.get(3);
        
        return streamsManager.readStream(streamKey, lastId);
    }
} 