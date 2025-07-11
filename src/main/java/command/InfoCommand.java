package command;

import config.ServerConfig;
import protocol.RespProtocol;
import service.ReplicationService;
import java.util.List;

public class InfoCommand implements Command {

    private final ServerConfig serverConfig;
    private final ReplicationService replicationService;

    public InfoCommand(ServerConfig serverConfig, ReplicationService replicationService) {
        this.serverConfig = serverConfig;
        this.replicationService = replicationService;
    }

    @Override
    public String execute(List<String> args) {
        if (args.size() > 1) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'info' command");
        }

        if (args.isEmpty() || "replication".equalsIgnoreCase(args.get(0))) {
            StringBuilder infoBuilder = new StringBuilder();
            infoBuilder.append("role:").append(serverConfig.getRole()).append("\r\n");
            infoBuilder.append("master_replid:").append(serverConfig.getMasterReplId()).append("\r\n");
            infoBuilder.append("master_repl_offset:").append(replicationService.getMasterReplOffset()).append("\r\n");
            return RespProtocol.createBulkString(infoBuilder.toString());
        } else {
            return RespProtocol.createBulkString(""); // 다른 섹션은 빈 문자열 반환
        }
    }
} 