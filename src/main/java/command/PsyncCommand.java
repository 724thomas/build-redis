package command;

import config.ServerConfig;
import protocol.RespProtocol;
import java.util.List;

public class PsyncCommand implements Command {

    private final ServerConfig serverConfig;

    public PsyncCommand(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    @Override
    public String execute(List<String> args) {
        // For simplicity in this stage, we always perform a full resynchronization.
        // A more complete implementation would handle partial resync based on PSYNC args.
        String response = "+FULLRESYNC " + serverConfig.getMasterReplId() + " 0";
        return response;
    }
} 