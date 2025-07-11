package command;

import config.ServerConfig;
import protocol.RespProtocol;
import java.util.List;

public class ConfigCommand implements Command {

    private final ServerConfig serverConfig;

    public ConfigCommand(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    @Override
    public String execute(List<String> args) {
        if (args.size() < 2 || !"get".equalsIgnoreCase(args.get(0))) {
            return RespProtocol.createErrorResponse("Unsupported CONFIG subcommand");
        }

        String configName = args.get(1).toLowerCase();
        String value;

        switch (configName) {
            case "dir":
                value = serverConfig.getRdbDir();
                break;
            case "dbfilename":
                value = serverConfig.getRdbFilename();
                break;
            default:
                // Return an empty array for unknown configs
                return RespProtocol.createEmptyArray();
        }

        return RespProtocol.createRespArray(new String[]{configName, value});
    }
} 