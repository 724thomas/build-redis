package command;

import protocol.RespProtocol;
import service.StorageService;
import java.util.List;
import java.util.Set;

public class KeysCommand implements Command {

    private final StorageService storageService;

    public KeysCommand(StorageService storageService) {
        this.storageService = storageService;
    }

    @Override
    public String execute(List<String> args) {
        if (args.size() != 1 || !"*".equals(args.get(0))) {
            return RespProtocol.createErrorResponse("Only 'KEYS *' is supported");
        }

        Set<String> keys = storageService.getAllKeys();
        return RespProtocol.createRespArray(keys.toArray(new String[0]));
    }
} 