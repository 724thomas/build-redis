package command;

import protocol.RespProtocol;
import service.ReplicationService;
import service.StorageService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SetCommand implements Command {
    
    private final StorageService storageService;
    private final ReplicationService replicationService;

    public SetCommand(StorageService storageService, ReplicationService replicationService) {
        this.storageService = storageService;
        this.replicationService = replicationService;
    }

    @Override
    public String execute(List<String> args) {
        if (args.size() < 2) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'set' command");
        }

        String key = args.get(0);
        String value = args.get(1);
        long expiry = -1;

        if (args.size() > 2) {
            for (int i = 2; i < args.size(); i++) {
                if ("px".equalsIgnoreCase(args.get(i)) && i + 1 < args.size()) {
                    try {
                        expiry = Long.parseLong(args.get(i + 1));
                        i++; // Skip next argument
                    } catch (NumberFormatException e) {
                        return RespProtocol.createErrorResponse("value is not an integer or out of range");
                    }
                }
            }
        }

        if (expiry != -1) {
            long expiryTime = System.currentTimeMillis() + expiry;
            storageService.setWithExpiry(key, value, expiryTime);
        } else {
            storageService.set(key, value);
        }
        
        // Propagate the original, full command to replicas
        List<String> fullCommand = new ArrayList<>();
        fullCommand.add("SET");
        fullCommand.addAll(args);
        replicationService.propagateCommand(fullCommand);

        return RespProtocol.createSimpleString("OK");
    }
} 