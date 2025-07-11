package command;

import protocol.RespProtocol;
import service.ReplicationService;
import service.StorageService;

import java.util.List;

public class IncrCommand implements Command {

    private final StorageService storageService;
    private final ReplicationService replicationService;

    public IncrCommand(StorageService storageService, ReplicationService replicationService) {
        this.storageService = storageService;
        this.replicationService = replicationService;
    }

    @Override
    public String execute(List<String> args) {
        if (args.size() != 1) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'incr' command");
        }
        String key = args.get(0);
        String value = storageService.get(key);

        if (value == null) {
            // Key doesn't exist, set to 1
            storageService.set(key, "1");
            replicationService.propagateCommand(List.of("SET", key, "1"));
            return RespProtocol.createInteger(1);
        }

        try {
            long number = Long.parseLong(value);
            number++;
            storageService.set(key, String.valueOf(number));
            // INCR is a write command, so we need to propagate it.
            // Redis propagates INCR itself, not the resulting SET.
            replicationService.propagateCommand(List.of("INCR", key));
            return RespProtocol.createInteger((int) number);
        } catch (NumberFormatException e) {
            return RespProtocol.createErrorResponse("value is not an integer or out of range");
        }
    }
}
