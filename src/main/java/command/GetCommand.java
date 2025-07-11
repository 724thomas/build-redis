package command;

import protocol.RespProtocol;
import service.StorageService;
import java.util.List;

public class GetCommand implements Command {

    private final StorageService storageService;

    public GetCommand(StorageService storageService) {
        this.storageService = storageService;
    }

    @Override
    public String execute(List<String> args) {
        if (args.size() != 1) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'get' command");
        }
        String key = args.get(0);
        String value = storageService.get(key);

        if (value == null) {
            return RespProtocol.createNullBulkString();
        } else {
            return RespProtocol.createBulkString(value);
        }
    }
} 