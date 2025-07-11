package command;

import protocol.RespProtocol;
import service.StorageService;
import service.StreamsService;
import java.util.List;

public class TypeCommand implements Command {

    private final StorageService storageService;
    private final StreamsService streamsService;

    public TypeCommand(StorageService storageService, StreamsService streamsService) {
        this.storageService = storageService;
        this.streamsService = streamsService;
    }

    @Override
    public String execute(List<String> args) {
        if (args.size() != 1) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'type' command");
        }
        String key = args.get(0);

        if (storageService.exists(key)) {
            return RespProtocol.createSimpleString("string");
        } else if (streamsService.exists(key)) {
            return RespProtocol.createSimpleString("stream");
        } else {
            return RespProtocol.createSimpleString("none");
        }
    }
} 