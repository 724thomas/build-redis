package command;

import protocol.RespProtocol;
import service.ReplicationService;
import service.StreamsService;

import java.util.ArrayList;
import java.util.List;

public class XaddCommand implements Command {

    private final StreamsService streamsService;
    private final ReplicationService replicationService;

    public XaddCommand(StreamsService streamsService, ReplicationService replicationService) {
        this.streamsService = streamsService;
        this.replicationService = replicationService;
    }

    @Override
    public String execute(List<String> args) {
        if (args.size() < 3) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'xadd' command");
        }

        String streamKey = args.get(0);
        String entryId = args.get(1);
        List<String> fieldValues = args.subList(2, args.size());

        String response = streamsService.addEntry(streamKey, entryId, fieldValues);

        // Propagate only on success
        if (!response.startsWith("-")) {
            List<String> fullCommand = new ArrayList<>();
            fullCommand.add("XADD");
            fullCommand.addAll(args);
            replicationService.propagateCommand(fullCommand);
        }

        return response;
    }
} 