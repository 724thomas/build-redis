package command;

import protocol.RespProtocol;
import service.StreamsService;
import java.util.List;

public class XrangeCommand implements Command {

    private final StreamsService streamsService;

    public XrangeCommand(StreamsService streamsService) {
        this.streamsService = streamsService;
    }

    @Override
    public String execute(List<String> args) {
        if (args.size() != 3) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'xrange' command");
        }

        String streamKey = args.get(0);
        String startId = args.get(1);
        String endId = args.get(2);

        return streamsService.xrange(streamKey, startId, endId);
    }
} 