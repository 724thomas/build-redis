package command;

import protocol.RespProtocol;
import service.StreamsService;
import java.util.List;

public class XreadCommand implements Command {

    private final StreamsService streamsService;

    public XreadCommand(StreamsService streamsService) {
        this.streamsService = streamsService;
    }

    @Override
    public String execute(List<String> args) {
        long blockTimeout = -1;
        int streamsIndex = -1;

        for (int i = 0; i < args.size(); i++) {
            if ("block".equalsIgnoreCase(args.get(i)) && i + 1 < args.size()) {
                try {
                    blockTimeout = Long.parseLong(args.get(i + 1));
                    i++; // skip timeout value
                } catch (NumberFormatException e) {
                    return RespProtocol.createErrorResponse("timeout is not an integer or out of range");
                }
            } else if ("streams".equalsIgnoreCase(args.get(i))) {
                streamsIndex = i;
                break; // Found the streams keyword, rest are keys and IDs
            }
        }

        if (streamsIndex == -1) {
            return RespProtocol.createErrorResponse("syntax error");
        }

        int remainingArgs = args.size() - (streamsIndex + 1);
        if (remainingArgs == 0 || remainingArgs % 2 != 0) {
            return RespProtocol.createErrorResponse("Unbalanced XREAD list of streams");
        }

        int numStreams = remainingArgs / 2;
        List<String> keys = args.subList(streamsIndex + 1, streamsIndex + 1 + numStreams);
        List<String> ids = args.subList(streamsIndex + 1 + numStreams, args.size());

        return streamsService.readStreams(keys, ids, blockTimeout);
    }
} 