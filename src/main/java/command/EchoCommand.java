package command;

import protocol.RespProtocol;
import java.util.List;

public class EchoCommand implements Command {
    @Override
    public String execute(List<String> args) {
        if (args.size() != 1) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'echo' command");
        }
        return RespProtocol.createBulkString(args.get(0));
    }
} 