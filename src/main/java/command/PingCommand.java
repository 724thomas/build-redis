package command;

import protocol.RespProtocol;
import java.util.List;

public class PingCommand implements Command {
    @Override
    public String execute(List<String> args) {
        if (args.isEmpty()) {
            return RespProtocol.createSimpleString("PONG");
        } else {
            return RespProtocol.createBulkString(args.get(0));
        }
    }
} 