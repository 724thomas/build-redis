package command;

import protocol.RespProtocol;
import service.ReplicationService;
import java.util.List;

public class WaitCommand implements Command {

    private final ReplicationService replicationService;

    public WaitCommand(ReplicationService replicationService) {
        this.replicationService = replicationService;
    }

    @Override
    public String execute(List<String> args) {
        if (args.size() != 2) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'wait' command");
        }

        try {
            int numReplicas = Integer.parseInt(args.get(0));
            long timeout = Long.parseLong(args.get(1));

            int syncedReplicas = replicationService.waitForReplicas(numReplicas, timeout);
            return RespProtocol.createInteger(syncedReplicas);
        } catch (NumberFormatException e) {
            return RespProtocol.createErrorResponse("value is not an integer or out of range");
        }
    }
} 