package command;

import protocol.RespProtocol;
import service.ReplicationService;

import java.net.Socket;
import java.util.List;

public class ReplconfCommand implements Command {

    private final ReplicationService replicationService;
    private final Socket clientSocket; // 각 클라이언트 핸들러마다 다른 소켓이 필요

    public ReplconfCommand(ReplicationService replicationService, Socket clientSocket) {
        this.replicationService = replicationService;
        this.clientSocket = clientSocket;
    }

    @Override
    public String execute(List<String> args) {
        if (args.isEmpty()) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'replconf' command");
        }

        String subCommand = args.get(0).toLowerCase();
        switch (subCommand) {
            case "getack":
                // This is a master-side command, but the logic is initiated from waitForReplicas.
                // A replica should not receive GETACK. If it does, maybe ignore or error.
                // For now, let's assume this is handled by the master and return OK for other REPLCONF.
                return RespProtocol.createSimpleString("OK");

            case "ack":
                if (args.size() > 1) {
                    try {
                        long offset = Long.parseLong(args.get(1));
                        replicationService.processAck(clientSocket, offset);
                        // ACK command does not send a response to the master.
                        return null; 
                    } catch (NumberFormatException e) {
                        return RespProtocol.createErrorResponse("value is not an integer or out of range");
                    }
                }
                return RespProtocol.createErrorResponse("wrong number of arguments for 'replconf ack' command");
            
            case "listening-port":
            case "capa":
                // These are part of the handshake, just reply OK.
                return RespProtocol.createSimpleString("OK");

            default:
                return RespProtocol.createErrorResponse("unknown subcommand for 'replconf'");
        }
    }
} 