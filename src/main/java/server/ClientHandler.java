package server;

import command.CommandHandler;
import protocol.RespProtocol;
import service.ReplicationService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

public class ClientHandler implements Runnable {
    
    private final Socket clientSocket;
    private final CommandHandler commandHandler;
    private final ReplicationService replicationService;
    private boolean inTransaction = false;
    private final List<List<String>> transactionQueue = new ArrayList<>();
    
    public ClientHandler(Socket clientSocket, CommandHandler commandHandler, ReplicationService replicationService) {
        this.clientSocket = clientSocket;
        this.commandHandler = commandHandler;
        this.replicationService = replicationService;
    }
    
    @Override
    public void run() {
        String clientAddress = clientSocket.getRemoteSocketAddress().toString();
        
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             OutputStream outputStream = clientSocket.getOutputStream()) {
            
            handleClientLoop(reader, outputStream, clientAddress);
            
        } catch (SocketException e) {
            System.out.println("Client disconnected: " + clientAddress);
        } catch (IOException e) {
            System.err.println("Error handling client " + clientAddress + ": " + e.getMessage());
        } finally {
            replicationService.removeReplica(clientSocket);
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket " + clientAddress + ": " + e.getMessage());
            }
        }
        
        System.out.println("Client connection closed: " + clientAddress);
    }
    
    private void handleClientLoop(BufferedReader reader, OutputStream outputStream, String clientAddress) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            try {
                if (!line.startsWith("*")) {
                    continue; // Ignore non-array commands for now
                }
                
                int arrayLength = Integer.parseInt(line.substring(1));
                List<String> commandParts = RespProtocol.parseRespArray(reader, arrayLength);
                if (commandParts.isEmpty()) {
                    continue;
                }
                
                handleCommand(commandParts, outputStream);
                
            } catch (Exception e) {
                System.err.println("Error processing command from client " + clientAddress + ": " + e.getMessage());
                sendResponse(outputStream, RespProtocol.createErrorResponse("internal server error"));
            }
        }
    }
    
    private void handleCommand(List<String> commandParts, OutputStream outputStream) throws IOException {
        String commandName = commandParts.get(0);
        List<String> args = commandParts.subList(1, commandParts.size());
        
        String response = commandHandler.handleCommand(commandName, args, clientSocket);
        
        if (response != null) {
            sendResponse(outputStream, response);
        }

        // Special handling for PSYNC after sending the initial response
        if (commandName.equalsIgnoreCase("PSYNC")) {
            sendEmptyRdb(outputStream, clientSocket.getRemoteSocketAddress().toString());
            replicationService.addReplica(clientSocket);
        }
    }
    
    private void sendEmptyRdb(OutputStream outputStream, String clientAddress) throws IOException {
        byte[] rdbFileBytes = RespProtocol.EMPTY_RDB_BYTES;
        String rdbFilePrefix = "$" + rdbFileBytes.length + "\r\n";
        
        outputStream.write(rdbFilePrefix.getBytes());
        outputStream.write(rdbFileBytes);
        outputStream.flush();
        System.out.println("Sent empty RDB file to " + clientAddress);
    }
    
    private void sendResponse(OutputStream outputStream, String response) throws IOException {
        outputStream.write(response.getBytes());
        outputStream.flush();
    }
} 