package server;

import command.CommandProcessor;
import protocol.RespProtocol;
import replication.ReplicationManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class ClientHandler implements Runnable {
    
    private final Socket clientSocket;
    private final CommandProcessor commandProcessor;
    private final ReplicationManager replicationManager;
    private boolean inTransaction = false;
    private final List<List<String>> transactionQueue = new ArrayList<>();
    
    private static final Set<String> WRITE_COMMANDS = Set.of("SET", "DEL", "XADD", "INCR");
    
    public ClientHandler(Socket clientSocket, CommandProcessor commandProcessor, ReplicationManager replicationManager) {
        this.clientSocket = clientSocket;
        this.commandProcessor = commandProcessor;
        this.replicationManager = replicationManager;
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
            replicationManager.removeReplica(clientSocket);
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
            System.out.println("Received from " + clientAddress + ": " + line.trim());
            
            try {
                if (!line.startsWith("*")) {
                    continue; // 단순 PING 등 비배열 명령어는 현재 로직에서 무시
                }
                
                int arrayLength = Integer.parseInt(line.substring(1));
                List<String> commands = RespProtocol.parseRespArray(reader, arrayLength);
                if (commands.isEmpty()) {
                    continue;
                }
                
                if (inTransaction) {
                    handleTransactionCommand(commands, outputStream);
                } else {
                    handleNormalCommand(commands, outputStream);
                }
                
            } catch (NumberFormatException e) {
                System.err.println("Invalid command format from client " + clientAddress + ": " + e.getMessage());
                sendResponse(outputStream, RespProtocol.createErrorResponse("invalid command format"));
            } catch (Exception e) {
                System.err.println("Error processing command from client " + clientAddress + ": " + e.getMessage());
                sendResponse(outputStream, RespProtocol.createErrorResponse("internal server error"));
            }
        }
    }
    
    private void handleTransactionCommand(List<String> commands, OutputStream outputStream) throws IOException {
        String command = commands.get(0).toUpperCase();
        
        switch (command) {
            case "MULTI":
                sendResponse(outputStream, RespProtocol.createErrorResponse("MULTI calls can not be nested"));
                break;
            case "EXEC":
                executeTransaction(outputStream);
                break;
            case "DISCARD":
                inTransaction = false;
                transactionQueue.clear();
                sendResponse(outputStream, RespProtocol.OK_RESPONSE);
                break;
            default:
                transactionQueue.add(commands);
                sendResponse(outputStream, RespProtocol.QUEUED_RESPONSE);
                break;
        }
    }
    
    private void executeTransaction(OutputStream outputStream) throws IOException {
        inTransaction = false;
        List<String> responses = new ArrayList<>();
        for (List<String> queuedCommand : transactionQueue) {
            String response = commandProcessor.processCommand(queuedCommand.get(0), queuedCommand);
            responses.add(response);
            
            // 쓰기 명령 전파
            if (WRITE_COMMANDS.contains(queuedCommand.get(0).toUpperCase())) {
                replicationManager.propagateCommand(queuedCommand);
            }
        }
        transactionQueue.clear();
        sendResponse(outputStream, RespProtocol.createRespArrayFromRaw(responses));
    }
    
    private void handleNormalCommand(List<String> commands, OutputStream outputStream) throws IOException {
        String command = commands.get(0).toUpperCase();
        String clientAddress = clientSocket.getRemoteSocketAddress().toString();
        
        switch (command) {
            case "MULTI":
                inTransaction = true;
                transactionQueue.clear();
                sendResponse(outputStream, RespProtocol.OK_RESPONSE);
                break;
            case "EXEC":
                sendResponse(outputStream, RespProtocol.createErrorResponse("EXEC without MULTI"));
                break;
            case "DISCARD":
                sendResponse(outputStream, RespProtocol.createErrorResponse("DISCARD without MULTI"));
                break;
            default:
                // 레플리카로부터의 ACK 처리
                if (command.equals("REPLCONF") && commands.size() >= 3 && "ACK".equalsIgnoreCase(commands.get(1))) {
                    replicationManager.processAck(clientSocket, Long.parseLong(commands.get(2)));
                    return; // ACK는 응답 없음
                }
                
                String response = commandProcessor.processCommand(command, commands);
                sendResponse(outputStream, response);
                System.out.println("Sent to " + clientAddress + ": " + response.trim());
                
                // PSYNC에 대한 특별 처리: RDB 파일 전송 및 레플리카 등록
                if (command.equals("PSYNC") && response.startsWith("+FULLRESYNC")) {
                    sendEmptyRdb(outputStream, clientAddress);
                    replicationManager.addReplica(clientSocket);
                }
                
                // 쓰기 명령어를 레플리카에 전파
                if (WRITE_COMMANDS.contains(command)) {
                    replicationManager.propagateCommand(commands);
                }
                break;
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
    
    /**
     * 클라이언트에게 응답을 전송합니다.
     */
    private void sendResponse(OutputStream outputStream, String response) throws IOException {
        outputStream.write(response.getBytes());
        outputStream.flush();
    }
} 