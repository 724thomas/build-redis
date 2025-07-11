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

public class ClientHandler implements Runnable {
    
    private final Socket clientSocket;
    private final CommandProcessor commandProcessor;
    private final ReplicationManager replicationManager;
    
    public ClientHandler(Socket clientSocket, CommandProcessor commandProcessor, ReplicationManager replicationManager) {
        this.clientSocket = clientSocket;
        this.commandProcessor = commandProcessor;
        this.replicationManager = replicationManager;
    }
    
    @Override
    public void run() {
        String clientAddress = clientSocket.getRemoteSocketAddress().toString();
        boolean inTransaction = false;
        List<List<String>> transactionQueue = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             OutputStream outputStream = clientSocket.getOutputStream()) {
            
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("Received from " + clientAddress + ": " + line);
                
                try {
                    // RESP 프로토콜 파싱
                    if (line.startsWith("*")) {
                        // 배열 명령어 처리
                        int arrayLength = Integer.parseInt(line.substring(1));
                        List<String> commands = RespProtocol.parseRespArray(reader, arrayLength);
                        
                        if (commands.isEmpty()) {
                            continue;
                        }
                        
                        String command = commands.get(0).toUpperCase();
                        
                        if (inTransaction) {
                            switch (command) {
                                case "MULTI":
                                    sendResponse(outputStream, RespProtocol.createErrorResponse("MULTI calls can not be nested"));
                                    break;
                                case "EXEC":
                                    inTransaction = false;
                                    List<String> responses = new ArrayList<>();
                                    for (List<String> queuedCommand : transactionQueue) {
                                        String response = commandProcessor.processCommand(queuedCommand.get(0), queuedCommand);
                                        responses.add(response);
                                        
                                        // 쓰기 명령 전파
                                        List<String> writeCommands = Arrays.asList("SET", "XADD", "INCR");
                                        if (writeCommands.contains(queuedCommand.get(0).toUpperCase())) {
                                            replicationManager.propagateCommand(queuedCommand);
                                        }
                                    }
                                    transactionQueue.clear();
                                    sendResponse(outputStream, RespProtocol.createRespArrayFromRaw(responses));
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
                        } else {
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
                                    // 기존 명령어 처리 로직
                                    // 레플리카로부터의 ACK 처리
                                    if (command.equals("REPLCONF") && commands.size() >= 3 && "ACK".equalsIgnoreCase(commands.get(1))) {
                                        replicationManager.processAck(clientSocket, Long.parseLong(commands.get(2)));
                                        continue; // ACK는 응답 없음
                                    }
                                    
                                    String response = commandProcessor.processCommand(command, commands);
                                    sendResponse(outputStream, response);
                                    System.out.println("Sent to " + clientAddress + ": " + response.trim());
                                    
                                    // PSYNC에 대한 특별 처리: RDB 파일 전송 및 레플리카 등록
                                    if (command.equals("PSYNC") && response.startsWith("+FULLRESYNC")) {
                                        byte[] rdbFileBytes = RespProtocol.EMPTY_RDB_BYTES;
                                        String rdbFilePrefix = "$" + rdbFileBytes.length + "\r\n";
                                        
                                        outputStream.write(rdbFilePrefix.getBytes());
                                        outputStream.write(rdbFileBytes);
                                        outputStream.flush();
                                        System.out.println("Sent empty RDB file to " + clientAddress);
                                        
                                        // 이 클라이언트를 레플리카로 등록
                                        replicationManager.addReplica(clientSocket);
                                    }
                                    
                                    // 쓰기 명령어를 레플리카에 전파
                                    List<String> writeCommands = Arrays.asList("SET", "XADD", "INCR");
                                    if (writeCommands.contains(command)) {
                                        replicationManager.propagateCommand(commands);
                                    }
                                    break;
                            }
                        }
                    } else if (line.equals("PING")) {
                        // 단순 text PING 처리 (이전 호환성)
                        sendResponse(outputStream, RespProtocol.PONG_RESPONSE);
                        System.out.println("Sent: PONG");
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid command format from client " + clientAddress + ": " + e.getMessage());
                    sendResponse(outputStream, RespProtocol.createErrorResponse("invalid command format"));
                } catch (Exception e) {
                    System.err.println("Error processing command from client " + clientAddress + ": " + e.getMessage());
                    sendResponse(outputStream, RespProtocol.createErrorResponse("internal server error"));
                }
            }
            
        } catch (SocketException e) {
            System.out.println("Client disconnected: " + clientAddress);
        } catch (IOException e) {
            System.err.println("Error handling client " + clientAddress + ": " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket " + clientAddress + ": " + e.getMessage());
            }
        }
        
        System.out.println("Client connection closed: " + clientAddress);
    }
    
    /**
     * 클라이언트에게 응답을 전송합니다.
     */
    private void sendResponse(OutputStream outputStream, String response) throws IOException {
        outputStream.write(response.getBytes());
        outputStream.flush();
    }
} 