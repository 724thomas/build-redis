package replication;

import command.CommandProcessor;
import config.ServerConfig;
import protocol.RespProtocol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Redis 복제 클라이언트
 * 마스터와의 연결 및 핸드셰이크를 담당합니다.
 */
public class ReplicationClient {
    
    private final ServerConfig config;
    private final CommandProcessor commandProcessor;
    private int processedBytesOffset = 0;
    
    public ReplicationClient(ServerConfig config, CommandProcessor commandProcessor) {
        this.config = config;
        this.commandProcessor = commandProcessor;
    }
    
    /**
     * 마스터와의 복제 핸드셰이크를 시작합니다.
     */
    public void startHandshake() {
        new Thread(this::performHandshake).start();
    }
    
    /**
     * 마스터와의 핸드셰이크를 수행합니다.
     */
    private void performHandshake() {
        try {
            System.out.println("Connecting to master " + config.getMasterHost() + ":" + config.getMasterPort());
            
            Socket masterSocket = new Socket(config.getMasterHost(), config.getMasterPort());
            OutputStream outputStream = masterSocket.getOutputStream();
            // We need to be able to "peek" the first byte of the RDB file vs a command
            PushbackInputStream inputStream = new PushbackInputStream(masterSocket.getInputStream());
            
            System.out.println("Connected to master. Starting handshake...");
            
            // Stage 18: PING 명령어 전송
            sendPing(outputStream);
            String pingResponse = readHandshakeResponse(inputStream);
            System.out.println("Master responded to PING: " + pingResponse);
            
            // Stage 19: REPLCONF listening-port 전송
            sendReplconfListeningPort(outputStream);
            String replconfPortResponse = readHandshakeResponse(inputStream);
            System.out.println("Master responded to REPLCONF listening-port: " + replconfPortResponse);
            
            // Stage 19: REPLCONF capa psync2 전송
            sendReplconfCapabilities(outputStream);
            String replconfCapaResponse = readHandshakeResponse(inputStream);
            System.out.println("Master responded to REPLCONF capa: " + replconfCapaResponse);
            
            // Stage 20: PSYNC ? -1 전송
            sendPsync(outputStream);
            String psyncResponse = readHandshakeResponse(inputStream);
            System.out.println("Master responded to PSYNC: " + psyncResponse);
            
            // Stage 23: RDB 파일 수신
            readRdbFile(inputStream);
            
            // Stage 26 & 28: 마스터로부터 명령어 전파 수신 및 처리
            listenForMasterCommands(inputStream, outputStream);
            
        } catch (IOException e) {
            System.err.println("Failed to connect to master: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 마스터에게 PING 명령어를 전송합니다.
     */
    private void sendPing(OutputStream outputStream) throws IOException {
        String command = "*1\r\n$4\r\nPING\r\n";
        outputStream.write(command.getBytes());
        outputStream.flush();
        System.out.println("Sent PING to master: " + command.replace("\r\n", "\\r\\n"));
    }
    
    /**
     * 마스터에게 REPLCONF listening-port 명령어를 전송합니다.
     */
    private void sendReplconfListeningPort(OutputStream outputStream) throws IOException {
        String portStr = String.valueOf(config.getPort());
        String command = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + portStr.length() + "\r\n" + portStr + "\r\n";
        outputStream.write(command.getBytes());
        outputStream.flush();
        System.out.println("Sent REPLCONF listening-port to master: " + command.replace("\r\n", "\\r\\n"));
    }
    
    /**
     * 마스터에게 REPLCONF capa psync2 명령어를 전송합니다.
     */
    private void sendReplconfCapabilities(OutputStream outputStream) throws IOException {
        String command = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        outputStream.write(command.getBytes());
        outputStream.flush();
        System.out.println("Sent REPLCONF capa to master: " + command.replace("\r\n", "\\r\\n"));
    }
    
    /**
     * 마스터에게 PSYNC ? -1 명령어를 전송합니다.
     * Stage 20: 첫 연결이므로 replication ID는 ?, offset은 -1로 전송
     */
    private void sendPsync(OutputStream outputStream) throws IOException {
        String command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        outputStream.write(command.getBytes());
        outputStream.flush();
        System.out.println("Sent PSYNC to master: " + command.replace("\r\n", "\\r\\n"));
    }
    
    /**
     * 마스터로부터 핸드셰이크 응답을 한 줄 읽습니다.
     * '+'로 시작하는 단순 문자열만 처리합니다.
     */
    private String readHandshakeResponse(InputStream inputStream) throws IOException {
        StringBuilder response = new StringBuilder();
        int b;
        while ((b = inputStream.read()) != -1) {
            if (b == '\r') {
                if (inputStream.read() != '\n') { // Consume the \n
                    throw new IOException("Malformed response from master: missing LF after CR.");
                }
                break;
            }
            response.append((char) b);
        }
        
        // +PONG, +OK, +FULLRESYNC...
        if (response.length() > 0 && response.charAt(0) == '+') {
            return response.toString();
        }
        
        throw new IOException("Unexpected handshake response: " + response);
    }
    
    /**
     * 마스터로부터 RDB 파일을 읽습니다.
     * Stage 23: RDB 파일 형식은 $<length>\\r\\n<contents> 입니다.
     */
    private void readRdbFile(PushbackInputStream inputStream) throws IOException {
        // RDB 파일 또는 다음 명령어를 확인하기 위해 첫 바이트를 읽습니다.
        int firstByte = inputStream.read();
        if (firstByte == -1) {
            throw new IOException("End of stream while expecting RDB file or command.");
        }

        // RDB 파일이 아니면 바이트를 다시 스트림으로 되돌리고 반환합니다.
        if (firstByte != '$') {
            System.out.println("Did not receive RDB file (first byte: " + (char)firstByte + "). Assuming no RDB transfer and proceeding to listen for commands.");
            inputStream.unread(firstByte);
            return;
        }
        
        System.out.println("Started receiving RDB file...");

        // RDB 파일 길이를 읽습니다.
        StringBuilder lengthStr = new StringBuilder();
        int b;
        while ((b = inputStream.read()) != -1 && b != '\r') {
            lengthStr.append((char) b);
        }
        
        // `\n` 건너뛰기
        if (inputStream.read() != '\n') {
            throw new IOException("Expected '\\n' after RDB file length.");
        }
        
        int length = Integer.parseInt(lengthStr.toString());
        System.out.println("RDB file length from master: " + length);
        
        // RDB 파일 내용 읽기
        if (length > 0) {
            byte[] rdbBytes = new byte[length];
            int totalBytesRead = 0;
            while (totalBytesRead < length) {
                int bytesRead = inputStream.read(rdbBytes, totalBytesRead, length - totalBytesRead);
                if (bytesRead == -1) {
                    throw new IOException("Unexpected end of stream while reading RDB file.");
                }
                totalBytesRead += bytesRead;
            }
            // 현재 단계에서는 RDB 파일 내용을 사용하지 않으므로, 읽기만 하고 넘어갑니다.
            System.out.println("Successfully read " + totalBytesRead + " bytes of RDB file.");
        }
    }
    
    /**
     * 마스터로부터 전파되는 명령어를 지속적으로 수신하고 처리합니다.
     * Stage 26: 이 단계에서는 응답을 보내지 않습니다.
     * Stage 28: 명령어 바이트 오프셋을 추적하고 GETACK에 응답합니다.
     */
    private void listenForMasterCommands(InputStream masterInputStream, OutputStream masterOutputStream) throws IOException {
        while (!Thread.currentThread().isInterrupted()) {
            int commandByteCount = 0;

            // Read Array Header
            String arrayHeader = readLine(masterInputStream);
            if (arrayHeader == null) {
                System.out.println("Master closed the connection.");
                break;
            }
            commandByteCount += arrayHeader.length(); // readLine includes \r\n
            if (!arrayHeader.startsWith("*")) {
                System.err.println("Warning: Expected RESP Array, but got: " + arrayHeader.replace("\r\n", "\\r\\n"));
                continue;
            }
            
            int arrayLength = Integer.parseInt(arrayHeader.substring(1, arrayHeader.length() - 2)); // remove * and \r\n
            List<String> commandParts = new ArrayList<>();

            for (int i = 0; i < arrayLength; i++) {
                // Read Bulk String Header
                String bulkHeader = readLine(masterInputStream);
                if (bulkHeader == null) {
                    throw new IOException("Unexpected end of stream: bulk header is null.");
                }
                commandByteCount += bulkHeader.length();
                if (!bulkHeader.startsWith("$")) {
                    throw new IOException("Expected Bulk String header, but got: " + bulkHeader.replace("\r\n", "\\r\\n"));
                }

                int bulkLength = Integer.parseInt(bulkHeader.substring(1, bulkHeader.length() - 2)); // remove $ and \r\n
                if (bulkLength == -1) {
                    commandParts.add(null); // Null bulk string
                    continue;
                }

                // Read Bulk String Content
                byte[] bulkContentBytes = new byte[bulkLength];
                int totalBytesRead = 0;
                while (totalBytesRead < bulkLength) {
                    int bytesRead = masterInputStream.read(bulkContentBytes, totalBytesRead, bulkLength - totalBytesRead);
                    if (bytesRead == -1) {
                        throw new IOException("Unexpected end of stream while reading bulk string content");
                    }
                    totalBytesRead += bytesRead;
                }
                commandByteCount += totalBytesRead;

                // Consume trailing \r\n
                if (masterInputStream.read() != '\r' || masterInputStream.read() != '\n') {
                    throw new IOException("Expected CRLF after bulk string content");
                }
                commandByteCount += 2;

                commandParts.add(new String(bulkContentBytes, StandardCharsets.UTF_8));
            }

            if (commandParts.isEmpty()) {
                continue;
            }

            String commandName = commandParts.get(0).toUpperCase();

            if (commandName.equals("REPLCONF") && commandParts.size() >= 2 && commandParts.get(1).equalsIgnoreCase("GETACK")) {
                String offsetStr = String.valueOf(processedBytesOffset);
                String ackResponse = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + offsetStr.length() + "\r\n" + offsetStr + "\r\n";
                masterOutputStream.write(ackResponse.getBytes(StandardCharsets.UTF_8));
                masterOutputStream.flush();
                System.out.println("Responded to GETACK with offset: " + processedBytesOffset);
            } else {
                commandProcessor.processCommand(commandName, commandParts);
                System.out.println("Processed propagated command from master: " + commandParts);
            }

            processedBytesOffset += commandByteCount;
        }
    }

    private String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            sb.append((char) b);
            if (sb.length() >= 2 && sb.charAt(sb.length() - 2) == '\r' && sb.charAt(sb.length() - 1) == '\n') {
                return sb.toString();
            }
        }
        return sb.length() > 0 ? sb.toString() : null;
    }
} 