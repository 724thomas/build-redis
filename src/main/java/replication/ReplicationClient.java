package replication;

import command.CommandProcessor;
import config.ServerConfig;
import protocol.RespProtocol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

/**
 * Redis 복제 클라이언트
 * 마스터와의 연결 및 핸드셰이크를 담당합니다.
 */
public class ReplicationClient {
    
    private final ServerConfig config;
    private final CommandProcessor commandProcessor;
    
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
            BufferedReader inputStreamReader = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
            
            System.out.println("Connected to master. Starting handshake...");
            
            // Stage 18: PING 명령어 전송
            sendPing(outputStream);
            String pingResponse = readResponse(inputStreamReader);
            System.out.println("Master responded to PING: " + pingResponse);
            
            // Stage 19: REPLCONF listening-port 전송
            sendReplconfListeningPort(outputStream);
            String replconfPortResponse = readResponse(inputStreamReader);
            System.out.println("Master responded to REPLCONF listening-port: " + replconfPortResponse);
            
            // Stage 19: REPLCONF capa psync2 전송
            sendReplconfCapabilities(outputStream);
            String replconfCapaResponse = readResponse(inputStreamReader);
            System.out.println("Master responded to REPLCONF capa: " + replconfCapaResponse);
            
            // Stage 20: PSYNC ? -1 전송
            sendPsync(outputStream);
            String psyncResponse = readResponse(inputStreamReader);
            System.out.println("Master responded to PSYNC: " + psyncResponse);
            
            // Stage 23: RDB 파일 수신
            readRdbFile(masterSocket.getInputStream());
            System.out.println("Finished receiving RDB file from master.");
            
            // Stage 26: 마스터로부터 명령어 전파 수신 및 처리
            listenForMasterCommands(masterSocket);
            
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
     * 마스터로부터 응답을 읽습니다.
     */
    private String readResponse(BufferedReader inputStream) throws IOException {
        String line = inputStream.readLine();
        
        if (line == null) {
            throw new IOException("Unexpected end of stream from master");
        }
        
        // RESP Simple String (+OK, +PONG, +FULLRESYNC) 처리
        if (line.startsWith("+")) {
            return line;
        }
        
        // 다른 RESP 타입도 처리 가능하도록 확장
        return line;
    }
    
    /**
     * 마스터로부터 RDB 파일을 읽습니다.
     * Stage 23: RDB 파일 형식은 $<length>\\r\\n<contents> 입니다.
     */
    private void readRdbFile(InputStream inputStream) throws IOException {
        // 첫 바이트 '$'를 읽고 확인
        int firstByte = inputStream.read();
        if (firstByte != '$') {
            throw new IOException("Expected RDB file to start with '$', but got: " + (char) firstByte);
        }
        
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
     */
    private void listenForMasterCommands(Socket masterSocket) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println("Received from master for propagation: " + line.replace("\r\n", "\\r\\n"));
            if (line.startsWith("*")) {
                try {
                    int arrayLength = Integer.parseInt(line.substring(1));
                    List<String> commands = RespProtocol.parseRespArray(reader, arrayLength);
                    
                    if (!commands.isEmpty()) {
                        String command = commands.get(0).toUpperCase();
                        // 명령어를 처리하고, 응답은 무시합니다.
                        commandProcessor.processCommand(command, commands);
                        System.out.println("Processed propagated command from master: " + commands);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing propagated command: " + e.getMessage());
                }
            }
        }
    }
} 