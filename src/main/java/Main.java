import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    private static final int DEFAULT_PORT = 6379;
    private static final String PONG_RESPONSE = "+PONG\r\n";
    private static final String OK_RESPONSE = "+OK\r\n";
    
    // 키-값 저장소 (스레드 안전)
    private static final Map<String, String> keyValueStore = new ConcurrentHashMap<>();
    // 키별 만료 시간 저장소 (밀리초 단위 timestamp)
    private static final Map<String, Long> keyExpiryStore = new ConcurrentHashMap<>();
    
    public static void main(String[] args) {
        System.out.println("Starting Redis server on port " + DEFAULT_PORT);
        
        try (ServerSocket serverSocket = createServerSocket(DEFAULT_PORT)) {
            System.out.println("Redis server started. Waiting for connections...");
            
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());
                    
                    // 클라이언트 연결을 별도의 스레드로 처리
                    new Thread(() -> handleClient(clientSocket)).start();
                } catch (IOException e) {
                    System.err.println("Error handling client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to start Redis server: " + e.getMessage());
        }
    }
    
    /**
     * 서버 소켓을 생성하고 설정합니다.
     */
    private static ServerSocket createServerSocket(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        // 서버 재시작 시 'Address already in use' 에러 방지
        serverSocket.setReuseAddress(true);
        return serverSocket;
    }
    
    /**
     * 클라이언트 연결을 처리하고 Redis 명령어에 응답합니다.
     */
    private static void handleClient(Socket clientSocket) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             OutputStream outputStream = clientSocket.getOutputStream()) {
            
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("Received: " + line);
                
                // RESP 프로토콜 파싱
                if (line.startsWith("*")) {
                    // 배열 명령어 처리
                    int arrayLength = Integer.parseInt(line.substring(1));
                    List<String> commands = parseRespArray(reader, arrayLength);
                    
                    if (!commands.isEmpty()) {
                        String command = commands.get(0).toUpperCase();
                        String response = processCommand(command, commands);
                        sendResponse(outputStream, response);
                        System.out.println("Sent: " + response.trim());
                    }
                } else if (line.equals("PING")) {
                    // 단순 텍스트 PING 처리 (이전 호환성)
                    sendResponse(outputStream, PONG_RESPONSE);
                    System.out.println("Sent: PONG");
                }
            }
            
        } catch (SocketException e) {
            System.out.println("Client disconnected: " + clientSocket.getRemoteSocketAddress());
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
        
        System.out.println("Client connection closed: " + clientSocket.getRemoteSocketAddress());
    }
    
    /**
     * RESP 배열 형식을 파싱합니다.
     */
    private static List<String> parseRespArray(BufferedReader reader, int arrayLength) throws IOException {
        List<String> commands = new ArrayList<>();
        
        for (int i = 0; i < arrayLength; i++) {
            String lengthLine = reader.readLine();
            if (lengthLine != null && lengthLine.startsWith("$")) {
                int stringLength = Integer.parseInt(lengthLine.substring(1));
                if (stringLength >= 0) {
                    String command = reader.readLine();
                    if (command != null) {
                        commands.add(command);
                        System.out.println("Parsed command part: " + command);
                    }
                }
            }
        }
        
        return commands;
    }
    
    /**
     * Redis 명령어를 처리하고 응답을 생성합니다.
     */
    private static String processCommand(String command, List<String> args) {
        switch (command) {
            case "PING":
                return PONG_RESPONSE;
            case "ECHO":
                if (args.size() >= 2) {
                    String value = args.get(1);
                    return createBulkString(value);
                }
                return "$-1\r\n"; // null bulk string
            case "SET":
                return handleSetCommand(args);
            case "GET":
                return handleGetCommand(args);
            default:
                return "-ERR unknown command '" + command + "'\r\n";
        }
    }
    
    /**
     * SET 명령어를 처리합니다. PX 옵션을 지원합니다.
     * 형식: SET key value [PX milliseconds]
     */
    private static String handleSetCommand(List<String> args) {
        if (args.size() < 3) {
            return "-ERR wrong number of arguments for 'SET' command\r\n";
        }
        
        String key = args.get(1);
        String value = args.get(2);
        
        // PX 옵션 확인
        if (args.size() >= 5 && "PX".equalsIgnoreCase(args.get(3))) {
            try {
                long expireInMs = Long.parseLong(args.get(4));
                long expiryTime = System.currentTimeMillis() + expireInMs;
                
                keyValueStore.put(key, value);
                keyExpiryStore.put(key, expiryTime);
                
                System.out.println("Stored with expiry: " + key + " = " + value + " (expires at: " + expiryTime + ")");
            } catch (NumberFormatException e) {
                return "-ERR value is not an integer or out of range\r\n";
            }
        } else {
            // 일반 SET (만료 시간 없음)
            keyValueStore.put(key, value);
            keyExpiryStore.remove(key); // 기존 만료 시간 제거
            System.out.println("Stored: " + key + " = " + value);
        }
        
        return OK_RESPONSE;
    }
    
    /**
     * GET 명령어를 처리합니다. 만료된 키는 자동으로 삭제합니다.
     */
    private static String handleGetCommand(List<String> args) {
        if (args.size() < 2) {
            return "-ERR wrong number of arguments for 'GET' command\r\n";
        }
        
        String key = args.get(1);
        
        // 만료 시간 검사
        if (isKeyExpired(key)) {
            // 만료된 키 삭제
            keyValueStore.remove(key);
            keyExpiryStore.remove(key);
            System.out.println("Key expired and removed: " + key);
            return "$-1\r\n"; // null bulk string
        }
        
        String value = keyValueStore.get(key);
        System.out.println("Retrieved: " + key + " = " + value);
        return createBulkString(value);
    }
    
    /**
     * 키가 만료되었는지 확인합니다.
     */
    private static boolean isKeyExpired(String key) {
        Long expiryTime = keyExpiryStore.get(key);
        if (expiryTime == null) {
            return false; // 만료 시간이 설정되지 않음
        }
        return System.currentTimeMillis() > expiryTime;
    }
    
    /**
     * RESP bulk string 형식으로 문자열을 인코딩합니다.
     */
    private static String createBulkString(String value) {
        if (value == null) {
            return "$-1\r\n"; // null bulk string
        }
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }
    
    /**
     * 클라이언트에게 응답을 전송합니다.
     */
    private static void sendResponse(OutputStream outputStream, String response) throws IOException {
        outputStream.write(response.getBytes());
        outputStream.flush();
    }
}
