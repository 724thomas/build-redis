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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
// ===== STAGE 9: RDB file handling imports =====
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {
    private static int serverPort = 6379; // 설정 가능한 포트
    private static final String PONG_RESPONSE = "+PONG\r\n";
    private static final String OK_RESPONSE = "+OK\r\n";
    
    // 키-값 저장소 (스레드 안전)
    private static final Map<String, String> keyValueStore = new ConcurrentHashMap<>();
    // 키별 만료 시간 저장소 (밀리초 단위 timestamp)
    private static final Map<String, Long> keyExpiryStore = new ConcurrentHashMap<>();
    
    // ===== STAGE 8: RDB 파일 설정 =====
    private static String rdbDir = "/tmp/redis-files";
    private static String rdbFilename = "dump.rdb";
    
    // ===== STAGE 11-13: Redis Streams 저장소 =====
    private static final Map<String, List<StreamEntry>> streams = new ConcurrentHashMap<>();
    
    public static void main(String[] args) {
        // ===== STAGE 8: 명령행 인수 파싱 =====
        parseCommandLineArgs(args);
        
        // ===== STAGE 9: RDB 파일에서 데이터 로드 =====
        loadRdbFile();
        
        System.out.println("Starting Redis server on port " + serverPort);
        System.out.println("RDB directory: " + rdbDir);
        System.out.println("RDB filename: " + rdbFilename);
        
        try (ServerSocket serverSocket = createServerSocket(serverPort)) {
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
     * ===== STAGE 8: 명령행 인수를 파싱합니다. =====
     * ===== STAGE BW1: --port 옵션 추가 =====
     */
    private static void parseCommandLineArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--dir":
                    if (i + 1 < args.length) {
                        rdbDir = args[++i];
                    }
                    break;
                case "--dbfilename":
                    if (i + 1 < args.length) {
                        rdbFilename = args[++i];
                    }
                    break;
                case "--port":
                    if (i + 1 < args.length) {
                        try {
                            serverPort = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid port number: " + args[i]);
                        }
                    }
                    break;
            }
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
            case "CONFIG":  // ===== STAGE 8: CONFIG GET 명령어 =====
                return handleConfigCommand(args);
            case "KEYS":    // ===== STAGE 9: KEYS 명령어 =====
                return handleKeysCommand(args);
            case "XADD":    // ===== STAGE 11: Redis Streams XADD 명령어 =====
                return handleXAddCommand(args);
            case "XRANGE":  // ===== STAGE 12: Redis Streams XRANGE 명령어 =====
                return handleXRangeCommand(args);
            case "XREAD":   // ===== STAGE 13: Redis Streams XREAD 명령어 =====
                return handleXReadCommand(args);
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
     * ===== STAGE 10: GET 명령어를 처리합니다. 만료된 키는 자동으로 삭제합니다. =====
     * ===== RDB에서 로드된 데이터도 함께 처리 =====
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
    
    /**
     * CONFIG 명령어를 처리합니다.
     */
    private static String handleConfigCommand(List<String> args) {
        if (args.size() < 3) {
            return "-ERR wrong number of arguments for 'CONFIG' command\r\n";
        }
        
        String subCommand = args.get(1).toUpperCase();
        if (!"GET".equals(subCommand)) {
            return "-ERR unknown subcommand '" + args.get(1) + "'\r\n";
        }
        
        String parameter = args.get(2);
        
        switch (parameter.toLowerCase()) {
            case "dir":
                return createRespArray(new String[]{"dir", rdbDir});
            case "dbfilename":
                return createRespArray(new String[]{"dbfilename", rdbFilename});
            default:
                return "*0\r\n"; // empty array for unknown parameters
        }
    }
    
    /**
     * RESP 배열을 생성합니다.
     */
    private static String createRespArray(String[] elements) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(elements.length).append("\r\n");
        
        for (String element : elements) {
            sb.append(createBulkString(element));
        }
        
        return sb.toString();
    }
    
    /**
     * ===== STAGE 9: KEYS 명령어를 처리합니다. 패턴 "*"만 지원합니다. =====
     */
    private static String handleKeysCommand(List<String> args) {
        if (args.size() < 2) {
            return "-ERR wrong number of arguments for 'KEYS' command\r\n";
        }
        
        String pattern = args.get(1);
        if (!"*".equals(pattern)) {
            return "-ERR pattern not supported\r\n";
        }
        
        // 만료된 키들을 먼저 정리
        cleanExpiredKeys();
        
        // 모든 키를 RESP 배열로 반환
        Set<String> keys = keyValueStore.keySet();
        List<String> keyList = new ArrayList<>(keys);
        
        return createRespArray(keyList.toArray(new String[0]));
    }
    
    /**
     * ===== STAGE 9: 만료된 키들을 정리합니다. =====
     */
    private static void cleanExpiredKeys() {
        List<String> expiredKeys = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        
        for (Map.Entry<String, Long> entry : keyExpiryStore.entrySet()) {
            if (currentTime > entry.getValue()) {
                expiredKeys.add(entry.getKey());
            }
        }
        
        for (String key : expiredKeys) {
            keyValueStore.remove(key);
            keyExpiryStore.remove(key);
            System.out.println("Expired key removed: " + key);
        }
    }
    
    /**
     * ===== STAGE 9: RDB 파일에서 데이터를 로드합니다. =====
     */
    private static void loadRdbFile() {
        Path rdbPath = Paths.get(rdbDir, rdbFilename);
        
        if (!Files.exists(rdbPath)) {
            System.out.println("RDB file not found: " + rdbPath + ". Starting with empty database.");
            return;
        }
        
        try {
            byte[] rdbData = Files.readAllBytes(rdbPath);
            parseRdbFile(rdbData);
            System.out.println("RDB file loaded successfully: " + rdbPath);
        } catch (IOException e) {
            System.err.println("Error loading RDB file: " + e.getMessage());
            System.out.println("Starting with empty database.");
        }
    }
    
    /**
     * ===== STAGE 9: RDB 파일 데이터를 파싱합니다. =====
     * ===== Redis RDB 파일 형식 완전 지원 =====
     */
    private static void parseRdbFile(byte[] data) {
        if (data.length < 9) {
            System.out.println("RDB file too short, starting with empty database.");
            return;
        }
        
        // Header 검증 (REDIS0011)
        String header = new String(data, 0, 9);
        if (!header.startsWith("REDIS")) {
            System.out.println("Invalid RDB file header, starting with empty database.");
            return;
        }
        
        System.out.println("RDB file header: " + header);
        
        int pos = 9; // Header 다음부터 시작
        
        try {
            while (pos < data.length) {
                int opcode = data[pos] & 0xFF;
                pos++;
                
                if (opcode == 0xFF) {
                    // 파일 끝
                    System.out.println("RDB file parsing completed.");
                    break;
                } else if (opcode == 0xFE) {
                    // 데이터베이스 선택
                    RdbParseResult dbResult = parseSize(data, pos);
                    pos = dbResult.nextPos;
                    System.out.println("Database index: " + dbResult.value);
                } else if (opcode == 0xFB) {
                    // 해시 테이블 크기 정보
                    RdbParseResult keyHashSize = parseSize(data, pos);
                    pos = keyHashSize.nextPos;
                    RdbParseResult expireHashSize = parseSize(data, pos);
                    pos = expireHashSize.nextPos;
                    System.out.println("Hash table sizes - Keys: " + keyHashSize.value + ", Expires: " + expireHashSize.value);
                } else if (opcode == 0xFA) {
                    // 메타데이터
                    RdbParseResult nameResult = parseString(data, pos);
                    pos = nameResult.nextPos;
                    RdbParseResult valueResult = parseString(data, pos);
                    pos = valueResult.nextPos;
                    System.out.println("Metadata - " + nameResult.stringValue + ": " + valueResult.stringValue);
                } else if (opcode == 0xFC) {
                    // 밀리초 만료 시간
                    if (pos + 8 >= data.length) break;
                    long expireTime = 0;
                    for (int i = 0; i < 8; i++) {
                        expireTime |= ((long) (data[pos + i] & 0xFF)) << (i * 8);
                    }
                    pos += 8;
                    
                    // 키-값 파싱
                    if (pos >= data.length) break;
                    int valueType = data[pos] & 0xFF;
                    pos++;
                    
                    RdbParseResult keyResult = parseString(data, pos);
                    if (keyResult == null) break;
                    pos = keyResult.nextPos;
                    
                    RdbParseResult valueResult = parseString(data, pos);
                    if (valueResult == null) break;
                    pos = valueResult.nextPos;
                    
                    keyValueStore.put(keyResult.stringValue, valueResult.stringValue);
                    keyExpiryStore.put(keyResult.stringValue, expireTime);
                    System.out.println("Loaded key with expiry: " + keyResult.stringValue + " = " + valueResult.stringValue + " (expires: " + expireTime + ")");
                } else if (opcode == 0xFD) {
                    // 초 단위 만료 시간
                    if (pos + 4 >= data.length) break;
                    long expireTime = 0;
                    for (int i = 0; i < 4; i++) {
                        expireTime |= ((long) (data[pos + i] & 0xFF)) << (i * 8);
                    }
                    expireTime *= 1000; // 밀리초로 변환
                    pos += 4;
                    
                    // 키-값 파싱
                    if (pos >= data.length) break;
                    int valueType = data[pos] & 0xFF;
                    pos++;
                    
                    RdbParseResult keyResult = parseString(data, pos);
                    if (keyResult == null) break;
                    pos = keyResult.nextPos;
                    
                    RdbParseResult valueResult = parseString(data, pos);
                    if (valueResult == null) break;
                    pos = valueResult.nextPos;
                    
                    keyValueStore.put(keyResult.stringValue, valueResult.stringValue);
                    keyExpiryStore.put(keyResult.stringValue, expireTime);
                    System.out.println("Loaded key with expiry: " + keyResult.stringValue + " = " + valueResult.stringValue + " (expires: " + expireTime + ")");
                } else if (opcode == 0x00) {
                    // 만료 시간 없는 문자열 값
                    RdbParseResult keyResult = parseString(data, pos);
                    if (keyResult == null) break;
                    pos = keyResult.nextPos;
                    
                    RdbParseResult valueResult = parseString(data, pos);
                    if (valueResult == null) break;
                    pos = valueResult.nextPos;
                    
                    keyValueStore.put(keyResult.stringValue, valueResult.stringValue);
                    System.out.println("Loaded key: " + keyResult.stringValue + " = " + valueResult.stringValue);
                } else {
                    // 알 수 없는 opcode, 안전하게 건너뛰기
                    System.out.println("Unknown opcode: 0x" + Integer.toHexString(opcode) + " at position " + (pos - 1));
                    break;
                }
            }
        } catch (Exception e) {
            System.err.println("Error parsing RDB file: " + e.getMessage());
            System.out.println("Continuing with partially loaded data.");
        }
    }
    
    /**
     * ===== STAGE 9: RDB 파일에서 크기 인코딩된 값을 파싱합니다. =====
     */
    private static RdbParseResult parseSize(byte[] data, int pos) {
        if (pos >= data.length) return null;
        
        int firstByte = data[pos] & 0xFF;
        int type = (firstByte & 0xC0) >> 6; // 상위 2비트
        
        switch (type) {
            case 0: // 00: 6비트 크기
                return new RdbParseResult(firstByte & 0x3F, pos + 1, null);
            case 1: // 01: 14비트 크기
                if (pos + 1 >= data.length) return null;
                int size14 = ((firstByte & 0x3F) << 8) | (data[pos + 1] & 0xFF);
                return new RdbParseResult(size14, pos + 2, null);
            case 2: // 10: 32비트 크기
                if (pos + 4 >= data.length) return null;
                long size32 = 0;
                for (int i = 1; i <= 4; i++) {
                    size32 = (size32 << 8) | (data[pos + i] & 0xFF);
                }
                return new RdbParseResult((int) size32, pos + 5, null);
            default: // 11: 특별한 인코딩
                return new RdbParseResult(firstByte & 0x3F, pos + 1, null);
        }
    }
    
    /**
     * ===== STAGE 9: RDB 파일에서 문자열을 파싱합니다. =====
     */
    private static RdbParseResult parseString(byte[] data, int pos) {
        RdbParseResult sizeResult = parseSize(data, pos);
        if (sizeResult == null) return null;
        
        int length = sizeResult.value;
        int nextPos = sizeResult.nextPos;
        
        if (nextPos + length > data.length) return null;
        
        // 특별한 인코딩 처리
        int firstByte = data[pos] & 0xFF;
        int type = (firstByte & 0xC0) >> 6;
        
        if (type == 3) { // 11: 특별한 인코딩
            int encoding = firstByte & 0x3F;
            if (encoding == 0) { // C0: 8비트 정수
                if (nextPos >= data.length) return null;
                int value = data[nextPos] & 0xFF;
                return new RdbParseResult(0, nextPos + 1, String.valueOf(value));
            } else if (encoding == 1) { // C1: 16비트 정수
                if (nextPos + 1 >= data.length) return null;
                int value = (data[nextPos] & 0xFF) | ((data[nextPos + 1] & 0xFF) << 8);
                return new RdbParseResult(0, nextPos + 2, String.valueOf(value));
            } else if (encoding == 2) { // C2: 32비트 정수
                if (nextPos + 3 >= data.length) return null;
                long value = 0;
                for (int i = 0; i < 4; i++) {
                    value |= ((long) (data[nextPos + i] & 0xFF)) << (i * 8);
                }
                return new RdbParseResult(0, nextPos + 4, String.valueOf(value));
            }
            // 다른 인코딩은 건너뛰기
            return new RdbParseResult(0, nextPos, "");
        }
        
        // 일반 문자열
        byte[] stringBytes = new byte[length];
        System.arraycopy(data, nextPos, stringBytes, 0, length);
        String stringValue = new String(stringBytes);
        
        return new RdbParseResult(length, nextPos + length, stringValue);
    }
    
    /**
     * RDB 파싱 결과를 담는 클래스
     */
    /**
     * ===== STAGE 11: Redis Streams XADD 명령어를 처리합니다. =====
     */
    private static String handleXAddCommand(List<String> args) {
        if (args.size() < 4) {
            return "-ERR wrong number of arguments for 'XADD' command\r\n";
        }
        
        String streamKey = args.get(1);
        String entryId = args.get(2);
        
        // 필드-값 쌍 추출
        List<String> fieldValues = args.subList(3, args.size());
        if (fieldValues.size() % 2 != 0) {
            return "-ERR wrong number of arguments for 'XADD' command\r\n";
        }
        
        // 엔트리 ID 검증 및 처리
        String finalEntryId = processEntryId(streamKey, entryId);
        if (finalEntryId.startsWith("-ERR")) {
            return finalEntryId;
        }
        
        // 스트림 엔트리 생성
        StreamEntry entry = new StreamEntry(finalEntryId, fieldValues);
        
        // 스트림에 추가
        streams.computeIfAbsent(streamKey, k -> new ArrayList<>()).add(entry);
        
        System.out.println("Added stream entry: " + streamKey + " " + finalEntryId);
        return createBulkString(finalEntryId);
    }
    
    /**
     * ===== STAGE 12: Redis Streams XRANGE 명령어를 처리합니다. =====
     */
    private static String handleXRangeCommand(List<String> args) {
        if (args.size() < 4) {
            return "-ERR wrong number of arguments for 'XRANGE' command\r\n";
        }
        
        String streamKey = args.get(1);
        String start = args.get(2);
        String end = args.get(3);
        
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries == null || streamEntries.isEmpty()) {
            return "*0\r\n"; // empty array
        }
        
        List<StreamEntry> result = new ArrayList<>();
        
        for (StreamEntry entry : streamEntries) {
            if (isInRange(entry.id, start, end)) {
                result.add(entry);
            }
        }
        
        return formatStreamEntries(result);
    }
    
    /**
     * ===== STAGE 13: Redis Streams XREAD 명령어를 처리합니다. =====
     */
    private static String handleXReadCommand(List<String> args) {
        if (args.size() < 4) {
            return "-ERR wrong number of arguments for 'XREAD' command\r\n";
        }
        
        // XREAD streams streamKey lastId
        if (!"streams".equalsIgnoreCase(args.get(1))) {
            return "-ERR wrong arguments for 'XREAD' command\r\n";
        }
        
        String streamKey = args.get(2);
        String lastId = args.get(3);
        
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries == null || streamEntries.isEmpty()) {
            return "*0\r\n"; // empty array
        }
        
        List<StreamEntry> result = new ArrayList<>();
        
        for (StreamEntry entry : streamEntries) {
            if (compareIds(entry.id, lastId) > 0) {
                result.add(entry);
            }
        }
        
        if (result.isEmpty()) {
            return "*0\r\n";
        }
        
        // XREAD 형식: [streamKey, [entries]]
        StringBuilder sb = new StringBuilder();
        sb.append("*1\r\n"); // 1개 스트림
        sb.append("*2\r\n"); // [streamKey, entries]
        sb.append(createBulkString(streamKey));
        sb.append(formatStreamEntries(result));
        
        return sb.toString();
    }
    
    /**
     * ===== STAGE 11: 엔트리 ID를 처리합니다 (자동 생성 포함) =====
     */
    private static String processEntryId(String streamKey, String entryId) {
        if ("*".equals(entryId)) {
            // 자동 ID 생성
            long currentTime = System.currentTimeMillis();
            return currentTime + "-0";
        }
        
        if (entryId.endsWith("-*")) {
            // 시간은 주어지고 시퀀스는 자동 생성
            String timeStr = entryId.substring(0, entryId.length() - 2);
            return timeStr + "-0";
        }
        
        // ID 검증
        if ("0-0".equals(entryId)) {
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        }
        
        // 기존 엔트리와 비교
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries != null && !streamEntries.isEmpty()) {
            StreamEntry lastEntry = streamEntries.get(streamEntries.size() - 1);
            if (compareIds(entryId, lastEntry.id) <= 0) {
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
            }
        }
        
        return entryId;
    }
    
    /**
     * ===== STAGE 12: ID가 범위 내에 있는지 확인합니다 =====
     */
    private static boolean isInRange(String id, String start, String end) {
        if ("-".equals(start)) {
            start = "0-0";
        }
        if ("+".equals(end)) {
            return compareIds(id, start) >= 0;
        }
        
        return compareIds(id, start) >= 0 && compareIds(id, end) <= 0;
    }
    
    /**
     * ===== STAGE 11-13: 두 엔트리 ID를 비교합니다 =====
     */
    private static int compareIds(String id1, String id2) {
        String[] parts1 = id1.split("-");
        String[] parts2 = id2.split("-");
        
        long time1 = Long.parseLong(parts1[0]);
        long time2 = Long.parseLong(parts2[0]);
        
        if (time1 != time2) {
            return Long.compare(time1, time2);
        }
        
        long seq1 = parts1.length > 1 ? Long.parseLong(parts1[1]) : 0;
        long seq2 = parts2.length > 1 ? Long.parseLong(parts2[1]) : 0;
        
        return Long.compare(seq1, seq2);
    }
    
    /**
     * ===== STAGE 12-13: 스트림 엔트리들을 RESP 형식으로 포맷합니다 =====
     */
    private static String formatStreamEntries(List<StreamEntry> entries) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(entries.size()).append("\r\n");
        
        for (StreamEntry entry : entries) {
            sb.append("*2\r\n"); // [id, [field, value, ...]]
            sb.append(createBulkString(entry.id));
            sb.append("*").append(entry.fieldValues.size()).append("\r\n");
            for (String fieldValue : entry.fieldValues) {
                sb.append(createBulkString(fieldValue));
            }
        }
        
        return sb.toString();
    }
    
    /**
     * ===== STAGE 9: RDB 파싱 결과를 저장하는 헬퍼 클래스 =====
     */
    private static class RdbParseResult {
        final int value;
        final int nextPos;
        final String stringValue;
        
        RdbParseResult(int value, int nextPos, String stringValue) {
            this.value = value;
            this.nextPos = nextPos;
            this.stringValue = stringValue;
        }
    }
    
    /**
     * ===== STAGE 11-13: Redis Streams 엔트리를 저장하는 클래스 =====
     */
    private static class StreamEntry {
        final String id;
        final List<String> fieldValues;
        
        StreamEntry(String id, List<String> fieldValues) {
            this.id = id;
            this.fieldValues = new ArrayList<>(fieldValues);
        }
    }
}
