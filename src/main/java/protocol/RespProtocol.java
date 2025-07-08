package protocol;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Redis RESP 프로토콜 파싱 및 응답 생성을 담당하는 클래스
 */
public class RespProtocol {
    
    public static final String PONG_RESPONSE = "+PONG\r\n";
    public static final String OK_RESPONSE = "+OK\r\n";
    
    /**
     * RESP 배열 형식을 파싱합니다.
     */
    public static List<String> parseRespArray(BufferedReader reader, int arrayLength) throws IOException {
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
     * RESP bulk string 형식으로 문자열을 인코딩합니다.
     */
    public static String createBulkString(String value) {
        if (value == null) {
            return "$-1\r\n"; // null bulk string
        }
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }
    
    /**
     * RESP 배열을 생성합니다.
     */
    public static String createRespArray(String[] elements) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(elements.length).append("\r\n");
        
        for (String element : elements) {
            sb.append(createBulkString(element));
        }
        
        return sb.toString();
    }
    
    /**
     * 에러 메시지를 RESP 형식으로 생성합니다.
     */
    public static String createErrorResponse(String message) {
        return "-ERR " + message + "\r\n";
    }
    
    /**
     * 단순 문자열 응답을 RESP 형식으로 생성합니다.
     */
    public static String createSimpleString(String value) {
        return "+" + value + "\r\n";
    }
    
    /**
     * 정수 응답을 RESP 형식으로 생성합니다.
     */
    public static String createInteger(long value) {
        return ":" + value + "\r\n";
    }
    
    /**
     * 빈 배열을 RESP 형식으로 생성합니다.
     */
    public static String createEmptyArray() {
        return "*0\r\n";
    }
} 