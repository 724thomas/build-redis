package protocol;

import model.StreamEntry;
import model.StreamId;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.io.ByteArrayOutputStream;
import org.apache.commons.io.input.TeeInputStream;

/**
 * Redis RESP 프로토콜 파싱 및 응답 생성을 담당하는 클래스
 */
public class RespProtocol {
    
    public static final String PONG_RESPONSE = "+PONG\r\n";
    public static final String OK_RESPONSE = "+OK\r\n";
    public static final String QUEUED_RESPONSE = "+QUEUED\r\n";
    private static final String EMPTY_RDB_HEX = "524544495330303131FE00FF6BFD95240E87F293";
    public static final byte[] EMPTY_RDB_BYTES = hexStringToByteArray("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d626974730140fa056374696d65c26d08bc65fa08757365642d6d656d02b0c0ff00fe00fb000000ff959a41639e7288f7");

    /**
     * RESP 프로토콜을 파싱하여 명령어와 바이트 수를 반환합니다.
     * @return List<Object> - [List<String> commands, Integer byteCount]
     */
    public static List<Object> parseResp(InputStream in) throws IOException {
        ByteArrayOutputStream commandBytes = new ByteArrayOutputStream();
        InputStream tee = new TeeInputStream(in, commandBytes);
        
        String arrayHeader = readLine(tee);
        if (arrayHeader == null || !arrayHeader.startsWith("*")) {
            return null; // or throw exception
        }
        
        int arrayLength = Integer.parseInt(arrayHeader.substring(1).trim());
        List<String> commands = new ArrayList<>();
        
        for (int i = 0; i < arrayLength; i++) {
            String bulkHeader = readLine(tee);
            if (bulkHeader == null || !bulkHeader.startsWith("$")) {
                throw new IOException("Expected bulk string header");
            }
            int bulkLength = Integer.parseInt(bulkHeader.substring(1).trim());
            
            if (bulkLength == -1) {
                commands.add(null);
            } else {
                byte[] bulkContent = new byte[bulkLength];
                int bytesRead = tee.read(bulkContent, 0, bulkLength);
                if (bytesRead != bulkLength) {
                    throw new IOException("Mismatched bulk string length");
                }
                // CRLF consuming
                readLine(tee);
                commands.add(new String(bulkContent, StandardCharsets.UTF_8));
            }
        }
        
        return Arrays.asList(commands, commandBytes.size());
    }
    
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
            return "$-1\r\n"; // Null Bulk String
        }
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }

    public static String createNullBulkString() {
        return "$-1\r\n"; // Null Bulk String
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
     * Raw RESP 응답 목록으로 RESP 배열을 생성합니다.
     * EXEC 명령어 응답 처리에 사용됩니다.
     */
    public static String createRespArrayFromRaw(List<String> rawResponses) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(rawResponses.size()).append("\r\n");
        for (String response : rawResponses) {
            sb.append(response);
        }
        return sb.toString();
    }
    
    public static String createArrayOfArrays(List<Object> arrays) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(arrays.size()).append("\r\n");
        for (Object item : arrays) {
            sb.append(formatArrayItem(item));
        }
        return sb.toString();
    }
    
    private static String formatArrayItem(Object item) {
        if (item instanceof List) {
            return createArrayOfArrays((List<Object>) item);
        } else if (item instanceof String) {
            return createBulkString((String) item);
        } else {
            // Handle other types if necessary, or throw an error
            return createBulkString(item.toString());
        }
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
     * RESP 정수를 생성합니다.
     */
    public static String createInteger(int value) {
        return ":" + value + "\r\n";
    }
    
    /**
     * 빈 배열을 RESP 형식으로 생성합니다.
     */
    public static String createEmptyArray() {
        return "*0\r\n";
    }
    
    /**
     * null 배열을 RESP 형식으로 생성합니다.
     */
    public static String createNullArray() {
        return "*-1\r\n";
    }
    
    /**
     * 여러 개의 bulk string을 배열로 생성합니다.
     */
    public static String createBulkStringArray(List<String> values) {
        if (values == null || values.isEmpty()) {
            return createEmptyArray();
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(values.size()).append("\r\n");
        
        for (String value : values) {
            sb.append(createBulkString(value));
        }
        
        return sb.toString();
    }
    
    /**
     * XRANGE 명령어의 응답을 RESP 형식으로 포맷팅합니다.
     */
    public static String formatXRangeResponse(List<StreamEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return createEmptyArray();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("*").append(entries.size()).append("\r\n");

        for (StreamEntry entry : entries) {
            sb.append("*2\r\n"); // Array for [entryId, fieldValues]
            sb.append(createBulkString(entry.getId().toString()));

            List<String> fieldValues = entry.getFieldValues();
            sb.append("*").append(fieldValues.size()).append("\r\n"); // Array for fieldValues
            for (String fieldValue : fieldValues) {
                sb.append(createBulkString(fieldValue));
            }
        }
        return sb.toString();
    }


    /**
     * XREAD 명령어의 응답을 RESP 형식으로 포맷팅합니다.
     */
    public static String formatXReadResponse(Map<String, List<StreamEntry>> result) {
        if (result == null || result.isEmpty()) {
            return createNullArray(); // As per XREAD spec, it should be null when no new entries or timeout
        }

        StringBuilder sb = new StringBuilder();
        sb.append("*").append(result.size()).append("\r\n");

        for (Map.Entry<String, List<StreamEntry>> streamResult : result.entrySet()) {
            String streamKey = streamResult.getKey();
            List<StreamEntry> entries = streamResult.getValue();

            sb.append("*2\r\n"); // Array for [streamKey, entries]
            sb.append(createBulkString(streamKey));

            sb.append(formatXRangeResponse(entries));
        }
        return sb.toString();
    }
    
    /**
     * 입력 스트림에서 한 줄을 읽습니다 (CRLF 포함).
     */
    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            bos.write(b);
            if (b == '\r') {
                int nextByte = in.read();
                if (nextByte == -1) break;
                bos.write(nextByte);
                if (nextByte == '\n') {
                    break;
                }
            }
        }
        if (bos.size() == 0) return null;
        return bos.toString(StandardCharsets.UTF_8);
    }
    
    /**
     * 16진수 문자열을 바이트 배열로 변환합니다.
     */
    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }
} 