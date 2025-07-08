package streams;

import lombok.Getter;
import protocol.RespProtocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis Streams 기능을 관리하는 클래스
 */
public class StreamsManager {
    
    private final Map<String, List<StreamEntry>> streams = new ConcurrentHashMap<>();
    
    /**
     * 스트림에 엔트리를 추가합니다.
     */
    public String addEntry(String streamKey, String entryId, List<String> fieldValues) {
        if (fieldValues.size() % 2 != 0) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'XADD' command");
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
        return RespProtocol.createBulkString(finalEntryId);
    }
    
    /**
     * 스트림에서 범위 조회를 수행합니다.
     */
    public String getRange(String streamKey, String start, String end) {
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries == null || streamEntries.isEmpty()) {
            return RespProtocol.createEmptyArray();
        }
        
        List<StreamEntry> result = new ArrayList<>();
        
        for (StreamEntry entry : streamEntries) {
            if (isInRange(entry.getId(), start, end)) {
                result.add(entry);
            }
        }
        
        return formatStreamEntries(result);
    }
    
    /**
     * 스트림에서 특정 ID 이후의 엔트리들을 읽어옵니다.
     */
    public String readStream(String streamKey, String lastId) {
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries == null || streamEntries.isEmpty()) {
            return RespProtocol.createEmptyArray();
        }
        
        List<StreamEntry> result = new ArrayList<>();
        
        for (StreamEntry entry : streamEntries) {
            if (compareIds(entry.getId(), lastId) > 0) {
                result.add(entry);
            }
        }
        
        if (result.isEmpty()) {
            return RespProtocol.createEmptyArray();
        }
        
        // XREAD 형식: [streamKey, [entries]]
        StringBuilder sb = new StringBuilder();
        sb.append("*1\r\n"); // 1개 스트림
        sb.append("*2\r\n"); // [streamKey, entries]
        sb.append(RespProtocol.createBulkString(streamKey));
        sb.append(formatStreamEntries(result));
        
        return sb.toString();
    }
    
    /**
     * 엔트리 ID를 처리합니다 (자동 생성 포함)
     */
    private String processEntryId(String streamKey, String entryId) {
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
            return RespProtocol.createErrorResponse("The ID specified in XADD must be greater than 0-0");
        }
        
        // 기존 엔트리와 비교
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries != null && !streamEntries.isEmpty()) {
            StreamEntry lastEntry = streamEntries.get(streamEntries.size() - 1);
            if (compareIds(entryId, lastEntry.getId()) <= 0) {
                return RespProtocol.createErrorResponse("The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }
        
        return entryId;
    }
    
    /**
     * ID가 범위 내에 있는지 확인합니다
     */
    private boolean isInRange(String id, String start, String end) {
        if ("-".equals(start)) {
            start = "0-0";
        }
        if ("+".equals(end)) {
            return compareIds(id, start) >= 0;
        }
        
        return compareIds(id, start) >= 0 && compareIds(id, end) <= 0;
    }
    
    /**
     * 두 엔트리 ID를 비교합니다
     */
    private int compareIds(String id1, String id2) {
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
     * 스트림 엔트리들을 RESP 형식으로 포맷합니다
     */
    private String formatStreamEntries(List<StreamEntry> entries) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(entries.size()).append("\r\n");
        
        for (StreamEntry entry : entries) {
            sb.append("*2\r\n"); // [id, [field, value, ...]]
            sb.append(RespProtocol.createBulkString(entry.getId()));
            sb.append("*").append(entry.getFieldValues().size()).append("\r\n");
            for (String fieldValue : entry.getFieldValues()) {
                sb.append(RespProtocol.createBulkString(fieldValue));
            }
        }
        
        return sb.toString();
    }
    
    /**
     * Redis Streams 엔트리를 저장하는 내부 클래스
     */
    @Getter
    public static class StreamEntry {
        private final String id;
        private final List<String> fieldValues;
        
        // 커스텀 생성자 - fieldValues를 새로운 ArrayList로 복사
        public StreamEntry(String id, List<String> fieldValues) {
            this.id = id;
            this.fieldValues = new ArrayList<>(fieldValues);
        }
        
        // Lombok이 getter 메서드들을 자동 생성합니다
    }
} 