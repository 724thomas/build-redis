package streams;

import protocol.RespProtocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Redis Streams 기능을 관리하는 클래스
 */
public class StreamsManager {
    
    private final Map<String, List<StreamEntry>> streams = new ConcurrentHashMap<>();
    
    /**
     * 스트림에 엔트리를 추가합니다.
     */
    public String addEntry(String streamKey, String entryIdStr, List<String> fieldValues) {
        if (fieldValues.size() % 2 != 0) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'XADD' command");
        }
        
        // 엔트리 ID 검증 및 처리
        StreamEntryId finalEntryId;
        try {
            finalEntryId = processEntryId(streamKey, entryIdStr);
        } catch (IllegalArgumentException e) {
            return RespProtocol.createErrorResponse(e.getMessage());
        }
        
        if (finalEntryId == null) {
            // processEntryId에서 에러 응답을 직접 생성하는 경우
            if ("0-0".equals(entryIdStr)) {
                return RespProtocol.createErrorResponse("The ID specified in XADD must be greater than 0-0");
            }
            return RespProtocol.createErrorResponse("The ID specified in XADD is equal or smaller than the target stream top item");
        }
        
        // 스트림 엔트리 생성
        StreamEntry entry = new StreamEntry(finalEntryId, fieldValues);
        
        // 스트림에 추가
        streams.computeIfAbsent(streamKey, k -> new ArrayList<>()).add(entry);
        
        return RespProtocol.createBulkString(finalEntryId.toString());
    }
    
    /**
     * 스트림에서 범위 조회를 수행합니다.
     */
    public String getRange(String streamKey, String startStr, String endStr) {
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries == null || streamEntries.isEmpty()) {
            return RespProtocol.createEmptyArray();
        }
        
        StreamEntryId startId = parseRangeId(startStr, true);
        StreamEntryId endId = parseRangeId(endStr, false);
        
        List<StreamEntry> result = streamEntries.stream()
                .filter(entry -> isInRange(entry.getId(), startId, endId))
                .collect(Collectors.toList());
        
        return formatStreamEntries(result);
    }
    
    private StreamEntryId parseRangeId(String idStr, boolean isStart) {
        if ("-".equals(idStr)) return new StreamEntryId(0, 0);
        if ("+".equals(idStr)) return new StreamEntryId(Long.MAX_VALUE, Long.MAX_VALUE);
        
        String[] parts = idStr.split("-");
        long time = Long.parseLong(parts[0]);
        long sequence;
        if (parts.length == 2) {
            sequence = Long.parseLong(parts[1]);
        } else {
            sequence = isStart ? 0 : Long.MAX_VALUE;
        }
        return new StreamEntryId(time, sequence);
    }
    
    /**
     * 스트림에서 특정 ID 이후의 엔트리들을 읽어옵니다.
     */
    public String readStream(String streamKey, String lastIdStr) {
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries == null || streamEntries.isEmpty()) {
            return RespProtocol.createEmptyArray();
        }
        
        StreamEntryId lastId = StreamEntryId.fromString(lastIdStr);
        
        List<StreamEntry> result = streamEntries.stream()
                .filter(entry -> entry.getId().compareTo(lastId) > 0)
                .collect(Collectors.toList());
        
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
    private StreamEntryId processEntryId(String streamKey, String entryIdStr) {
        if ("*".equals(entryIdStr)) {
            long currentTime = System.currentTimeMillis();
            List<StreamEntry> streamEntries = streams.get(streamKey);
            if (streamEntries != null && !streamEntries.isEmpty()) {
                StreamEntryId lastId = streamEntries.get(streamEntries.size() - 1).getId();
                if (lastId.getTime() == currentTime) {
                    return new StreamEntryId(currentTime, lastId.getSequence() + 1);
                }
            }
            return new StreamEntryId(currentTime, 0);
        }
        
        if (entryIdStr.endsWith("-*")) {
            long time = Long.parseLong(entryIdStr.substring(0, entryIdStr.length() - 2));
            List<StreamEntry> streamEntries = streams.get(streamKey);
            long sequence = 0;
            if (streamEntries != null && !streamEntries.isEmpty()) {
                StreamEntryId lastId = streamEntries.get(streamEntries.size() - 1).getId();
                if (lastId.getTime() == time) {
                    sequence = lastId.getSequence() + 1;
                }
            }
            if (time == 0 && sequence == 0) {
                sequence = 1;
            }
            return new StreamEntryId(time, sequence);
        }
        
        StreamEntryId currentId = StreamEntryId.fromString(entryIdStr);
        
        if (currentId.getTime() == 0 && currentId.getSequence() == 0) {
            throw new IllegalArgumentException("The ID specified in XADD must be greater than 0-0");
        }
        
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries != null && !streamEntries.isEmpty()) {
            StreamEntryId lastId = streamEntries.get(streamEntries.size() - 1).getId();
            if (currentId.compareTo(lastId) <= 0) {
                throw new IllegalArgumentException("The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }
        
        return currentId;
    }
    
    /**
     * ID가 범위 내에 있는지 확인합니다
     */
    private boolean isInRange(StreamEntryId id, StreamEntryId start, StreamEntryId end) {
        return id.compareTo(start) >= 0 && id.compareTo(end) <= 0;
    }
    
    /**
     * 스트림 엔트리들을 RESP 형식으로 포맷합니다
     */
    private String formatStreamEntries(List<StreamEntry> entries) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(entries.size()).append("\r\n");
        
        for (StreamEntry entry : entries) {
            sb.append("*2\r\n"); // [id, [field, value, ...]]
            sb.append(RespProtocol.createBulkString(entry.getId().toString()));
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
    public static class StreamEntry {
        private final StreamEntryId id;
        private final List<String> fieldValues;
        
        public StreamEntry(StreamEntryId id, List<String> fieldValues) {
            this.id = id;
            this.fieldValues = new ArrayList<>(fieldValues);
        }
        
        public StreamEntryId getId() {
            return id;
        }
        
        public List<String> getFieldValues() {
            return fieldValues;
        }
    }
} 