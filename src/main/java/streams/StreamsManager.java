package streams;

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
     * Represents a stream entry ID.
     */
    public record StreamId(long time, long sequence) implements Comparable<StreamId> {
        public static StreamId fromString(String idStr) {
            if (idStr == null || !idStr.contains("-")) {
                throw new IllegalArgumentException("Invalid stream ID format");
            }
            String[] parts = idStr.split("-");
            return new StreamId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        }

        @Override
        public String toString() {
            return time + "-" + sequence;
        }

        @Override
        public int compareTo(StreamId other) {
            if (this.time != other.time) {
                return Long.compare(this.time, other.time);
            }
            return Long.compare(this.sequence, other.sequence);
        }
    }
    
    /**
     * 스트림에 엔트리를 추가합니다.
     */
    public String addEntry(String streamKey, String entryId, List<String> fieldValues) {
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
        StreamId lastStreamId = StreamId.fromString(lastId);

        for (StreamEntry entry : streamEntries) {
            if (StreamId.fromString(entry.getId()).compareTo(lastStreamId) > 0) {
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

    public boolean exists(String key) {
        return streams.containsKey(key);
    }
    
    /**
     * 엔트리 ID를 처리합니다 (자동 생성 포함)
     */
    private String processEntryId(String streamKey, String entryId) {
        List<StreamEntry> streamEntries = streams.get(streamKey);
        StreamEntry lastEntry = (streamEntries != null && !streamEntries.isEmpty()) ? streamEntries.get(streamEntries.size() - 1) : null;
        StreamId lastId = lastEntry != null ? StreamId.fromString(lastEntry.getId()) : null;

        if ("*".equals(entryId)) {
            long currentTime = System.currentTimeMillis();
            if (lastId != null && lastId.time() == currentTime) {
                return new StreamId(currentTime, lastId.sequence() + 1).toString();
            }
            return new StreamId(currentTime, 0).toString();
        }

        if (entryId.endsWith("-*")) {
            long timePart = Long.parseLong(entryId.substring(0, entryId.indexOf('-')));
            if (lastId != null && lastId.time() == timePart) {
                return new StreamId(timePart, lastId.sequence() + 1).toString();
            } else if (timePart == 0) {
                return new StreamId(0, 1).toString();
            }
            return new StreamId(timePart, 0).toString();
        }

        StreamId newId;
        try {
            newId = StreamId.fromString(entryId);
        } catch (IllegalArgumentException e) {
            return RespProtocol.createErrorResponse("Invalid stream ID specified");
        }

        if (newId.compareTo(new StreamId(0, 0)) <= 0) {
            return RespProtocol.createErrorResponse("The ID specified in XADD must be greater than 0-0");
        }

        if (lastId != null && newId.compareTo(lastId) <= 0) {
            return RespProtocol.createErrorResponse("The ID specified in XADD is equal or smaller than the target stream top item");
        }
        
        return entryId;
    }
    
    /**
     * ID가 범위 내에 있는지 확인합니다
     */
    private boolean isInRange(String id, String start, String end) {
        StreamId streamId = StreamId.fromString(id);
        StreamId startId = parseRangeId(start, true);
        StreamId endId = parseRangeId(end, false);
        
        return streamId.compareTo(startId) >= 0 && streamId.compareTo(endId) <= 0;
    }
    
    private StreamId parseRangeId(String idStr, boolean isStart) {
        if ("-".equals(idStr)) {
            return new StreamId(0, 0);
        }
        if ("+".equals(idStr)) {
            return new StreamId(Long.MAX_VALUE, Long.MAX_VALUE);
        }
        if (!idStr.contains("-")) {
            long time = Long.parseLong(idStr);
            return isStart ? new StreamId(time, 0) : new StreamId(time, Long.MAX_VALUE);
        }
        return StreamId.fromString(idStr);
    }

    /**
     * 두 엔트리 ID를 비교합니다
     */
    private int compareIds(String id1, String id2) {
        return StreamId.fromString(id1).compareTo(StreamId.fromString(id2));
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
    public static class StreamEntry {
        private final String id;
        private final List<String> fieldValues;
        
        // 커스텀 생성자 - fieldValues를 새로운 ArrayList로 복사
        public StreamEntry(String id, List<String> fieldValues) {
            this.id = id;
            this.fieldValues = new ArrayList<>(fieldValues);
        }
        
        // Getter 메서드들
        public String getId() {
            return id;
        }
        
        public List<String> getFieldValues() {
            return fieldValues;
        }
    }
} 