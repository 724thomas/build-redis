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
     * Represents a stream entry ID.
     */
    public record StreamId(long time, long sequence) implements Comparable<StreamId> {
        public static StreamId fromString(String idStr) {
            if (idStr == null) {
                throw new IllegalArgumentException("Invalid stream ID format: null");
            }
            if ("-".equals(idStr)) {
                return new StreamId(0, 0);
            }
            if ("+".equals(idStr)) {
                return new StreamId(Long.MAX_VALUE, Long.MAX_VALUE);
            }
            
            String[] parts = idStr.split("-");
            if (parts.length == 1) {
                return new StreamId(Long.parseLong(parts[0]), 0);
            }
            if (parts.length == 2) {
                return new StreamId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
            }
            throw new IllegalArgumentException("Invalid stream ID format: " + idStr);
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
     * Redis Streams 엔트리를 저장하는 내부 클래스
     */
    public static class StreamEntry {
        private final StreamId id;
        private final List<String> fieldValues;
        
        public StreamEntry(StreamId id, List<String> fieldValues) {
            this.id = id;
            this.fieldValues = fieldValues;
        }
        
        public StreamId getId() {
            return id;
        }
        
        public List<String> getFieldValues() {
            return fieldValues;
        }
    }
    
    /**
     * 스트림에 엔트리를 추가합니다.
     */
    public String addEntry(String streamKey, String entryIdStr, List<String> fieldValues) {
        if (fieldValues.size() % 2 != 0) {
            return RespProtocol.createErrorResponse("wrong number of arguments for 'XADD' command");
        }
        
        StreamId finalEntryId;
        try {
            finalEntryId = processEntryId(streamKey, entryIdStr);
        } catch (IllegalArgumentException e) {
            return RespProtocol.createErrorResponse(e.getMessage());
        }
        
        StreamEntry entry = new StreamEntry(finalEntryId, fieldValues);
        
        streams.computeIfAbsent(streamKey, k -> new ArrayList<>()).add(entry);
        
        return RespProtocol.createBulkString(finalEntryId.toString());
    }
    
    /**
     * 스트림에서 범위 조회를 수행합니다.
     */
    public String xrange(String streamKey, String startStr, String endStr) {
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries == null || streamEntries.isEmpty()) {
            return RespProtocol.createEmptyArray();
        }
        
        StreamId startId = parseRangeId(startStr, true);
        StreamId endId = parseRangeId(endStr, false);
        
        List<StreamEntry> result = streamEntries.stream()
                                                 .filter(entry -> isInRange(entry.getId(), startId, endId))
                                                 .collect(Collectors.toList());
        
        return formatStreamEntries(result);
    }
    
    private boolean isInRange(StreamId id, StreamId startId, StreamId endId) {
        return id.compareTo(startId) >= 0 && id.compareTo(endId) <= 0;
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
     * 스트림에서 특정 ID 이후의 엔트리들을 읽어옵니다.
     */
    public String readStream(String streamKey, String lastIdStr) {
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries == null || streamEntries.isEmpty()) {
            return RespProtocol.createEmptyArray();
        }
        
        StreamId lastStreamId;
        if ("$".equals(lastIdStr)) {
            if (streamEntries.isEmpty()) {
                return RespProtocol.createEmptyArray();
            }
            lastStreamId = streamEntries.get(streamEntries.size() - 1).getId();
        } else {
            lastStreamId = StreamId.fromString(lastIdStr);
        }
        
        List<StreamEntry> result = streamEntries.stream()
                                                 .filter(entry -> entry.getId().compareTo(lastStreamId) > 0)
                                                 .collect(Collectors.toList());
        
        if (result.isEmpty()) {
            return RespProtocol.createEmptyArray();
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("*1\r\n");
        sb.append("*2\r\n");
        sb.append(RespProtocol.createBulkString(streamKey));
        sb.append(formatStreamEntries(result));
        
        return sb.toString();
    }
    
    public boolean exists(String key) {
        return streams.containsKey(key);
    }
    
    private StreamId processEntryId(String streamKey, String entryIdStr) {
        if ("*".equals(entryIdStr)) {
            long currentTime = System.currentTimeMillis();
            List<StreamEntry> streamEntries = streams.get(streamKey);
            if (streamEntries != null && !streamEntries.isEmpty()) {
                StreamId lastId = streamEntries.get(streamEntries.size() - 1).getId();
                if (lastId.time() == currentTime) {
                    return new StreamId(currentTime, lastId.sequence() + 1);
                }
            }
            return new StreamId(currentTime, 0);
        }
        
        if (entryIdStr.endsWith("-*")) {
            long time = Long.parseLong(entryIdStr.substring(0, entryIdStr.length() - 2));
            List<StreamEntry> streamEntries = streams.get(streamKey);
            long sequence = 0;
            if (streamEntries != null && !streamEntries.isEmpty()) {
                StreamId lastId = streamEntries.get(streamEntries.size() - 1).getId();
                if (lastId.time() == time) {
                    sequence = lastId.sequence() + 1;
                }
            }
            if (time == 0 && sequence == 0) {
                sequence = 1;
            }
            return new StreamId(time, sequence);
        }
        
        StreamId currentId = StreamId.fromString(entryIdStr);
        
        if (currentId.time() == 0 && currentId.sequence() == 0) {
            throw new IllegalArgumentException("The ID specified in XADD must be greater than 0-0");
        }
        
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries != null && !streamEntries.isEmpty()) {
            StreamId lastId = streamEntries.get(streamEntries.size() - 1).getId();
            if (currentId.compareTo(lastId) <= 0) {
                throw new IllegalArgumentException("The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }
        
        return currentId;
    }
    
    private String formatStreamEntries(List<StreamEntry> entries) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(entries.size()).append("\r\n");
        
        for (StreamEntry entry : entries) {
            sb.append("*2\r\n");
            sb.append(RespProtocol.createBulkString(entry.getId().toString()));
            sb.append("*").append(entry.getFieldValues().size()).append("\r\n");
            for (String fieldValue : entry.getFieldValues()) {
                sb.append(RespProtocol.createBulkString(fieldValue));
            }
        }
        
        return sb.toString();
    }
} 