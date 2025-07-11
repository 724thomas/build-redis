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
    private final Object notifier = new Object();
    
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
     * 스트림에서 특정 ID 이후의 엔트리들을 가져옵니다.
     */
    public List<StreamEntry> getEntriesAfter(String streamKey, StreamId startId) {
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries == null || streamEntries.isEmpty()) {
            return new ArrayList<>();
        }
        
        return streamEntries.stream()
            .filter(entry -> entry.getId().compareTo(startId) > 0)
            .collect(Collectors.toList());
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
        
        synchronized (notifier) {
            notifier.notifyAll();
        }
        
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
        
        return formatXRangeResponse(result);
    }
    
    private boolean isInRange(StreamId id, StreamId startId, StreamId endId) {
        return id.compareTo(startId) >= 0 && id.compareTo(endId) <= 0;
    }
    
    private StreamId parseRangeId(String idStr, boolean isStart) {
        if ("-".equals(idStr)) {
            return new StreamId(0, 0); // Represents the minimum possible ID
        }
        if ("+".equals(idStr)) {
            return new StreamId(Long.MAX_VALUE, Long.MAX_VALUE); // Represents the maximum possible ID
        }
        if (!idStr.contains("-")) {
            long time = Long.parseLong(idStr);
            return isStart ? new StreamId(time, 0) : new StreamId(time, Long.MAX_VALUE);
        }
        return StreamId.fromString(idStr);
    }

    public String readStreams(List<String> streamKeys, List<String> lastIdStrs) {
        return readStreams(streamKeys, lastIdStrs, -1); // Non-blocking by default
    }
    
    public String readStreams(List<String> streamKeys, List<String> lastIdStrs, long timeout) {
        // For blocking calls, we must resolve '$' to the current latest ID *before* waiting.
        List<String> idsForRead = new ArrayList<>(lastIdStrs);
        if (timeout >= 0) {
            for (int i = 0; i < streamKeys.size(); i++) {
                if ("$".equals(idsForRead.get(i))) {
                    String streamKey = streamKeys.get(i);
                    List<StreamEntry> streamEntries = streams.get(streamKey);
                    if (streamEntries != null && !streamEntries.isEmpty()) {
                        idsForRead.set(i, streamEntries.get(streamEntries.size() - 1).getId().toString());
                    } else {
                        // If stream is empty, we want to get all entries from the beginning
                        // once they are added.
                        idsForRead.set(i, "0-0");
                    }
                }
            }
        }
        
        // First, try a non-blocking read.
        List<Object> results = readStreamsNonBlocking(streamKeys, idsForRead);
        
        if (!results.isEmpty() || timeout < 0) { // If there are results or it's a non-blocking call
            return formatXReadResponse(results);
        }
        
        long deadline = (timeout > 0) ? System.currentTimeMillis() + timeout : 0;
        
        synchronized (notifier) {
            while (true) {
                // Check for results again after waking up or before the first wait
                results = readStreamsNonBlocking(streamKeys, idsForRead);
                if (!results.isEmpty()) {
                    return formatXReadResponse(results);
                }
                
                long currentTime = System.currentTimeMillis();
                long remainingTime = 0;
                
                if (timeout > 0) {
                    remainingTime = deadline - currentTime;
                    if (remainingTime <= 0) {
                        break; // Timeout expired
                    }
                }
                
                try {
                    if (timeout == 0) {
                        notifier.wait(); // Wait indefinitely
                    } else {
                        notifier.wait(remainingTime);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return RespProtocol.createErrorResponse("ERR command interrupted");
                }
                
                // After waiting, check again if timeout expired (in case of spurious wakeup)
                if (timeout > 0 && System.currentTimeMillis() >= deadline) {
                    break;
                }
            }
        }
        
        // Timeout expired and no data found.
        return RespProtocol.createNullBulkString();
    }
    
    private List<Object> readStreamsNonBlocking(List<String> streamKeys, List<String> lastIdStrs) {
        List<Object> allStreamResults = new ArrayList<>();
        
        for (int i = 0; i < streamKeys.size(); i++) {
            String streamKey = streamKeys.get(i);
            String lastIdStr = lastIdStrs.get(i);
            
            List<StreamEntry> streamEntries = streams.get(streamKey);
            if (streamEntries == null || streamEntries.isEmpty()) {
                continue;
            }
            
            StreamId lastStreamId;
            if ("$".equals(lastIdStr)) {
                if (streamEntries.isEmpty()) {
                    continue;
                }
                lastStreamId = streamEntries.get(streamEntries.size() - 1).getId();
            } else {
                lastStreamId = StreamId.fromString(lastIdStr);
            }
            
            List<StreamEntry> result = streamEntries.stream()
                .filter(entry -> entry.getId().compareTo(lastStreamId) > 0)
                .collect(Collectors.toList());
            
            if (!result.isEmpty()) {
                List<Object> singleStreamResult = new ArrayList<>();
                singleStreamResult.add(streamKey);
                singleStreamResult.add(result.stream()
                    .map(entry -> {
                        List<Object> entryList = new ArrayList<>();
                        entryList.add(entry.getId().toString());
                        entryList.add(entry.getFieldValues());
                        return entryList;
                    })
                    .collect(Collectors.toList()));
                allStreamResults.add(singleStreamResult);
            }
        }
        return allStreamResults;
    }
    
    private String formatXReadResponse(List<Object> allStreamResults) {
        if (allStreamResults.isEmpty()) {
            return RespProtocol.createNullBulkString();
        }
        return RespProtocol.createArrayOfArrays(allStreamResults);
    }
    
    private String formatXRangeResponse(List<StreamEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return RespProtocol.createEmptyArray();
        }
        
        List<Object> responseList = new ArrayList<>();
        for (StreamEntry entry : entries) {
            List<Object> entryList = new ArrayList<>();
            entryList.add(entry.getId().toString());
            entryList.add(new ArrayList<>(entry.getFieldValues()));
            responseList.add(entryList);
        }
        
        return RespProtocol.createArrayOfArrays(responseList);
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
            long timePart = Long.parseLong(entryIdStr.substring(0, entryIdStr.length() - 2));
            if (timePart == 0) {
                 List<StreamEntry> streamEntries = streams.get(streamKey);
                if (streamEntries != null && !streamEntries.isEmpty()) {
                    StreamId lastId = streamEntries.get(streamEntries.size() - 1).getId();
                    if (lastId.time() == 0) {
                        return new StreamId(0, lastId.sequence() + 1);
                    }
                }
                return new StreamId(0, 1);
            }
            
            List<StreamEntry> streamEntries = streams.get(streamKey);
            long sequencePart = 0;
            if (streamEntries != null && !streamEntries.isEmpty()) {
                StreamId lastId = streamEntries.get(streamEntries.size() - 1).getId();
                if (lastId.time() == timePart) {
                    sequencePart = lastId.sequence() + 1;
                }
            }
            return new StreamId(timePart, sequencePart);
        }
        
        StreamId currentId = StreamId.fromString(entryIdStr);
        List<StreamEntry> streamEntries = streams.get(streamKey);
        
        if (currentId.compareTo(new StreamId(0, 0)) <= 0) {
            throw new IllegalArgumentException("The ID specified in XADD must be greater than 0-0");
        }
        
        if (streamEntries != null && !streamEntries.isEmpty()) {
            StreamId lastId = streamEntries.get(streamEntries.size() - 1).getId();
            if (currentId.compareTo(lastId) <= 0) {
                throw new IllegalArgumentException("The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }
        
        return currentId;
    }
}