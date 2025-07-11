package service;

import model.StreamEntry;
import model.StreamEntryId;
import util.RespProtocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
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

        StreamEntryId finalEntryId;
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

        StreamEntryId startId = parseRangeId(startStr, true);
        StreamEntryId endId = parseRangeId(endStr, false);

        List<StreamEntry> result = streamEntries.stream()
                .filter(entry -> isInRange(entry.getId(), startId, endId))
                .collect(Collectors.toList());

        return RespProtocol.formatXRangeResponse(result);
    }

    /**
     * 스트림에서 특정 ID 이후의 엔트리들을 읽어옵니다. (XREAD 로직)
     * @return Map<String, List<StreamEntry>> 형태의 결과. 키는 스트림 키, 값은 엔트리 리스트.
     */
    public Map<String, List<StreamEntry>> xread(List<String> keys, List<String> ids) {
        Map<String, List<StreamEntry>> resultMap = new HashMap<>();

        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            String id = ids.get(i);

            List<StreamEntry> streamEntries = streams.get(key);
            if (streamEntries == null || streamEntries.isEmpty()) {
                continue;
            }

            StreamEntryId startId = StreamEntryId.fromString(id);

            List<StreamEntry> resultEntries = streamEntries.stream()
                    .filter(entry -> entry.getId().compareTo(startId) > 0)
                    .collect(Collectors.toList());

            if (!resultEntries.isEmpty()) {
                resultMap.put(key, resultEntries);
            }
        }

        return resultMap;
    }

    private boolean isInRange(StreamEntryId id, StreamEntryId start, StreamEntryId end) {
        return id.compareTo(start) >= 0 && id.compareTo(end) <= 0;
    }

    private StreamEntryId parseRangeId(String idStr, boolean isStart) {
        if ("-".equals(idStr)) {
            return new StreamEntryId(0, 0); // Represents the minimum possible ID
        }
        if ("+".equals(idStr)) {
            return new StreamEntryId(Long.MAX_VALUE, Long.MAX_VALUE); // Represents the maximum possible ID
        }
        if (!idStr.contains("-")) {
            long time = Long.parseLong(idStr);
            return isStart ? new StreamEntryId(time, 0) : new StreamEntryId(time, Long.MAX_VALUE);
        }
        return StreamEntryId.fromString(idStr);
    }

    /**
     * 스트림에서 특정 ID 이후의 엔트리들을 읽어옵니다.
     */
    public String readStream(String streamKey, String lastIdStr) {
        List<StreamEntry> streamEntries = streams.get(streamKey);
        if (streamEntries == null || streamEntries.isEmpty()) {
            return RespProtocol.createEmptyArray();
        }

        StreamEntryId lastStreamId;
        if ("$".equals(lastIdStr)) {
            if (streamEntries.isEmpty()) {
                return RespProtocol.createEmptyArray();
            }
            lastStreamId = streamEntries.get(streamEntries.size() - 1).getId();
        } else {
            lastStreamId = StreamEntryId.fromString(lastIdStr);
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

    private StreamEntryId processEntryId(String streamKey, String entryIdStr) {
        if ("*".equals(entryIdStr)) {
            long currentTime = System.currentTimeMillis();
            List<StreamEntry> stream = streams.get(streamKey);
            if (stream != null && !stream.isEmpty()) {
                StreamEntryId lastId = stream.get(stream.size() - 1).getId();
                if (lastId.getTime() == currentTime) {
                    return new StreamEntryId(currentTime, lastId.getSequence() + 1);
                }
            }
            return new StreamEntryId(currentTime, 0);
        }

        if (entryIdStr.endsWith("-*")) {
            long time = Long.parseLong(entryIdStr.substring(0, entryIdStr.length() - 2));
            List<StreamEntry> stream = streams.get(streamKey);
            long sequence = 0;
            if (stream != null && !stream.isEmpty()) {
                StreamEntryId lastId = stream.get(stream.size() - 1).getId();
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

        List<StreamEntry> stream = streams.get(streamKey);
        if (stream != null && !stream.isEmpty()) {
            StreamEntryId lastId = stream.get(stream.size() - 1).getId();
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