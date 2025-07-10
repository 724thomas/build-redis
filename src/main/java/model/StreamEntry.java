package model;

import java.util.List;

/**
 * Redis Streams 엔트리를 저장하는 클래스
 */
public class StreamEntry {
    private final StreamEntryId id;
    private final List<String> fieldValues;

    public StreamEntry(StreamEntryId id, List<String> fieldValues) {
        this.id = id;
        this.fieldValues = fieldValues;
    }

    public StreamEntryId getId() {
        return id;
    }

    public List<String> getFieldValues() {
        return fieldValues;
    }
} 