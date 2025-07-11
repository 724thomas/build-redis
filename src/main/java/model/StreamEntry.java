package model;

import java.util.List;

public class StreamEntry {
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