package model;

import java.util.Objects;

public class StreamEntryId implements Comparable<StreamEntryId> {
    
    private final long time;
    private final long sequence;
    
    public StreamEntryId(long time, long sequence) {
        this.time = time;
        this.sequence = sequence;
    }
    
    public static StreamEntryId fromString(String idStr) {
        if (idStr == null) {
            throw new IllegalArgumentException("ID string cannot be null");
        }
        
        String[] parts = idStr.split("-");
        if (parts.length < 1 || parts.length > 2) {
            throw new IllegalArgumentException("Invalid ID format: " + idStr);
        }
        
        try {
            long time = Long.parseLong(parts[0]);
            long sequence = (parts.length == 2) ? Long.parseLong(parts[1]) : 0;
            return new StreamEntryId(time, sequence);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number in ID: " + idStr, e);
        }
    }
    
    public long getTime() {
        return time;
    }
    
    public long getSequence() {
        return sequence;
    }
    
    @Override
    public int compareTo(StreamEntryId other) {
        if (this.time != other.time) {
            return Long.compare(this.time, other.time);
        }
        return Long.compare(this.sequence, other.sequence);
    }
    
    @Override
    public String toString() {
        return time + "-" + sequence;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamEntryId that = (StreamEntryId) o;
        return time == that.time && sequence == that.sequence;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(time, sequence);
    }
} 