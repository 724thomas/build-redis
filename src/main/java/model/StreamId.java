package model;

public record StreamId(long time, long sequence) implements Comparable<model.StreamId> {
    public static model.StreamId fromString(String idStr) {
        if (idStr == null) {
            throw new IllegalArgumentException("Invalid stream ID format: null");
        }
        if ("-".equals(idStr)) {
            return new model.StreamId(0, 0);
        }
        if ("+".equals(idStr)) {
            return new model.StreamId(Long.MAX_VALUE, Long.MAX_VALUE);
        }
        
        String[] parts = idStr.split("-");
        if (parts.length == 1) {
            return new model.StreamId(Long.parseLong(parts[0]), 0);
        }
        if (parts.length == 2) {
            return new model.StreamId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        }
        throw new IllegalArgumentException("Invalid stream ID format: " + idStr);
    }
    
    @Override
    public String toString() {
        return time + "-" + sequence;
    }
    
    @Override
    public int compareTo(model.StreamId other) {
        if (this.time != other.time) {
            return Long.compare(this.time, other.time);
        }
        return Long.compare(this.sequence, other.sequence);
    }
} 