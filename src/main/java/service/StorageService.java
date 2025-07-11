package service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 키-값 저장소와 만료 시간 관리를 담당하는 서비스 클래스
 */
public class StorageService {
    
    // 키-값 저장소 (스레드 안전)
    private final Map<String, String> keyValueStore = new ConcurrentHashMap<>();
    // 키별 만료 시간 저장소 (밀리초 단위 timestamp)
    private final Map<String, Long> keyExpiryStore = new ConcurrentHashMap<>();
    
    /**
     * 키-값을 저장합니다.
     */
    public void set(String key, String value) {
        keyValueStore.put(key, value);
        keyExpiryStore.remove(key); // 기존 만료 시간 제거
        System.out.println("Stored: " + key + " = " + value);
    }
    
    /**
     * 만료 시간과 함께 키-값을 저장합니다.
     */
    public void setWithExpiry(String key, String value, long expiryTimeMs) {
        keyValueStore.put(key, value);
        keyExpiryStore.put(key, expiryTimeMs);
        System.out.println("Stored with expiry: " + key + " = " + value + " (expires at: " + expiryTimeMs + ")");
    }
    
    /**
     * 키에 해당하는 값을 가져옵니다. 만료된 키는 자동으로 삭제됩니다.
     */
    public String get(String key) {
        // 만료 시간 검사
        if (isKeyExpired(key)) {
            // 만료된 키 삭제
            keyValueStore.remove(key);
            keyExpiryStore.remove(key);
            System.out.println("Key expired and removed: " + key);
            return null;
        }
        
        String value = keyValueStore.get(key);
        System.out.println("Retrieved: " + key + " = " + value);
        return value;
    }
    
    /**
     * 모든 키를 반환합니다. 만료된 키들은 자동으로 정리됩니다.
     */
    public Set<String> getAllKeys() {
        cleanExpiredKeys();
        return keyValueStore.keySet();
    }
    
    /**
     * 키가 존재하는지 확인합니다.
     */
    public boolean exists(String key) {
        if (isKeyExpired(key)) {
            keyValueStore.remove(key);
            keyExpiryStore.remove(key);
            return false;
        }
        return keyValueStore.containsKey(key);
    }
    
    /**
     * 키를 삭제합니다.
     */
    public boolean delete(String key) {
        boolean removed = keyValueStore.remove(key) != null;
        keyExpiryStore.remove(key);
        return removed;
    }
    
    /**
     * 저장소를 초기화합니다.
     */
    public void clear() {
        keyValueStore.clear();
        keyExpiryStore.clear();
    }
    
    /**
     * 키가 만료되었는지 확인합니다.
     */
    private boolean isKeyExpired(String key) {
        Long expiryTime = keyExpiryStore.get(key);
        if (expiryTime == null) {
            return false; // 만료 시간이 설정되지 않음
        }
        return System.currentTimeMillis() > expiryTime;
    }
    
    /**
     * 만료된 키들을 정리합니다.
     */
    public void cleanExpiredKeys() {
        List<String> expiredKeys = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        
        for (Map.Entry<String, Long> entry : keyExpiryStore.entrySet()) {
            if (currentTime > entry.getValue()) {
                expiredKeys.add(entry.getKey());
            }
        }
        
        for (String key : expiredKeys) {
            keyValueStore.remove(key);
            keyExpiryStore.remove(key);
            System.out.println("Expired key removed: " + key);
        }
    }
    
    /**
     * 저장된 키의 개수를 반환합니다.
     */
    public int size() {
        cleanExpiredKeys();
        return keyValueStore.size();
    }
} 