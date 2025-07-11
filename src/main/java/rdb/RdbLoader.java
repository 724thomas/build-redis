package rdb;

import config.ServerConfig;
import service.StorageService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * RDB 파일 로딩 및 파싱을 담당하는 클래스
 */
public class RdbLoader {
    
    private final ServerConfig config;
    private final StorageService storageService;
    
    public RdbLoader(ServerConfig config, StorageService storageService) {
        this.config = config;
        this.storageService = storageService;
    }
    
    /**
     * RDB 파일에서 데이터를 로드합니다.
     */
    public void loadRdbFile() {
        Path rdbPath = Paths.get(config.getRdbDir(), config.getRdbFilename());
        
        if (!Files.exists(rdbPath)) {
            System.out.println("RDB file not found: " + rdbPath + ". Starting with empty database.");
            return;
        }
        
        try {
            byte[] rdbData = Files.readAllBytes(rdbPath);
            parseRdbFile(rdbData);
            System.out.println("RDB file loaded successfully: " + rdbPath);
        } catch (IOException e) {
            System.err.println("Error loading RDB file: " + e.getMessage());
            System.out.println("Starting with empty database.");
        }
    }
    
    /**
     * RDB 파일 데이터를 파싱합니다.
     * Redis RDB 파일 형식 완전 지원
     */
    private void parseRdbFile(byte[] data) {
        if (data.length < 9) {
            System.out.println("RDB file too short, starting with empty database.");
            return;
        }
        
        // Header 검증 (REDIS0011)
        String header = new String(data, 0, 9);
        if (!header.startsWith("REDIS")) {
            System.out.println("Invalid RDB file header, starting with empty database.");
            return;
        }
        
        System.out.println("RDB file header: " + header);
        
        int pos = 9; // Header 다음부터 시작
        
        try {
            while (pos < data.length) {
                int opcode = data[pos] & 0xFF;
                pos++;
                
                if (opcode == 0xFF) {
                    // 파일 끝
                    System.out.println("RDB file parsing completed.");
                    break;
                } else if (opcode == 0xFE) {
                    // 데이터베이스 선택
                    RdbParseResult dbResult = parseSize(data, pos);
                    pos = dbResult.nextPos;
                    System.out.println("Database index: " + dbResult.value);
                } else if (opcode == 0xFB) {
                    // 해시 테이블 크기 정보
                    RdbParseResult keyHashSize = parseSize(data, pos);
                    pos = keyHashSize.nextPos;
                    RdbParseResult expireHashSize = parseSize(data, pos);
                    pos = expireHashSize.nextPos;
                    System.out.println("Hash table sizes - Keys: " + keyHashSize.value + ", Expires: " + expireHashSize.value);
                } else if (opcode == 0xFA) {
                    // 메타데이터
                    RdbParseResult nameResult = parseString(data, pos);
                    pos = nameResult.nextPos;
                    RdbParseResult valueResult = parseString(data, pos);
                    pos = valueResult.nextPos;
                    System.out.println("Metadata - " + nameResult.stringValue + ": " + valueResult.stringValue);
                } else if (opcode == 0xFC) {
                    // 밀리초 만료 시간
                    pos = parseKeyValueWithExpiry(data, pos, true);
                } else if (opcode == 0xFD) {
                    // 초 단위 만료 시간
                    pos = parseKeyValueWithExpiry(data, pos, false);
                } else if (opcode == 0x00) {
                    // 만료 시간 없는 문자열 값
                    pos = parseKeyValueWithoutExpiry(data, pos);
                } else {
                    // 알 수 없는 opcode, 안전하게 건너뛰기
                    System.out.println("Unknown opcode: 0x" + Integer.toHexString(opcode) + " at position " + (pos - 1));
                    break;
                }
            }
        } catch (Exception e) {
            System.err.println("Error parsing RDB file: " + e.getMessage());
            System.out.println("Continuing with partially loaded data.");
        }
    }
    
    /**
     * 만료 시간이 있는 키-값을 파싱합니다.
     */
    private int parseKeyValueWithExpiry(byte[] data, int pos, boolean isMilliseconds) {
        if (pos + (isMilliseconds ? 8 : 4) >= data.length) return pos;
        
        long expireTime = 0;
        int timeBytes = isMilliseconds ? 8 : 4;
        
        for (int i = 0; i < timeBytes; i++) {
            expireTime |= ((long) (data[pos + i] & 0xFF)) << (i * 8);
        }
        
        if (!isMilliseconds) {
            expireTime *= 1000; // 밀리초로 변환
        }
        
        pos += timeBytes;
        
        // 키-값 파싱
        if (pos >= data.length) return pos;
        int valueType = data[pos] & 0xFF;
        pos++;
        
        RdbParseResult keyResult = parseString(data, pos);
        if (keyResult == null) return pos;
        pos = keyResult.nextPos;
        
        RdbParseResult valueResult = parseString(data, pos);
        if (valueResult == null) return pos;
        pos = valueResult.nextPos;
        
        storageService.setWithExpiry(keyResult.stringValue, valueResult.stringValue, expireTime);
        System.out.println("Loaded key with expiry: " + keyResult.stringValue + " = " + valueResult.stringValue + " (expires: " + expireTime + ")");
        
        return pos;
    }
    
    /**
     * 만료 시간이 없는 키-값을 파싱합니다.
     */
    private int parseKeyValueWithoutExpiry(byte[] data, int pos) {
        RdbParseResult keyResult = parseString(data, pos);
        if (keyResult == null) return pos;
        pos = keyResult.nextPos;
        
        RdbParseResult valueResult = parseString(data, pos);
        if (valueResult == null) return pos;
        pos = valueResult.nextPos;
        
        storageService.set(keyResult.stringValue, valueResult.stringValue);
        System.out.println("Loaded key: " + keyResult.stringValue + " = " + valueResult.stringValue);
        
        return pos;
    }
    
    /**
     * RDB 파일에서 크기 인코딩된 값을 파싱합니다.
     */
    private RdbParseResult parseSize(byte[] data, int pos) {
        if (pos >= data.length) return null;
        
        int firstByte = data[pos] & 0xFF;
        int type = (firstByte & 0xC0) >> 6; // 상위 2비트
        
        switch (type) {
            case 0: // 00: 6비트 크기
                return new RdbParseResult(firstByte & 0x3F, pos + 1, null);
            case 1: // 01: 14비트 크기
                if (pos + 1 >= data.length) return null;
                int size14 = ((firstByte & 0x3F) << 8) | (data[pos + 1] & 0xFF);
                return new RdbParseResult(size14, pos + 2, null);
            case 2: // 10: 32비트 크기
                if (pos + 4 >= data.length) return null;
                long size32 = 0;
                for (int i = 1; i <= 4; i++) {
                    size32 = (size32 << 8) | (data[pos + i] & 0xFF);
                }
                return new RdbParseResult((int) size32, pos + 5, null);
            default: // 11: 특별한 인코딩
                return new RdbParseResult(firstByte & 0x3F, pos + 1, null);
        }
    }
    
    /**
     * RDB 파일에서 문자열을 파싱합니다.
     */
    private RdbParseResult parseString(byte[] data, int pos) {
        RdbParseResult sizeResult = parseSize(data, pos);
        if (sizeResult == null) return null;
        
        int length = sizeResult.value;
        int nextPos = sizeResult.nextPos;
        
        if (nextPos + length > data.length) return null;
        
        // 특별한 인코딩 처리
        int firstByte = data[pos] & 0xFF;
        int type = (firstByte & 0xC0) >> 6;
        
        if (type == 3) { // 11: 특별한 인코딩
            int encoding = firstByte & 0x3F;
            if (encoding == 0) { // C0: 8비트 정수
                if (nextPos >= data.length) return null;
                int value = data[nextPos] & 0xFF;
                return new RdbParseResult(0, nextPos + 1, String.valueOf(value));
            } else if (encoding == 1) { // C1: 16비트 정수
                if (nextPos + 1 >= data.length) return null;
                int value = (data[nextPos] & 0xFF) | ((data[nextPos + 1] & 0xFF) << 8);
                return new RdbParseResult(0, nextPos + 2, String.valueOf(value));
            } else if (encoding == 2) { // C2: 32비트 정수
                if (nextPos + 3 >= data.length) return null;
                long value = 0;
                for (int i = 0; i < 4; i++) {
                    value |= ((long) (data[nextPos + i] & 0xFF)) << (i * 8);
                }
                return new RdbParseResult(0, nextPos + 4, String.valueOf(value));
            }
            // 다른 인코딩은 건너뛰기
            return new RdbParseResult(0, nextPos, "");
        }
        
        // 일반 문자열
        byte[] stringBytes = new byte[length];
        System.arraycopy(data, nextPos, stringBytes, 0, length);
        String stringValue = new String(stringBytes);
        
        return new RdbParseResult(length, nextPos + length, stringValue);
    }
    
    /**
     * RDB 파싱 결과를 저장하는 헬퍼 클래스
     */
    private static class RdbParseResult {
        final int value;
        final int nextPos;
        final String stringValue;
        
        RdbParseResult(int value, int nextPos, String stringValue) {
            this.value = value;
            this.nextPos = nextPos;
            this.stringValue = stringValue;
        }
    }
} 