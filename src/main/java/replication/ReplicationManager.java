package replication;

import command.CommandProcessor;
import config.ServerConfig;
import protocol.RespProtocol;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Master-Replica 복제 관련 로직을 관리하는 클래스
 */
public class ReplicationManager {
    
    private final ServerConfig config;
    private final List<ReplicaInfo> replicas = new CopyOnWriteArrayList<>();
    private final AtomicLong masterReplOffset = new AtomicLong(0);
    private final Object waitLock = new Object();
    
    public ReplicationManager(ServerConfig config) {
        this.config = config;
    }
    
    /**
     * 새로운 레플리카를 등록합니다.
     */
    public void addReplica(Socket clientSocket) {
        try {
            replicas.add(new ReplicaInfo(clientSocket));
            System.out.println("New replica registered. Total replicas: " + replicas.size());
        } catch (IOException e) {
            System.err.println("Failed to register new replica: " + e.getMessage());
        }
    }
    
    /**
     * 연결된 모든 레플리카에게 명령어를 전파합니다.
     */
    public void propagateCommand(List<String> commandParts) {
        if (replicas.isEmpty()) {
            return;
        }
        
        String respCommand = RespProtocol.createRespArray(commandParts.toArray(new String[0]));
        byte[] commandBytes = respCommand.getBytes();
        
        System.out.println("Propagating to " + replicas.size() + " replicas: " + respCommand.trim());
        
        for (ReplicaInfo replica : replicas) {
            try {
                replica.getOutputStream().write(commandBytes);
                replica.getOutputStream().flush();
            } catch (IOException e) {
                System.err.println("Failed to propagate command to replica " + replica.getAddress() + ": " + e.getMessage());
                // 연결이 끊긴 레플리카 제거
                replicas.remove(replica);
            }
        }
        
        // 마스터 복제 오프셋 증가
        masterReplOffset.addAndGet(commandBytes.length);
    }
    
    /**
     * WAIT 명령어를 처리합니다.
     * 지정된 수의 레플리카가 현재 오프셋까지 동기화될 때까지 대기합니다.
     */
    public int waitForReplicas(int numReplicas, long timeout) {
        long targetOffset = masterReplOffset.get();
        
        if (targetOffset == 0 || numReplicas == 0) {
            return replicas.size();
        }
        
        // 즉시 조건을 만족하는 레플리카 수 확인
        int syncedReplicas = countSyncedReplicas(targetOffset);
        if (syncedReplicas >= numReplicas) {
            return syncedReplicas;
        }
        
        long startTime = System.currentTimeMillis();
        long remainingTimeout = timeout;
        
        synchronized (waitLock) {
            while (remainingTimeout > 0) {
                try {
                    // ACK 응답을 기다림
                    requestAcksFromReplicas();
                    waitLock.wait(remainingTimeout);
                    
                    syncedReplicas = countSyncedReplicas(targetOffset);
                    if (syncedReplicas >= numReplicas) {
                        break;
                    }
                    
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    remainingTimeout = timeout - elapsedTime;
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("WAIT command interrupted.");
                    break;
                }
            }
        }
        
        return countSyncedReplicas(targetOffset);
    }
    
    /**
     * 모든 레플리카에게 ACK 응답을 요청합니다 (GETACK).
     */
    private void requestAcksFromReplicas() {
        String getAckCommand = RespProtocol.createRespArray(new String[]{"REPLCONF", "GETACK", "*"});
        for (ReplicaInfo replica : replicas) {
            try {
                replica.getOutputStream().write(getAckCommand.getBytes());
                replica.getOutputStream().flush();
            } catch (IOException e) {
                System.err.println("Failed to send GETACK to replica " + replica.getAddress());
                replicas.remove(replica);
            }
        }
    }
    
    /**
     * 레플리카로부터 ACK 응답을 처리합니다.
     */
    public void processAck(Socket replicaSocket, long offset) {
        for (ReplicaInfo replica : replicas) {
            if (replica.getSocket().equals(replicaSocket)) {
                replica.setAckOffset(offset);
                System.out.println("Received ACK from replica " + replica.getAddress() + ": " + offset);
                break;
            }
        }
        
        synchronized (waitLock) {
            waitLock.notifyAll();
        }
    }
    
    private int countSyncedReplicas(long targetOffset) {
        return (int) replicas.stream()
                .filter(r -> r.getAckOffset() >= targetOffset)
                .count();
    }
    
    public long getMasterReplOffset() {
        return masterReplOffset.get();
    }
    
    public int getReplicaCount() {
        return replicas.size();
    }
} 