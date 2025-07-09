package config;

/**
 * Redis 서버 설정을 관리하는 클래스
 */
public class ServerConfig {
    private int port = 6379;
    private String rdbDir = "/tmp/redis-files";
    private String rdbFilename = "dump.rdb";
    
    private boolean isReplica = false;  // 이 서버가 replica인지 여부
    private String masterHost;          // 마스터 서버의 호스트
    private int masterPort;             // 마스터 서버의 포트
    
    public ServerConfig() {
        System.out.println("서버 설정 초기화:");
        System.out.println("  포트: " + this.port);
        System.out.println("  RDB 디렉토리: " + this.rdbDir);
        System.out.println("  RDB 파일명: " + this.rdbFilename);
    }
    
    // Getter 메서드들
    public int getPort() {
        return port;
    }
    
    public String getRdbDir() {
        return rdbDir;
    }
    
    public String getRdbFilename() {
        return rdbFilename;
    }
    
    public boolean isReplica() {
        return isReplica;
    }
    
    public String getMasterHost() {
        return masterHost;
    }
    
    public int getMasterPort() {
        return masterPort;
    }
    
    // Setter 메서드들
    public void setPort(int port) {
        this.port = port;
    }
    
    public void setRdbDir(String rdbDir) {
        this.rdbDir = rdbDir;
    }
    
    public void setRdbFilename(String rdbFilename) {
        this.rdbFilename = rdbFilename;
    }
    
    public void setReplica(boolean replica) {
        isReplica = replica;
    }
    
    public void setMasterHost(String masterHost) {
        this.masterHost = masterHost;
    }
    
    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }
    
    /**
     * 명령행 인수를 파싱하여 설정을 업데이트합니다.
     */
    public void parseCommandLineArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--dir":
                    if (i + 1 < args.length) {
                        this.rdbDir = args[++i];
                        System.out.println("명령행에서 RDB 디렉토리 설정: " + this.rdbDir);
                    }
                    break;
                case "--dbfilename":
                    if (i + 1 < args.length) {
                        this.rdbFilename = args[++i];
                        System.out.println("명령행에서 RDB 파일명 설정: " + this.rdbFilename);
                    }
                    break;
                case "--port":
                    if (i + 1 < args.length) {
                        try {
                            this.port = Integer.parseInt(args[++i]);
                            System.out.println("명령행에서 포트 설정: " + this.port);
                        } catch (NumberFormatException e) {
                            System.err.println("잘못된 포트 번호: " + args[i]);
                        }
                    }
                    break;
                
                case "--replicaof":
                    if (i + 1 < args.length) {
                        String replicaofValue = args[++i];
                        String[] parts = replicaofValue.split(" ");
                        if (parts.length == 2) {
                            this.isReplica = true;
                            this.masterHost = parts[0];
                            try {
                                this.masterPort = Integer.parseInt(parts[1]);
                                System.out.println("Replica 모드 설정: " + this.masterHost + ":" + this.masterPort);
                            } catch (NumberFormatException e) {
                                System.err.println("잘못된 마스터 포트 번호: " + parts[1]);
                            }
                        } else {
                            System.err.println("잘못된 --replicaof 형식: " + replicaofValue + " (예: --replicaof \"localhost 6379\")");
                        }
                    }
                    break;
            }
        }
    }
} 