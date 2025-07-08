package config;

/**
 * Redis 서버 설정을 관리하는 클래스
 */
public class ServerConfig {
    private int port = 6379;
    private String rdbDir = "/tmp/redis-files";
    private String rdbFilename = "dump.rdb";
    
    public ServerConfig() {}
    
    public ServerConfig(int port, String rdbDir, String rdbFilename) {
        this.port = port;
        this.rdbDir = rdbDir;
        this.rdbFilename = rdbFilename;
    }
    
    public int getPort() {
        return port;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public String getRdbDir() {
        return rdbDir;
    }
    
    public void setRdbDir(String rdbDir) {
        this.rdbDir = rdbDir;
    }
    
    public String getRdbFilename() {
        return rdbFilename;
    }
    
    public void setRdbFilename(String rdbFilename) {
        this.rdbFilename = rdbFilename;
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
                    }
                    break;
                case "--dbfilename":
                    if (i + 1 < args.length) {
                        this.rdbFilename = args[++i];
                    }
                    break;
                case "--port":
                    if (i + 1 < args.length) {
                        try {
                            this.port = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid port number: " + args[i]);
                        }
                    }
                    break;
            }
        }
    }
} 