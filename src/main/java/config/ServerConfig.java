package config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

/**
 * Redis 서버 설정을 관리하는 클래스
 * application.yml 파일에서 설정을 읽어옵니다
 */
public class ServerConfig {
    private int port = 6379;
    private String rdbDir = "/tmp/redis-files";
    private String rdbFilename = "dump.rdb";
    
    public ServerConfig() {
        loadFromYaml();
    }
    
    /**
     * application.yml 파일에서 설정을 로드합니다
     */
    private void loadFromYaml() {
        try {
            Yaml yaml = new Yaml();
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("application.yml");
            
            if (inputStream != null) {
                Map<String, Object> data = yaml.load(inputStream);
                
                // redis.server 설정 읽기
                if (data.containsKey("redis")) {
                    Map<String, Object> redis = (Map<String, Object>) data.get("redis");
                    if (redis.containsKey("server")) {
                        Map<String, Object> server = (Map<String, Object>) redis.get("server");
                        
                        // 포트 설정
                        if (server.containsKey("port")) {
                            this.port = (Integer) server.get("port");
                        }
                        
                        // RDB 설정
                        if (server.containsKey("rdb")) {
                            Map<String, Object> rdb = (Map<String, Object>) server.get("rdb");
                            if (rdb.containsKey("directory")) {
                                this.rdbDir = (String) rdb.get("directory");
                            }
                            if (rdb.containsKey("filename")) {
                                this.rdbFilename = (String) rdb.get("filename");
                            }
                        }
                    }
                }
                
                System.out.println("application.yml 설정을 성공적으로 로드했습니다:");
                System.out.println("  포트: " + this.port);
                System.out.println("  RDB 디렉토리: " + this.rdbDir);
                System.out.println("  RDB 파일명: " + this.rdbFilename);
                
                inputStream.close();
            } else {
                System.out.println("application.yml 파일을 찾을 수 없습니다. 기본값을 사용합니다.");
            }
        } catch (Exception e) {
            System.err.println("application.yml 파일 로드 중 오류 발생: " + e.getMessage());
            System.out.println("기본값을 사용합니다.");
        }
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
     * 명령행 인수는 application.yml 설정을 오버라이드합니다.
     */
    public void parseCommandLineArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--dir":
                    if (i + 1 < args.length) {
                        this.rdbDir = args[++i];
                        System.out.println("명령행에서 RDB 디렉토리 오버라이드: " + this.rdbDir);
                    }
                    break;
                case "--dbfilename":
                    if (i + 1 < args.length) {
                        this.rdbFilename = args[++i];
                        System.out.println("명령행에서 RDB 파일명 오버라이드: " + this.rdbFilename);
                    }
                    break;
                case "--port":
                    if (i + 1 < args.length) {
                        try {
                            this.port = Integer.parseInt(args[++i]);
                            System.out.println("명령행에서 포트 오버라이드: " + this.port);
                        } catch (NumberFormatException e) {
                            System.err.println("잘못된 포트 번호: " + args[i]);
                        }
                    }
                    break;
            }
        }
    }
} 