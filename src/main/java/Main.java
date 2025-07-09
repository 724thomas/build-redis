import config.ServerConfig;
import server.RedisServer;

/**
 * Redis 서버 애플리케이션의 진입점
 */
public class Main {
    
    public static void main(String[] args) {
        // 서버 설정 생성
        ServerConfig config = new ServerConfig();
        
        // 명령행 인수로 설정 오버라이드
        config.parseCommandLineArgs(args);
        
        
        if (config.isReplica()) {
            System.out.println("Replica 모드로 시작: 마스터 " + config.getMasterHost() + ":" + config.getMasterPort());
        } else {
            System.out.println("Master 모드로 시작");
        }
        
        // Redis 서버 생성 및 시작
        RedisServer server = new RedisServer(config);
        server.start();
    }
}
