import config.ServerConfig;
import server.RedisServer;

/**
 * Redis 서버 애플리케이션의 진입점
 */
public class Main {
    
    public static void main(String[] args) {
        // 서버 설정 생성 및 명령행 인수 파싱
        ServerConfig config = new ServerConfig();
        config.parseCommandLineArgs(args);
        
        // Redis 서버 생성 및 시작
        RedisServer server = new RedisServer(config);
        server.start();
    }
}
