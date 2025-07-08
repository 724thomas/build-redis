import config.ServerConfig;
import server.RedisServer;

/**
 * Redis 서버 애플리케이션의 진입점
 * application.yml 설정 파일을 사용합니다
 */
public class Main {
    
    public static void main(String[] args) {
        // 서버 설정 생성 (application.yml에서 자동 로드)
        ServerConfig config = new ServerConfig();
        
        // 명령행 인수로 설정 오버라이드
        config.parseCommandLineArgs(args);
        
        // Redis 서버 생성 및 시작
        RedisServer server = new RedisServer(config);
        server.start();
    }
}
