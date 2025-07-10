package service;

import config.ServerConfig;

/**
 * 애플리케이션의 메인 진입점
 */
public class Main {
    public static void main(String[] args) {
        System.out.println("Starting Redis server from main...");

        // 서버 설정 초기화
        ServerConfig config = new ServerConfig(args);

        // Redis 서버 인스턴스 생성 및 시작
        RedisServer server = new RedisServer(config);
        server.start();
    }
} 