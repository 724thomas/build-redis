package command;

import java.util.List;

/**
 * 모든 Redis 명령어 클래스가 구현해야 하는 공통 인터페이스
 */
public interface Command {
    
    /**
     * 명령어 실행 로직
     * @param args 명령어 인자 리스트
     * @return 클라이언트에게 보낼 RESP 프로토콜 형식의 응답 문자열
     */
    String execute(List<String> args);
} 