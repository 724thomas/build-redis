# Redis Server Implementation in Java

Java로 구현한 완전한 Redis 서버입니다. TCP 연결, 기본 Redis 명령어, RDB 파일 처리, 복제(Replication), Redis Streams, 트랜잭션 등 Redis의 핵심 기능을 모두 지원합니다.

## 🎯 프로젝트 개요

이 프로젝트는 Redis의 주요 기능들을 단계별로 구현하여 분산 시스템과 데이터베이스의 내부 동작을 깊이 이해하는 것을 목표로 합니다. 실제 Redis 서버의 동작 방식을 모방하여 학습 목적으로 제작되었습니다.

## 🚀 지원 기능

### 🔧 기본 서버 기능
- **TCP 서버**: 기본 포트 6379, 사용자 정의 포트 지원
- **동시 클라이언트 처리**: 멀티스레드 기반 동시 연결 처리
- **RESP 프로토콜**: Redis Serialization Protocol 완전 지원
- **설정 관리**: 런타임 설정 조회 및 관리

### 📋 Redis 명령어 지원

#### 기본 명령어
- **PING**: 서버 상태 확인
- **ECHO**: 문자열 에코
- **INFO**: 서버 정보 조회 (복제 정보 포함)
- **CONFIG GET**: 설정 값 조회

#### 데이터 조작 명령어
- **SET**: 키-값 저장 (만료 시간 PX 옵션 지원)
- **GET**: 키 값 조회 (만료된 키 자동 삭제)
- **INCR**: 숫자 값 증가 (존재하지 않는 키, 비숫자 값 처리)
- **KEYS**: 패턴 매칭 키 목록 조회
- **TYPE**: 키의 데이터 타입 확인

#### Redis Streams
- **XADD**: 스트림 엔트리 추가
  - 명시적 ID 지정 (`1234567890123-0`)
  - 자동 ID 생성 (`*`)
  - 부분 자동 생성 (`timestamp-*`)
  - ID 검증 및 순서 보장
- **XRANGE**: 범위 기반 엔트리 조회
  - 시작/끝 ID 지정
  - 특수 값 지원 (`-`, `+`)
- **XREAD**: 스트림 데이터 읽기
  - 단일/다중 스트림 지원
  - 블로킹 읽기 (BLOCK 옵션)
  - 타임아웃 지원 (무한 대기 포함)
  - 새 메시지만 읽기 (`$` 지원)

#### 트랜잭션
- **MULTI**: 트랜잭션 시작
- **EXEC**: 트랜잭션 실행
- **DISCARD**: 트랜잭션 취소
- **명령어 큐잉**: 트랜잭션 내 명령어 대기열 관리
- **오류 처리**: 트랜잭션 내 개별 명령어 실패 처리
- **다중 트랜잭션**: 여러 클라이언트의 동시 트랜잭션 지원

### 🔄 복제 (Replication)
- **마스터-레플리카 구조**: 완전한 리더-팔로워 복제
- **핸드셰이크 프로토콜**: 
  - PING → REPLCONF (listening-port, capa) → PSYNC
- **전체 동기화**: RDB 파일 전송을 통한 초기 동기화
- **명령어 전파**: 쓰기 명령어 실시간 전파
- **ACK 시스템**: 레플리카 동기화 상태 추적
- **WAIT 명령어**: 지정된 수의 레플리카 동기화 대기

### 💾 영속성 (Persistence)
- **RDB 파일 처리**:
  - 서버 시작 시 데이터 로딩
  - 다양한 인코딩 방식 지원 (문자열, 정수)
  - 만료 시간 처리 (초/밀리초 단위)
  - 메타데이터 파싱
  - 빈 RDB 파일 전송 (복제용)

### ⏱️ 만료 처리
- **키 만료**: TTL 기반 자동 키 삭제
- **지연 만료**: 접근 시점 만료 확인
- **만료 시간 형식**: 밀리초 단위 타임스탬프

## 🛠️ 사용법

### 서버 실행

```bash
# 기본 설정으로 실행 (포트 6379)
./your_program.sh

# 마스터 서버 (사용자 정의 포트)
./your_program.sh --port 6380

# 레플리카 서버
./your_program.sh --port 6381 --replicaof "localhost 6380"

# RDB 파일 설정
./your_program.sh --dir /data --dbfilename redis.rdb
```

### 클라이언트 연결

```bash
# 기본 연결
redis-cli

# 특정 포트 연결
redis-cli -p 6380
```

### 명령어 예제

#### 기본 명령어
```bash
redis-cli PING                    # PONG
redis-cli ECHO "Hello World"      # "Hello World"
redis-cli INFO replication        # 복제 정보
```

#### 데이터 조작
```bash
# 키-값 저장/조회
redis-cli SET mykey "Hello"
redis-cli GET mykey
redis-cli SET tempkey "value" PX 5000  # 5초 후 만료
redis-cli INCR counter             # 숫자 증가

# 키 관리
redis-cli KEYS "*"                 # 모든 키 조회
redis-cli TYPE mykey               # 키 타입 확인
```

#### Redis Streams
```bash
# 스트림 엔트리 추가
redis-cli XADD sensor:temp "*" temperature 25.6 humidity 60
redis-cli XADD sensor:temp "1234567890123-0" temp 26.1

# 스트림 조회
redis-cli XRANGE sensor:temp - +           # 모든 엔트리
redis-cli XRANGE sensor:temp 1234567890123 +  # 특정 시점 이후

# 스트림 읽기
redis-cli XREAD streams sensor:temp 0-0    # 모든 엔트리 읽기
redis-cli XREAD BLOCK 1000 streams sensor:temp $ # 새 메시지 대기
```

#### 트랜잭션
```bash
redis-cli MULTI                   # 트랜잭션 시작
redis-cli SET account1 100        # QUEUED
redis-cli SET account2 200        # QUEUED
redis-cli INCR account1           # QUEUED
redis-cli EXEC                    # 트랜잭션 실행
# 1) OK
# 2) OK  
# 3) (integer) 101
```

#### 복제
```bash
# 마스터에서
redis-cli SET shared_data "Hello from master"

# 레플리카에서 확인
redis-cli GET shared_data          # "Hello from master"

# 동기화 대기
redis-cli WAIT 2 5000             # 2개 레플리카가 5초 내 동기화 대기
```

## 📁 프로젝트 구조

```
codecrafters-redis-java/
├── src/main/java/
│   └── Main.java                 # 메인 서버 구현
├── pom.xml                       # Maven 설정
├── your_program.sh              # 실행 스크립트
├── codecrafters.yml             # CodeCrafters 설정
└── README.md                    # 프로젝트 문서
```

## 🔧 기술 스택

- **Java 8+**: 서버 구현 언어
- **Maven**: 빌드 및 의존성 관리
- **TCP Sockets**: 네트워크 통신
- **NIO**: 논블로킹 I/O (블로킹 명령어용)
- **Concurrent Collections**: 스레드 안전한 데이터 저장
- **RESP Protocol**: Redis 직렬화 프로토콜

## 📋 지원 데이터 타입

### ✅ 구현된 타입
- **String**: 문자열 및 숫자 값
- **Stream**: 로그 형태의 데이터 스트림

### 🔄 각 타입별 명령어
- **String**: SET, GET, INCR, TYPE
- **Stream**: XADD, XRANGE, XREAD, TYPE

## 🌐 네트워크 & 프로토콜

### RESP (Redis Serialization Protocol)
- **Simple Strings**: `+OK\r\n`
- **Errors**: `-ERR message\r\n`
- **Integers**: `:123\r\n`
- **Bulk Strings**: `$5\r\nhello\r\n`
- **Arrays**: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`
- **Null**: `$-1\r\n`

### 복제 프로토콜
- **핸드셰이크**: PING → REPLCONF → PSYNC
- **전체 동기화**: FULLRESYNC + RDB 전송
- **명령어 전파**: 실시간 명령어 스트리밍
- **ACK**: REPLCONF GETACK으로 동기화 확인

## 🏗️ 빌드 및 실행

```bash
# 프로젝트 빌드
mvn compile

# 테스트 실행
mvn test

# 서버 시작
./your_program.sh

# 개발 모드 (디버그 정보 포함)
./your_program.sh --debug
```

## 🧪 테스트 및 검증

### 단일 서버 테스트
```bash
# 기본 기능 테스트
redis-cli PING
redis-cli SET test "value"
redis-cli GET test

# 스트림 테스트
redis-cli XADD mystream "*" field1 value1
redis-cli XREAD streams mystream 0-0
```

### 복제 테스트
```bash
# 터미널 1: 마스터 시작
./your_program.sh --port 6379

# 터미널 2: 레플리카 시작  
./your_program.sh --port 6380 --replicaof "localhost 6379"

# 터미널 3: 복제 확인
redis-cli -p 6379 SET replicated_key "master_value"
redis-cli -p 6380 GET replicated_key  # "master_value"
```

## 🎓 학습 목표

이 프로젝트를 통해 다음을 학습할 수 있습니다:

1. **네트워크 프로그래밍**: TCP 소켓, 프로토콜 설계
2. **동시성**: 멀티스레딩, 스레드 안전성
3. **데이터 구조**: 해시맵, 리스트, 스트림
4. **직렬화**: 바이너리 프로토콜, 인코딩/디코딩
5. **분산 시스템**: 복제, 일관성, 동기화
6. **트랜잭션**: ACID 속성, 롤백 처리
7. **영속성**: 파일 I/O, 데이터 복구
