
# Redis-like Server in Java

이 프로젝트는 CodeCrafters의 "Build Your Own Redis" 챌린지를 기반으로 Java로 구현된 Redis 서버입니다. 순수 Java 네트워킹과 동시성 모델을 사용하여 Redis의 핵심 기능들을 처음부터 구현하는 데 중점을 두었습니다. RESP(Redis Serialization Protocol) 파싱, 데이터 저장, 만료 처리, RDB 파일 파싱, 리더-팔로워(Leader-Follower) 복제, Redis Streams, 트랜잭션 등 다양한 기능을 지원합니다.

## ✨ 주요 기능

이 Redis 서버는 다음과 같은 광범위한 기능을 구현하고 있습니다.

### 🗄️ **기본 명령어 (General Commands)**
- `PING`: 서버의 가용성을 확인합니다.
- `ECHO`: 주어진 메시지를 그대로 반환합니다.
- `CONFIG GET`: 서버의 설정 파라미터를 가져옵니다.

### 🔑 **키-값 저장소 (Key-Value Store)**
- `SET`: 키에 값을 저장합니다. (`PX` 옵션을 사용한 만료 시간 설정 지원)
- `GET`: 키에 해당하는 값을 가져옵니다.
- `INCR`: 키의 숫자 값을 1 증가시킵니다. (키 부재, 타입 오류 처리 포함)
- `KEYS`: 패턴(`*`만 지원)과 일치하는 모든 키를 반환합니다.
- `TYPE`: 키에 저장된 값의 데이터 타입을 반환합니다. (`string`, `stream`, `none`)

### 🌊 **스트림 (Streams)**
- `XADD`: 스트림에 새로운 엔트리를 추가합니다. (ID 유효성 검사 및 자동 생성 지원: `*`, `<ms>-*`)
- `XRANGE`: 스트림의 특정 범위에 있는 엔트리를 조회합니다. (특수 ID `-`, `+` 지원)
- `XREAD`: 하나 또는 여러 스트림에서 새로운 엔트리를 읽습니다. (`BLOCK` 옵션을 통한 블로킹 읽기, 특수 ID `$` 지원)

### 🔄 **복제 (Replication)**
- **리더-팔로워 구조**: 서버를 리더(Master) 또는 팔로워(Replica) 모드로 실행할 수 있습니다.
- **핸드셰이크**: 팔로워가 리더에 연결 시 `PING`, `REPLCONF`, `PSYNC`를 통한 핸드셰이크 과정을 수행합니다.
- **데이터 동기화**: `PSYNC` 후 빈 RDB 파일을 전송하여 초기 상태를 동기화합니다.
- **명령어 전파**: 리더에서 실행된 쓰기 명령어가 모든 팔로워에게 실시간으로 전파됩니다.
- `INFO replication`: 서버의 복제 관련 정보(역할, 복제 ID, 오프셋)를 반환합니다.
- `WAIT`: 지정된 수의 팔로워가 특정 오프셋까지 동기화될 때까지 대기하여 쓰기 작업의 일관성을 보장합니다.

### 🔀 **트랜잭션 (Transactions)**
- `MULTI`: 트랜잭션 블록을 시작합니다.
- `EXEC`: 큐에 쌓인 모든 명령어를 원자적으로 실행하고 결과를 반환합니다.
- `DISCARD`: 트랜잭션을 중단하고 큐를 비웁니다.
- **명령어 큐잉**: `MULTI`와 `EXEC` 사이의 모든 명령어는 큐에 저장되며, `QUEUED` 응답을 받습니다.

### 💾 **영속성 (Persistence)**
- **RDB 파일 로딩**: 서버 시작 시 `--dir`와 `--dbfilename` 인자로 지정된 RDB 파일을 읽어 메모리에 로드합니다. (키-값, 만료 시간 포함)

---

## 🚀 빌드 및 실행 방법

### **전제 조건**
- Java 17 이상
- Apache Maven

### **빌드**

프로젝트 루트 디렉토리에서 다음 명령어를 실행하여 실행 가능한 JAR 파일을 빌드합니다.

```bash
mvn clean package
```

빌드가 성공하면 `target/` 디렉토리에 `codecrafters-redis-jar-with-dependencies.jar` 파일이 생성됩니다.

### **실행**

#### **리더(Master) 모드**

기본 포트(6379)로 서버를 시작합니다.
```bash
java -jar target/codecrafters-redis-jar-with-dependencies.jar
```

다른 포트를 사용하려면 `--port` 인자를 사용하세요.
```bash
java -jar target/codecrafters-redis-jar-with-dependencies.jar --port 6380
```

#### **팔로워(Replica) 모드**

리더 서버에 연결하는 팔로워로 서버를 시작합니다.
```bash
# 리더가 localhost:6379에서 실행 중이라고 가정
java -jar target/codecrafters-redis-jar-with-dependencies.jar --port 6381 --replicaof localhost 6379
```

#### **RDB 파일 로딩**

서버 시작 시 RDB 파일을 로드하려면 `--dir`와 `--dbfilename` 인자를 사용하세요.
```bash
java -jar target/codecrafters-redis-jar-with-dependencies.jar --dir /path/to/rdb/dir --dbfilename dump.rdb
```

---

## 📋 구현된 명령어 목록

| Category      | Command        | Description                                       |
|---------------|----------------|---------------------------------------------------|
| **Connection**| `PING`         | 서버의 가용성을 확인합니다.                       |
|               | `ECHO`         | 주어진 메시지를 반환합니다.                       |
| **Server**    | `CONFIG`       | 서버 설정을 조회합니다.                           |
|               | `INFO`         | 서버 정보를 (주로 복제 관련) 반환합니다.          |
| **Keys**      | `GET`          | 키의 값을 가져옵니다.                             |
|               | `SET`          | 키에 값을 저장합니다. (만료 시간 `PX` 지원)       |
|               | `INCR`         | 키의 숫자 값을 1 증가시킵니다.                    |
|               | `KEYS`         | 패턴과 일치하는 키를 찾습니다. (`*` 만 지원)      |
|               | `TYPE`         | 키의 데이터 타입을 반환합니다.                    |
| **Replication** | `PSYNC`      | 팔로워가 리더와 동기화를 시작합니다.              |
|               | `REPLCONF`     | 복제 관련 설정을 구성합니다. (`GETACK` 등)        |
|               | `WAIT`         | 쓰기 명령이 일정 수의 팔로워에게 전파될 때까지 대기합니다.|
| **Transactions**| `MULTI`      | 트랜잭션을 시작합니다.                            |
|               | `EXEC`         | 트랜잭션 큐의 모든 명령을 실행합니다.             |
|               | `DISCARD`      | 트랜잭션을 중단합니다.                            |
| **Streams**   | `XADD`         | 스트림에 새 엔트리를 추가합니다.                  |
|               | `XRANGE`       | 스트림의 특정 범위 엔트리를 조회합니다.           |
|               | `XREAD`        | 하나 이상의 스트림에서 새 엔트리를 읽습니다. (`BLOCK` 지원)|
