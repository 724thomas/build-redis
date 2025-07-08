import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class Main {
    private static final int DEFAULT_PORT = 6379;
    private static final String PONG_RESPONSE = "+PONG\r\n";
    
    public static void main(String[] args) {
        System.out.println("Starting Redis server on port " + DEFAULT_PORT);
        
        try (ServerSocket serverSocket = createServerSocket(DEFAULT_PORT)) {
            System.out.println("Redis server started. Waiting for connections...");
            
            while (true) {
                try (Socket clientSocket = serverSocket.accept()) {
                    System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());
                    handleClient(clientSocket);
                } catch (IOException e) {
                    System.err.println("Error handling client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to start Redis server: " + e.getMessage());
        }
    }
    
    /**
     * 서버 소켓을 생성하고 설정합니다.
     */
    private static ServerSocket createServerSocket(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        // 서버 재시작 시 'Address already in use' 에러 방지
        serverSocket.setReuseAddress(true);
        return serverSocket;
    }
    
    /**
     * 클라이언트 연결을 처리하고 Redis 명령어에 응답합니다.
     */
    private static void handleClient(Socket clientSocket) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             OutputStream outputStream = clientSocket.getOutputStream()) {
            
            String command;
            while ((command = reader.readLine()) != null) {
                System.out.println("Received: " + command);
                
                // 현재는 모든 명령어에 대해 PONG 응답 (Stage 2 요구사항)
                sendResponse(outputStream, PONG_RESPONSE);
                System.out.println("Sent: PONG");
            }
            
        } catch (SocketException e) {
            System.out.println("Client disconnected: " + clientSocket.getRemoteSocketAddress());
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        }
        
        System.out.println("Client connection closed: " + clientSocket.getRemoteSocketAddress());
    }
    
    /**
     * 클라이언트에게 응답을 전송합니다.
     */
    private static void sendResponse(OutputStream outputStream, String response) throws IOException {
        outputStream.write(response.getBytes());
        outputStream.flush();
    }
}
