import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Simulator {
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(1234);
        while(true){
            Socket sock = serverSocket.accept();
        }
    }
}
