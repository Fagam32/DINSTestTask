package helpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class TestServer {
    private ServerSocket server;
    private Socket socket;
    private BufferedReader in;
    boolean isStopped = false;
    @SuppressWarnings("StatementWithEmptyBody")
    public void start() {
        try {
            server = new ServerSocket(4445);
            do {
                socket = server.accept();
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                while (!socket.isClosed()) {
                }
            } while (!isStopped);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        try {
            isStopped = true;
            Thread.sleep(10);
            in.close();
            socket.close();
            server.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}