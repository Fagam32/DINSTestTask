package helpers;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;

public class TestClient {
    private Socket socket;
    private OutputStream out;

    public void startConnection() {
        try {
            socket = new Socket("localhost", 4445);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Message size including all headers on Linux is 168 bytes (Idk why)
    public void sendMessage() {
        byte[] message = new byte[100];
        Arrays.fill(message, (byte) 3);

        try {
            out = socket.getOutputStream();
            out.write(message);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        try {
            if (out != null)
                out.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
