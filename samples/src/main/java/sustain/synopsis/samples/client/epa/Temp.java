package sustain.synopsis.samples.client.epa;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Temp {
    public static void main(String[] args) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
        try {
            dos.writeUTF("9xy");
            dos.writeLong(12344554545454L);
            dos.writeUTF("Carbon monoxide");
            dos.writeFloat(0.5f);
            dos.flush();
            byteArrayOutputStream.flush();
            System.out.println(2616883 * byteArrayOutputStream.toByteArray().length / (1024.0 * 1024));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
