import com.chinaums.poscomm.MsgPacket;
import com.chinaums.poscomm.PospCaller;

import java.util.concurrent.Executors;

public class TestCaller {
    public static void main(String[] args) throws Throwable {
        PospCaller caller = new PospCaller();
        caller.setHosts("127.0.0.1:1234,127.0.0.1:2345");
        caller.setExecutorService(Executors.newFixedThreadPool(10));

        caller.start();

        Thread.sleep(5000);

        MsgPacket result = caller.send(new MsgPacket("60", "1234", "0000", "hello".getBytes()));
        System.out.println(result.getPayload().length);

        caller.stop();
    }
}
