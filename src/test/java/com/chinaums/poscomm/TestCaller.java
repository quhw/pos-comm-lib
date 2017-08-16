package com.chinaums.poscomm;

import java.text.DecimalFormat;

public class TestCaller {
    public static void main(String[] args) throws Throwable {
        LongTCPTransportWithTPDU transport = new LongTCPTransportWithTPDU();
        transport.setHosts("127.0.0.1:1234");
        transport.start();

        Thread.sleep(1000);

        for(int i=0; i<10000; i++) {
            String seqStr = new DecimalFormat("0000").format(i);
            transport.send(new MsgPacket("60", "1234", seqStr, "hello".getBytes()),
                    new ITransportCallback() {
                        public void onSuccess(MsgPacket msg) {
                            System.out.println("Success: " + msg.getTpduDst());
                        }

                        public void onTimeout() {
                            System.out.println("Timeout");
                            System.exit(-1);
                        }

                        public void onError(Exception e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }
                    });
        }
    }
}
