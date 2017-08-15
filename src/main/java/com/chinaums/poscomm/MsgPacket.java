package com.chinaums.poscomm;

public class MsgPacket {
    private String version = "60";
    private String tpduDst;
    private String tpduSrc;
    private byte[] payload;

    public MsgPacket(String version, String tpduDst, String tpduSrc, byte[] payload) {
        this.version = version;
        this.tpduDst = tpduDst;
        this.tpduSrc = tpduSrc;
        this.payload = payload;
    }

    public MsgPacket() {
    }

    public String getVersion() {
        return version;
    }

    public String getTpduDst() {
        return tpduDst;
    }

    public String getTpduSrc() {
        return tpduSrc;
    }

    public byte[] getPayload() {
        return payload;
    }
}
