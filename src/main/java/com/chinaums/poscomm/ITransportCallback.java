package com.chinaums.poscomm;

public interface ITransportCallback {
    public void onSuccess(MsgPacket msg);

    public void onTimeout();

    public void onError(Exception e);
}
