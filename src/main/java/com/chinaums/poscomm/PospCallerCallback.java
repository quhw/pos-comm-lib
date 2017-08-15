package com.chinaums.poscomm;

public interface PospCallerCallback {
    public void onSuccess(MsgPacket msg);
    public void onTimeout();
    public void onError(Throwable e);
}
