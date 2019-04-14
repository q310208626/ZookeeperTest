package com.lsj.test.zkcli;

import java.util.concurrent.CountDownLatch;

public class ZkConnectCountDownUtils {
    private static CountDownLatch connectCountDown = new CountDownLatch(1);

    public static void beforeConnect() throws InterruptedException {
        connectCountDown.await();
    }

    public static void afterConnect(){
        connectCountDown.countDown();
    }
}
