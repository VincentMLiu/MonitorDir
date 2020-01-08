package com.act;

import static org.junit.Assert.assertTrue;

import com.act.Utils.MonitorDirUtilPartitionedQueue;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
//    /**
//     * Rigorous Test :-)
//     */
//    @Test
//    public void shouldAnswerWithTrue()
//    {
//        assertTrue( true );
//    }
    private static ScheduledExecutorService scanService;

    public static void main(String[] args) {
//        String monitorPath = "/opt/ftp_gr/records/0011";
//        String parentPath = "/opt/ftp_gr/records";
//        System.out.println(parentPath.lastIndexOf("/"));
//        int i = parentPath.lastIndexOf("/");
//        if(i < 0 || i > parentPath.length()){
//
//        }else{
//            System.out.println(parentPath.substring(parentPath.lastIndexOf("/")));
//        }
//        String fileName = "11111111111.txt.loading";
//        System.out.println(fileName.substring(0, fileName.lastIndexOf(".loading")));

        scanService = Executors.newScheduledThreadPool(3);
        for(int i = 0; i < 10; i ++){
            ScanDirThread scanDir_td_today = new ScanDirThread(i);
    //                    scanDir_td_today.start();
            scanService.scheduleWithFixedDelay(scanDir_td_today, 0 , 5 , TimeUnit.SECONDS);
        }

    }


    public static class ScanDirThread implements Runnable {
        private Thread t;
        private String threadName = "ScanDirThread";
        private int num;

        ScanDirThread(int num) {
            threadName = threadName + ":" + num;
        }

        public void start () {
            if (t == null) {
                t = new Thread (this, threadName);
                t.start ();
            }
        }

        public void run() {
            System.out.println("[" + threadName + "] run " + new Date());

        }
    }

}
