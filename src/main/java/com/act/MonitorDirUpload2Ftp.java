package com.act;

import com.act.Utils.MonitorDirUtil;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class MonitorDirUpload2Ftp {

    //存放待处理的文件
    public static Queue<List<File>> deallingQueue = new LinkedBlockingQueue<List<File>>();


    public static void main(String[] args){
        //Init Configs

        Properties pp = new Properties();

        //scanDirs
        Thread scanTd = new Thread(new ScanDirThread());
        scanTd.start();

    }

    public static class ScanDirThread implements Runnable {
        public void run() {
            MonitorDirUtil monitorDirUtil = MonitorDirUtil.builder().build();
            List<File> candidateFiles = monitorDirUtil.getCandidateFiles(Paths.get(new File("").getAbsolutePath()));
            deallingQueue.offer(candidateFiles);
            try {
                Thread.sleep(300000l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
