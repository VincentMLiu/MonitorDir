package com.act;

import com.act.Utils.MonitorDirUtil;

import java.io.File;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.SimpleTimeZone;
import java.util.concurrent.LinkedBlockingQueue;

public class MonitorDirUpload2Ftp {

    //存放待处理的文件
     public static LinkedBlockingQueue<List<File>> deallingQueue = new LinkedBlockingQueue<List<File>>(100);

    private static String monitorFilePath;

    public static void main(String[] args){
        //Init Configs

//        ConfigerationUtils.init(args[0]);
//
//        ConfigerationUtils.get("","");

        monitorFilePath = args[0];

        //scanDirs 今天
        ScanDirThread scanDir_td_today = new ScanDirThread("scanDir-td-today", 0);
        scanDir_td_today.start();

        //scanDirs 昨天
        ScanDirThread scanDir_td_yesterday = new ScanDirThread("scanDir-td-yesterday", -1);
        scanDir_td_yesterday.start();

        //upload TO FTP
        for(int utn = 0; utn < 3; utn++){
            UploadThread uploadTd = new UploadThread("upload-td-num[" + utn + "]");
            uploadTd.start();
        }


    }

    public static class ScanDirThread implements Runnable {
        private Thread t;
        private String threadName;
        private int dayDifference;
        private SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");

        ScanDirThread( String name, int dayDifference) {
            this.dayDifference = dayDifference;
            threadName = name + dayDifference;
            System.out.println("Creating " +  threadName );
        }

        public void start () {
            System.out.println("Starting " +  threadName );
            if (t == null) {
                t = new Thread (this, threadName);
                t.start ();
            }
        }

        public void run() {
            MonitorDirUtil monitorDirUtil = MonitorDirUtil.builder()
                    .spoolDirectory(new File(monitorFilePath))
                    .includePattern("^\\d{8,}\\_\\w.+(.pcm)$")
                    .ignorePattern(".+(.tmp|.loaded)$")
                    .recursiveDirectorySearch(true)
                    .build();

            try {
                while(true){
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(new Date());
                    cal.add(Calendar.DATE, dayDifference);
                    String datePath = yyyyMMdd.format(cal.getTime());

                    List<File> candidateFiles = monitorDirUtil.getCandidateFiles(Paths.get(monitorFilePath + "/" + datePath));
                    if(!candidateFiles.isEmpty()){
                     deallingQueue.put(candidateFiles);
                     System.out.println("NOW deallingQueue has" + deallingQueue);
                    }
                    Thread.sleep(10000l);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class UploadThread implements Runnable {

        private Thread t;
        private String threadName;

        UploadThread( String name) {
            threadName = name;
            System.out.println("Creating " +  threadName );
        }

        public void start () {
            System.out.println("Starting " +  threadName );
            if (t == null) {
                t = new Thread (this, threadName);
                t.start ();
            }
        }
        @Override
        public void run() {
            try {
                while(true){
                    List<File> oneBatch = deallingQueue.take();
                    if(oneBatch!=null && !oneBatch.isEmpty()){
                        for(File file : oneBatch){
                            System.out.println("[" + Thread.currentThread().getName() + "] dealling file [" + file.getAbsolutePath() + "/" + file.getName() + "]");
                            File loadedFile = new File(file.getAbsolutePath() + ".loaded" );
                            file.renameTo(loadedFile);
                            System.out.println("Complete of dealling and rename to " + loadedFile.getName());
                        }
                    }else {
                        System.out.println("There is no new Files need to deal");
                    }

                    Thread.sleep(10000l);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
