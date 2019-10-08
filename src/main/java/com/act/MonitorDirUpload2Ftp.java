package com.act;

import com.act.Utils.ConfigerationUtils;
import com.act.Utils.FtpUtil;
import com.act.Utils.MonitorDirUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.SimpleTimeZone;
import java.util.concurrent.LinkedBlockingQueue;

public class MonitorDirUpload2Ftp {

    //存放待处理的文件
     public static LinkedBlockingQueue<List<File>> dealingQueue = new LinkedBlockingQueue<List<File>>(100);

        //监控基础目录
        private static String monitorFilePath;
        //需要匹配的文件
        private static String includePattern;
        //需要忽略掉的文件
        private static String ignorePattern;
        //上传完成的文件后缀
        private static String afterUploadSuffix;
        //扫描频率
        private static long scanningFrequency=10000l;
        //ftpHost
        private static String ftpHost;
        //ftpUserName
        private static String ftpUserName;
        //ftpPassword
        private static String ftpPassword;
        //ftpPort
        private static int ftpPort;
        //ftpRemotePath默认为根目录
        private static String ftpRemotePath;

        //如果需要扫描指定天
        private static String theDay;

    public static void main(String[] args) throws IOException {
        //Init Configs
        //args[0] = properties fileName
        //WINDOWS: {resource dir}/monitorDir.properties
        //Linux: {jar dir}/conf/monitorDir.properties
        ConfigerationUtils.init(args[0]);

        monitorFilePath = ConfigerationUtils.get("monitorFilePath", "/");
        includePattern = ConfigerationUtils.get("includePattern", "^\\d{14,}\\.\\d{5,}\\.\\d{5,}\\w.+(.pcm)$");
        ignorePattern = ConfigerationUtils.get("ignorePattern", ".+(.tmp|.uploaded)$");
        afterUploadSuffix = ConfigerationUtils.get("afterUploadSuffix", ".uploaded");
        scanningFrequency = Long.parseLong(ConfigerationUtils.get("scanningFrequency", "10000"));
        ftpHost = ConfigerationUtils.get("ftpHost", "172.168.12.112");
        ftpUserName = ConfigerationUtils.get("ftpUserName", "ftp_web2changan");
        ftpPassword = ConfigerationUtils.get("ftpPassword", "fraud@2018");
        ftpPort = Integer.parseInt(ConfigerationUtils.get("ftpPort", "21"));
        ftpRemotePath = ConfigerationUtils.get("ftpRemotePath", "/");
        File monitorDir = new File(monitorFilePath);
        monitorDir.list();

        if(args.length > 1){
            theDay = args[1];
            //scanDirs 指定天
            ScanDirThread scanDir_td_today = new ScanDirThread("scanDir-td-theDay", theDay);
            scanDir_td_today.start();
        }else {
            //scanDirs 今天
            ScanDirThread scanDir_td_today = new ScanDirThread("scanDir-td-today", 0);
            scanDir_td_today.start();

            //scanDirs 昨天
            ScanDirThread scanDir_td_yesterday = new ScanDirThread("scanDir-td-yesterday", -1);
            scanDir_td_yesterday.start();
        }


        //upload TO FTP
        for(int utn = 0; utn < 3; utn++){
            UploadThread uploadTd = new UploadThread("upload-td-num[" + utn + "]", ftpHost, ftpUserName, ftpPassword, ftpPort, ftpRemotePath);
            uploadTd.start();
        }


    }

    public static class ScanDirThread implements Runnable {
        private Thread t;
        private String threadName;
        private int dayDifference;
        private SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");

        ScanDirThread( String name, String theDay) {
            this.dayDifference = 0;
            threadName = name + theDay;
            System.out.println("Creating " +  threadName );
        }


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
                    .includePattern(includePattern)
                    .ignorePattern(ignorePattern)
                    .recursiveDirectorySearch(true)
                    .build();

            try {
                while(true){
                    List<File> candidateFiles;
                    if(StringUtils.isNotBlank(theDay)){
                        candidateFiles = monitorDirUtil.getCandidateFiles(Paths.get(monitorFilePath + File.separator + theDay));
                    }else{
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(new Date());
                        cal.add(Calendar.DATE, dayDifference);
                        String datePath = yyyyMMdd.format(cal.getTime());

                        candidateFiles = monitorDirUtil.getCandidateFiles(Paths.get(monitorFilePath + File.separator + datePath));
                    }
                    if(!candidateFiles.isEmpty()){
                        dealingQueue.put(candidateFiles);
                     System.out.println("NOW dealingQueue has" + dealingQueue);
                    }
                    Thread.sleep(scanningFrequency);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class UploadThread implements Runnable {

        private Thread t;
        private String threadName;
        private String ftpHost;
        private String ftpUserName;
        private String ftpPassword;
        private String ftpRemotePath;
        private String workingDir;
        private int ftpPort;
        private FTPClient tFtp;

        UploadThread( String name, String ftpHost, String ftpUserName, String ftpPassword, int ftpPort, String ftpRemotePath) throws IOException {
            this.threadName = name;
            this.ftpHost = ftpHost;
            this.ftpUserName = ftpUserName;
            this.ftpPassword = ftpPassword;
            this.ftpRemotePath = ftpRemotePath;
            this.ftpPort = ftpPort;
            System.out.println("Creating " +  threadName );
            this.tFtp = FtpUtil.getThreadSafeFtpClient(ftpHost, ftpUserName, ftpPassword, ftpPort);
            this.workingDir = tFtp.printWorkingDirectory();
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
                    List<File> oneBatch = dealingQueue.take();
                    if(oneBatch!=null && !oneBatch.isEmpty()){

                        for(File file : oneBatch){
                            //check wether tFtp isConnected
                            while(!tFtp.isConnected()){
                                System.out.println("");
                                tFtp = FtpUtil.getThreadSafeFtpClient(ftpHost, ftpUserName, ftpPassword, ftpPort);
                                Thread.sleep(30000l);
                            }
                            System.out.println("[" + Thread.currentThread().getName() + "] dealing file [" + file.getAbsolutePath() + "]");
                            //GET Path
                            String hourDirAbs = file.getParent();
                            String dayDirAbs = new File(file.getParent()).getParent();
                            String hourDir = hourDirAbs.substring(hourDirAbs.lastIndexOf(File.separator) + 1);
                            String dayDir = dayDirAbs.substring(dayDirAbs.lastIndexOf(File.separator) + 1);
                            String wholePath = ftpRemotePath + File.separator + dayDir + File.separator + hourDir;

                            //upload to ftp
                            Boolean uploadSucceed = this.uploadFile(wholePath, file.getName(),  file);
                            if(uploadSucceed){
                                System.out.println("Successfully upload file to [" + wholePath + File.separator + file.getName() + "]");
                                File loadedFile = new File(file.getAbsolutePath() + afterUploadSuffix );
                                file.renameTo(loadedFile);
                                System.out.println("Complete of dealing file [" + file.getAbsolutePath() + "] and rename to [" + loadedFile.getName() + "]");
                            }else{
                                System.out.println("Unsuccessfully dealing file [" + file.getAbsolutePath() + "], will scan it later!");
                            }

                        }
                    }else {
                        System.out.println("This batch is empty");
                    }

                    Thread.sleep(10000l);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        public boolean uploadFile(String path, String filename,File localFile) {
            boolean success = false;
            try {
                int reply;
                reply = tFtp.getReplyCode();
                if (!FTPReply.isPositiveCompletion(reply)) {
                    tFtp.disconnect();
                    return success;
                }

                InputStream input = new FileInputStream(localFile);
                tFtp.enterLocalPassiveMode();
                tFtp.setBufferSize(1024);
                success = tFtp.setFileType(FTP.BINARY_FILE_TYPE);
                tFtp.setControlEncoding("UTF-8");
                success = tFtp.changeWorkingDirectory(workingDir + path);
                if(!success){
                    System.out.println("ChangeWorkingDirectory [" + workingDir + path + "] = " + success);
                    return success;
                }
                success = tFtp.storeFile(filename, input);
                input.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
            return success;
        }
    }

}
