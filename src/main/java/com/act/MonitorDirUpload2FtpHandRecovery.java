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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class MonitorDirUpload2FtpHandRecovery {

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
        //上报线程数
        private static int uploadThreadNum = 3;
        //是否拷贝
        private static Boolean allowCopy = false;
        //拷贝到哪
        private static String copyToBasicPath;

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


        uploadThreadNum = Integer.parseInt(ConfigerationUtils.get("uploadThreadNum", "3"));

        allowCopy = Boolean.getBoolean(ConfigerationUtils.get("allowCopy", "false"));
        copyToBasicPath = ConfigerationUtils.get("copyToBasicPath", "/home");

        File monitorDir = new File(monitorFilePath);
        File[] pointList = monitorDir.listFiles();

        for(int i = 0 ; i < pointList.length ; i++){

            if(pointList[i].isDirectory()){
                theDay = args[1];
                //scanDirs 指定天
                ScanDirThread scanDir_td_today = new ScanDirThread(pointList[i].getAbsolutePath(), "scanDir-td-theDay[" + i + "]", theDay);
                scanDir_td_today.start();
            }
        }


        //upload TO FTP
        for(int utn = 0; utn < uploadThreadNum; utn++){
            //有多少个线程就先创造多少个QUEUE，模仿kafka的分片，scan_thread写入，upload_thread读取。


            UploadThread uploadTd = new UploadThread("upload-td-num[" + utn + "]", ftpHost, ftpUserName, ftpPassword, ftpPort, ftpRemotePath);
            uploadTd.start();
        }


    }

    public static class ScanDirThread implements Runnable {
        private Thread t;
        private String threadName;
        private int dayDifference;
        private SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
        private String baseMonitorDirPath;

        ScanDirThread( String baseMonitorDirPath, String name, String theDay) {
            this.dayDifference = 0;
            threadName = name + theDay;
            this.baseMonitorDirPath = baseMonitorDirPath;
            System.out.println("Creating " +  threadName + " to monitor [" + baseMonitorDirPath + "/" + theDay + "]");
        }


        ScanDirThread( String baseMonitorDirPath, String name, int dayDifference) {
            this.dayDifference = dayDifference;
            threadName = name + dayDifference;
            this.baseMonitorDirPath = baseMonitorDirPath;
            System.out.println("Creating " +  threadName + " to monitor [" + baseMonitorDirPath + "]");
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
                    .spoolDirectory(new File(baseMonitorDirPath))
                    .includePattern(includePattern)
                    .ignorePattern(ignorePattern)
                    .recursiveDirectorySearch(true)
                    .build();

            try {
                List<File> candidateFiles;
                candidateFiles = monitorDirUtil.getCandidateFiles(Paths.get(baseMonitorDirPath + File.separator + theDay));
                if(!candidateFiles.isEmpty()){
                    dealingQueue.put(candidateFiles);
                }
                Thread.sleep(scanningFrequency);
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
                            //GET Path
                            ///opt/ftp_gr/records/0011/20191010/14/20191010145007.00002.44830.up.voic.pcm
//                            String[] dirNames = file.getAbsolutePath().split(File.separator);
//                            String hourDir = dirNames[dirNames.length - 2];
//                            String dayDir = dirNames[dirNames.length - 3];
//                            String pointDir = dirNames[dirNames.length - 4];
//                            String uploadedPath = ftpRemotePath + File.separator + pointDir + File.separator + dayDir + File.separator + hourDir;

                            String fileParentAbsolutePath = file.getParent();
                            String uploadedPath = "/";
                            //如果不是传到根目录下
                            if(!ftpRemotePath.equals("/")){
                                int cut = fileParentAbsolutePath.lastIndexOf(ftpRemotePath);
                                if(cut < 0 || cut > fileParentAbsolutePath.length()){
                                    uploadedPath = ftpRemotePath;
                                } else {
                                    uploadedPath = fileParentAbsolutePath.substring(fileParentAbsolutePath.lastIndexOf(ftpRemotePath));
                                }
                            }

                            //upload to ftp
                            String fileOriginalName = file.getName().substring(0,file.getName().lastIndexOf(".loading"));

                            Boolean uploadSucceed = this.uploadFile(uploadedPath, fileOriginalName,  file);
                            if(uploadSucceed){
                                //拷贝到其他目录
                                if(allowCopy && StringUtils.isNotBlank(copyToBasicPath)){
                                    String copyToPath = copyToBasicPath +  uploadedPath + file.getName();
                                    Files.copy(file.toPath(), new File(copyToPath).toPath());
                                }
                                String uploadedName= file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".loading"))+ afterUploadSuffix;
                                File loadedFile = new File(uploadedName);
                                file.renameTo(loadedFile);
                            }else{
                                System.out.println("Unsuccessfully dealing file [" + file.getAbsolutePath() + "], will scan it later!");
                            }

                        }
                    }else {
                        System.out.println("This batch is empty");
                    }

                    Thread.sleep(10000l);
                }
            } catch (InterruptedException | IOException e) {
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
