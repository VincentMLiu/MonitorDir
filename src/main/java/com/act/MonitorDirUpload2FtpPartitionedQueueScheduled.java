package com.act;

import com.act.Utils.ConfigerationUtils;
import com.act.Utils.FtpUtil;
import com.act.Utils.MonitorDirUtilPartitionedQueue;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MonitorDirUpload2FtpPartitionedQueueScheduled {


        static Logger logger = Logger.getLogger ( MonitorDirUpload2FtpPartitionedQueueScheduled.class.getName ());

    //存放待处理的文件
//     public static LinkedBlockingQueue<List<File>> dealingQueue = new LinkedBlockingQueue<List<File>>(100);

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
        //是否递归子目录
        private static boolean recursiveDirectorySearch;
        //失败重试次数
        private static int retryLimit;

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

        private static List<UploadThread> uploadedThreadList = new ArrayList<UploadThread>();


        private static ScheduledExecutorService scanService;

        private static ScheduledExecutorService uploadService;

    public static void main(String[] args) throws IOException {
        //Init Configs
        //args[0] = properties fileName
        //WINDOWS: {resource dir}/monitorDir.properties
        //Linux: {jar dir}/conf/monitorDir.properties
        PropertyConfigurator.configure("../log4j.properties");
        ConfigerationUtils.init(args[0]);

        monitorFilePath = ConfigerationUtils.get("monitorFilePath", "/");
        includePattern = ConfigerationUtils.get("includePattern", "^\\d{14,}\\.\\d{5,}\\.\\d{5,}\\w.+(.pcm)$");
        ignorePattern = ConfigerationUtils.get("ignorePattern", ".+(.tmp|.uploaded|.loading)$");
        afterUploadSuffix = ConfigerationUtils.get("afterUploadSuffix", ".uploaded");
        scanningFrequency = Long.parseLong(ConfigerationUtils.get("scanningFrequency", "10000"));
        ftpHost = ConfigerationUtils.get("ftpHost", "172.168.12.112");
        ftpUserName = ConfigerationUtils.get("ftpUserName", "ftp_web2changan");
        ftpPassword = ConfigerationUtils.get("ftpPassword", "fraud@2018");
        ftpPort = Integer.parseInt(ConfigerationUtils.get("ftpPort", "21"));
        ftpRemotePath = ConfigerationUtils.get("ftpRemotePath", "/");
        recursiveDirectorySearch = Boolean.parseBoolean(ConfigerationUtils.get("recursiveDirectorySearch", "true"));
        retryLimit = Integer.parseInt(ConfigerationUtils.get("retryLimit", "3"));

        uploadThreadNum = Integer.parseInt(ConfigerationUtils.get("uploadThreadNum", "3"));

        allowCopy = Boolean.getBoolean(ConfigerationUtils.get("allowCopy", "false"));
        copyToBasicPath = ConfigerationUtils.get("copyToBasicPath", "/home");

        File monitorDir = new File(monitorFilePath);
        File[] pointList = monitorDir.listFiles();


        if(StringUtils.isNotBlank(theDay)){
            scanService = Executors.newScheduledThreadPool(pointList.length);
        }else {
            scanService = Executors.newScheduledThreadPool(pointList.length * 2);
        }

        for(int i = 0 ; i < pointList.length ; i++){

            if(pointList[i].isDirectory()){
                if(args.length > 1){
                    theDay = args[1];
                    //scanDirs 指定天
                    ScanDirThread scanDir_td_today = new ScanDirThread(pointList[i].getAbsolutePath(), "scanDir-td-theDay[" + i + "]", theDay);
//                    scanDir_td_today.start();
                    scanService.scheduleWithFixedDelay(scanDir_td_today, 0 , scanningFrequency , TimeUnit.SECONDS);
                }else {
                    //scanDirs 今天
                    ScanDirThread scanDir_td_today = new ScanDirThread(pointList[i].getAbsolutePath(), "scanDir-td-today[" + i + "]", 0);
//                    scanDir_td_today.start();
                    scanService.scheduleWithFixedDelay(scanDir_td_today, 0 , scanningFrequency , TimeUnit.SECONDS);

                    //scanDirs 昨天
                    ScanDirThread scanDir_td_yesterday = new ScanDirThread(pointList[i].getAbsolutePath(), "scanDir-td-yesterday[" + i + "]", -1);
//                    scanDir_td_yesterday.start();
                    scanService.scheduleWithFixedDelay(scanDir_td_yesterday, 0 , scanningFrequency , TimeUnit.SECONDS);


                }
            }
        }

        uploadService = Executors.newScheduledThreadPool(uploadThreadNum);

        //upload TO FTP
        for(int utn = 0; utn < uploadThreadNum; utn++){
            //有多少个线程就先创造多少个QUEUE，模仿kafka的分片，scan_thread写入，upload_thread读取。

            UploadThread uploadTd = new UploadThread("upload-td-num[" + utn + "]", ftpHost, ftpUserName, ftpPassword, ftpPort, ftpRemotePath);
//            uploadTd.start();
            uploadedThreadList.add(uploadTd);
            uploadService.scheduleWithFixedDelay(uploadTd, 30 , 10 , TimeUnit.SECONDS);
        }


    }

    public static class ScanDirThread implements Runnable {
        private Thread t;
        private String threadName;
        private int dayDifference;
        private SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
        private String baseMonitorDirPath;
        private MonitorDirUtilPartitionedQueue monitorDirUtil;
        private long scanningNo = 0l;

        ScanDirThread( String baseMonitorDirPath, String name, String theDay) {
            this.dayDifference = 0;
            threadName = name + theDay;
            this.baseMonitorDirPath = baseMonitorDirPath;
            monitorDirUtil = MonitorDirUtilPartitionedQueue.builder()
                    .spoolDirectory(new File(baseMonitorDirPath))
                    .includePattern(includePattern)
                    .ignorePattern(ignorePattern)
                    .recursiveDirectorySearch(recursiveDirectorySearch)
                    .build();
            logger.info("Creating " +  threadName + " to monitor [" + baseMonitorDirPath + "/" + theDay + "]");
        }


        ScanDirThread( String baseMonitorDirPath, String name, int dayDifference) {
            this.dayDifference = dayDifference;
            threadName = name + dayDifference;
            this.baseMonitorDirPath = baseMonitorDirPath;
            monitorDirUtil = MonitorDirUtilPartitionedQueue.builder()
                    .spoolDirectory(new File(baseMonitorDirPath))
                    .includePattern(includePattern)
                    .ignorePattern(ignorePattern)
                    .recursiveDirectorySearch(recursiveDirectorySearch)
                    .build();
            logger.info("Creating " +  threadName + " to monitor [" + baseMonitorDirPath + "]");
        }

        public void start () {
            logger.info("Starting " +  threadName );

            if (t == null) {
                t = new Thread (this, threadName);
                t.start ();
            }
        }

        public void run() {
            try {

                List<File> candidateFiles;
                String scanningPath;
                if(StringUtils.isNotBlank(theDay)){
                    scanningPath = baseMonitorDirPath + File.separator + theDay;
                    candidateFiles = monitorDirUtil.getCandidateFiles(Paths.get(scanningPath));
                    logger.debug("Scanning No." + scanningNo + " -- Scanning Path: [" + scanningPath + "] get [" + candidateFiles.size() + "] files");
                }else{
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(new Date());
                    cal.add(Calendar.DATE, dayDifference);
                    String datePath = yyyyMMdd.format(cal.getTime());
                    scanningPath = baseMonitorDirPath + File.separator + datePath;
                    candidateFiles = monitorDirUtil.getCandidateFiles(Paths.get(scanningPath));
                    logger.debug("Scanning No." + scanningNo + " -- Scanning Path: [" + scanningPath + "] get [" + candidateFiles.size() + "] files");
                }
                if(!candidateFiles.isEmpty()){
                    uploadedThreadList.get((int)scanningNo%uploadThreadNum).putFileListIntoDealingQueue(candidateFiles);
                    logger.info(" ["+ threadName + "] -- Put "+ candidateFiles.size() + " files of [" + scanningPath + "] into thread [" + uploadedThreadList.get((int)scanningNo%uploadThreadNum).getThreadName() + "]");
                    scanningNo++;
                }
            } catch (Exception e) {
                logger.error("Found Exception in ScanningThread" + threadName);
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


        public  LinkedBlockingQueue<List<File>> dealingQueue = new LinkedBlockingQueue<List<File>>(100);
        private Map<String, Integer> retryMap = new HashMap<String, Integer>();

        UploadThread( String name, String ftpHost, String ftpUserName, String ftpPassword, int ftpPort, String ftpRemotePath) throws IOException {
            this.threadName = name;
            this.ftpHost = ftpHost;
            this.ftpUserName = ftpUserName;
            this.ftpPassword = ftpPassword;
            this.ftpRemotePath = ftpRemotePath;
            this.ftpPort = ftpPort;
            logger.info("Creating " +  threadName );
            this.tFtp = FtpUtil.getThreadSafeFtpClient(ftpHost, ftpUserName, ftpPassword, ftpPort);
            this.workingDir = tFtp.printWorkingDirectory();
        }

        public void start () {
            logger.info("Starting " +  threadName );
            if (t == null) {
                t = new Thread (this, threadName);
                t.start ();
            }
        }
        @Override
        public void run() {
            try {
                List<File> oneBatch = dealingQueue.take();
                if(oneBatch!=null && !oneBatch.isEmpty()){
                    List<File> reDealingList = new ArrayList<>(oneBatch.size());
                    for(File file : oneBatch){
                        //check wether tFtp isConnected
                        while(!tFtp.isConnected()){
                            logger.warn("ftp is not connected will try to re-connect it");
                            tFtp = FtpUtil.getThreadSafeFtpClient(ftpHost, ftpUserName, ftpPassword, ftpPort);
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

                            Integer retryTimesInt = retryMap.get(file.getAbsolutePath());
                            if(retryTimesInt !=null ){
                                if(retryTimesInt <= retryLimit){
                                    logger.info("Unsuccessfully dealing file [" + file.getAbsolutePath() + "], will reUpload it later! -- " + retryTimesInt.intValue());
                                    reDealingList.add(file);
                                    retryMap.put(file.getAbsolutePath(), ++retryTimesInt);
                                }else {
                                    retryMap.remove(file.getAbsolutePath());
                                    logger.warn("Unsuccessfully dealing file [" + file.getAbsolutePath() + "] for "+ retryLimit + " times, it will be dropped!" );
                                }
                            }else {
                                logger.info("Unsuccessfully dealing file [" + file.getAbsolutePath() + "], will reUpload it later!");
                                retryTimesInt = 1;
                                retryMap.put(file.getAbsolutePath(), retryTimesInt);
                                reDealingList.add(file);
                            }

                        }

                    }

                    this.putFileListIntoDealingQueue(reDealingList);
                    logger.info(threadName + " Total dealing file:" + oneBatch.size() + " Successful loaded: " + (oneBatch.size()-reDealingList.size())
                            +" reDealingFile: " + reDealingList.size());
                }else {
                    logger.info(threadName + " get an empty batch");
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
                    logger.warn("ChangeWorkingDirectory [" + workingDir + path + "] = " + success);
                    return success;
                }
                success = tFtp.storeFile(filename, input);
                input.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
            return success;
        }


        public synchronized void putFileListIntoDealingQueue(List<File> candidateFilesList) throws InterruptedException {
            this.dealingQueue.put(candidateFilesList);
        }

        public String getThreadName(){
            return this.threadName;
        }
    }

}
