package com.act;

import com.act.Utils.MonitorDirUtil;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

public class MonitorDirUpload2FtpSingleThread {

    //存放待处理的文件
    public static LinkedBlockingQueue<List<File>> deallingQueue = new LinkedBlockingQueue<List<File>>();

    private static String monitorFilePath;

    public static void main(String[] args) {

        //Init Configs

//        ConfigerationUtils.init(args[0]);
//
//        ConfigerationUtils.get("","");




//        String fileName = "20190915_alkjsoiur.pcm.loaded";
//
//        Pattern inP = Pattern.compile("^\\d{8,}\\_\\w.+(.pcm)$");
//        Pattern inE = Pattern.compile(".+(.tmp|.loaded)$");
//
//        System.out.println(inP.matcher(fileName).matches());
//        System.out.println(inE.matcher(fileName).matches());

        monitorFilePath = args[0];

        MonitorDirUtil monitorDirUtil = MonitorDirUtil.builder()
                .spoolDirectory(new File(monitorFilePath))
                .includePattern("^\\d{8,}\\_\\w.+(.pcm)$")
                .ignorePattern(".+(.tmp|.loaded)$")
                .recursiveDirectorySearch(true)
                .build();
        try {
            while(true){
                List<File> candidateFiles = monitorDirUtil.getCandidateFiles(Paths.get(monitorFilePath));

                System.out.println("NOW candidateFiles has" + candidateFiles);

                if(candidateFiles!=null && !candidateFiles.isEmpty()){
                    for(File file : candidateFiles){
                        System.out.println("[" + Thread.currentThread().getName() + "] dealling file [" + file.getAbsolutePath() + "/" + file.getName() + "]");
                        File loadedFile = new File(file.getAbsolutePath() + ".loaded" );
                        file.renameTo(loadedFile);
                        System.out.println("Complete of dealling and rename to" + loadedFile.getName());
                    }
                }else {
                    System.out.println("There is no new Files need to deal");
                }

                Thread.sleep(15000l);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }












    }



}
