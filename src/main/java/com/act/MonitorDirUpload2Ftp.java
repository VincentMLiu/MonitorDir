package com.act;

import java.io.File;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class MonitorDirUpload2Ftp {

    //存放待处理的文件
    public static Queue<List<File>> deallingQueue = new LinkedBlockingQueue<List<File>>();

    public static void main(String[] args){
        //Init Configs



    }
}
