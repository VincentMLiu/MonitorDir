package com.act.Utils;

import org.apache.commons.net.ftp.*;

import java.io.*;
import java.net.SocketException;

public class FtpUtil {
    private static FTPClient ftp;

    /**
     * Description:
     * @param path
     * @param filename
     * @param localFile
     * @throws Exception
     */
    public static boolean uploadFile(String path, String filename,String localFile) throws Exception {
        boolean success = false;
        try {
//	        int reply;
//	        reply = ftp.getReplyCode();
//	        if (!FTPReply.isPositiveCompletion(reply)) {
//	            ftp.disconnect();
//	            return success;
//	        }

            InputStream input = new FileInputStream(new File(localFile));
            ftp.enterLocalPassiveMode();
            ftp.setBufferSize(1024);
            success = ftp.setFileType(FTP.BINARY_FILE_TYPE);
//	        System.out.println("ftp.setFileType(FTP.BINARY_FILE_TYPE) ====="  + success);
            ftp.setControlEncoding("UTF-8");
//	        ftp.makeDirectory(path);
            success = ftp.changeWorkingDirectory(path);
//	        System.out.println("ftp.changeWorkingDirectory(path) ====="  + success);
            success = ftp.storeFile(filename, input);
//	        System.out.println("ftp.storeFile(filename, input) ====="  + success);
//	    	ftp.rename(filename,filename.substring(0,filename.lastIndexOf(".")));
            input.close();
//	        ftp.logout();
            success = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return success;
    }

    public static boolean uploadFile2GR(String path, String filename,String localFile) {
        boolean success = false;
        try {
            int reply;
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return success;
            }

            InputStream input = new FileInputStream(new File(localFile));
            ftp.enterLocalPassiveMode();
            ftp.setBufferSize(1024);
            ftp.setFileType(FTP.BINARY_FILE_TYPE);
            ftp.setControlEncoding("UTF-8");
            ftp.changeWorkingDirectory(path);
            ftp.storeFile(filename, input);
            ftp.rename(filename,filename.substring(0,filename.lastIndexOf(".")));
            input.close();
            ftp.logout();
            success = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException ioe) {
                }
            }
        }
        return success;
    }


    /**
     * 获取FTPClient对象
     *
     * @param ftpHost     FTP主机服务器
     * @param ftpPassword FTP 登录密码
     * @param ftpUserName FTP登录用户名
     * @param ftpPort     FTP端口 默认为21
     * @return
     */
    public static void initFtpClient(String ftpHost, String ftpUserName,
                                     String ftpPassword, int ftpPort) {
        try {
            ftp = new FTPClient();
            ftp.connect(ftpHost, ftpPort);// 连接FTP服务器
            ftp.login(ftpUserName, ftpPassword);// 登陆FTP服务器
            if (!FTPReply.isPositiveCompletion(ftp.getReplyCode())) {
                System.out.println("未连接到FTP，用户名或密码错误。");
                ftp.disconnect();
            } else {
                System.out.println("FTP连接成功。");
            }
        } catch (SocketException e) {
            e.printStackTrace();
            System.out.println("FTP的IP地址可能错误，请正确配置。");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("FTP的端口错误,请正确配置。");
        }
    }


    public static void shutDownConnection() {
        boolean isLogout = false;
        if (ftp.isConnected()) {
            try {
                ftp.logout();
                ftp.disconnect();
                isLogout=true;
            } catch (IOException ioe) {
            }
        }
    }

    /*
     * 从FTP服务器下载文件
     *
     * @param ftpHost FTP IP地址
     * @param ftpUserName FTP 用户名
     * @param ftpPassword FTP用户名密码
     * @param ftpPort FTP端口
     * @param ftpPath FTP服务器中文件所在路径 格式： ftptest/aa
     * @param localPath 下载到本地的位置 格式：H:/download
     * @param fileName 文件名称
     * @param fileType 下载那一类的文件
     */
    public static void downloadFtpFile(String ftpHost, String ftpUserName,
                                       String ftpPassword, int ftpPort, String ftpPath, String localPath,
                                       String fileType) {


        try {
            initFtpClient(ftpHost, ftpUserName, ftpPassword, ftpPort);
            ftp.setControlEncoding("UTF-8"); // 中文支持
            ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
            ftp.enterLocalPassiveMode();
            /*ftpClient.changeWorkingDirectory(ftpPath);*/
            ftp.setFileTransferMode(FTPClient.STREAM_TRANSFER_MODE);

            File localDir = new File(localPath);
            if (!localDir.exists()) {
                localDir.mkdirs();
            }
            /*FTPFile[] okFiles1 = ftpClient.listFiles(ftpPath);*/
            FTPFile[] files = ftp.listFiles(ftpPath, new FTPFileFilter() {
                public boolean accept(FTPFile file) {
                    String fileName = file.getName().toString();
                    return  fileName.contains(fileType);
                }
            });


            for (FTPFile file : files) {
                if (file != null) {
                    String downloadFileName = file.getName().replaceAll("\\.ok$", ".zip");
                    String localFileName = localPath +  File.separator + downloadFileName;
                    OutputStream os = new FileOutputStream(localFileName);
                    downloadFileName = ftpPath +"/" + downloadFileName;

                    try {
                        boolean success = ftp.retrieveFile(downloadFileName, os);
                        System.out.println(success);
                    } catch (IOException ex) {
                        System.out.println(ex.getMessage());
                    } finally {
                        os.close();

                    }
                }
            }

            //删除文件
            ftp.changeWorkingDirectory(ftpPath);
            for (FTPFile file : files) {
                if (file != null) {
                    try {

                        ftp.deleteFile(file.getName());//删除文件

                    } catch (IOException ex) {
                        System.out.println(ex.getMessage());
                    } finally {


                    }
                }
            }

            ftp.logout();

        } catch (FileNotFoundException e) {
            System.out.println("没有找到" + ftpPath + "文件");
            e.printStackTrace();
        } catch (SocketException e) {
            System.out.println("连接FTP失败.");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("文件读取错误。");
            e.printStackTrace();
        }finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }


    public static boolean mkdir(String path) {
        boolean success = false;
        try {
            success = ftp.makeDirectory(path);
            if(success) {
                System.out.println("successfully create dir " + path);
            }else {
                System.out.println("failed create dir " + path);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return success;
    }


    /**
     * 获取FTPClient对象
     *
     * @param ftpHost     FTP主机服务器
     * @param ftpPassword FTP 登录密码
     * @param ftpUserName FTP登录用户名
     * @param ftpPort     FTP端口 默认为21
     * @return
     */
    public static FTPClient getThreadSafeFtpClient(String ftpHost, String ftpUserName, String ftpPassword, int ftpPort){
        FTPClient tFtp = new FTPClient();
        try {
            tFtp.connect(ftpHost, ftpPort);// 连接FTP服务器
            tFtp.login(ftpUserName, ftpPassword);// 登陆FTP服务器
            if (!FTPReply.isPositiveCompletion(tFtp.getReplyCode())) {
                System.out.println("未连接到FTP，用户名或密码错误。");
                tFtp.disconnect();
            } else {
                System.out.println("FTP连接成功。");
            }
        } catch (SocketException e) {
            e.printStackTrace();
            System.out.println("FTP的IP地址可能错误，请正确配置。");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("FTP的端口错误,请正确配置。");
        }

        return tFtp;
    }

}
