package com.act;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

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


    public static void main(String[] args) {
        String monitorPath = "/opt/ftp_gr/records/0011";
        String parentPath = "/opt/ftp_gr/records";
        System.out.println(parentPath.lastIndexOf("/"));
        int i = parentPath.lastIndexOf("/");
        if(i < 0 || i > parentPath.length()){

        }else{
            System.out.println(parentPath.substring(parentPath.lastIndexOf("/")));
        }


        String fileName = "11111111111.txt.loading";
        System.out.println(fileName.substring(0, fileName.lastIndexOf(".loading")));

    }
}
