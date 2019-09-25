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
        String parentPath = "/opt/ftp_gr/records/0011/20190829";
        System.out.println(parentPath.substring(parentPath.lastIndexOf("/") + 1));

    }
}
