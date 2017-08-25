package com.bigdata.pig.test;

import com.bigdata.pig.FundsRetrive;

/**
 * Created by WangBin on 2017/8/24.
 */
public class StringUtilTest {
    public static void main(String[] args)throws Exception{
        String val = "";
        val = new FundsRetrive().getHistoryData("150008");
        /*val = "2/29";
        System.out.println(val.contains("/"));
        System.out.println("==>"+val.substring(0,val.indexOf("/")));*/
        System.out.println(val);
    }
}
