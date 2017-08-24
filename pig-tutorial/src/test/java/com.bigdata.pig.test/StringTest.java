package com.bigdata.pig.test;

import com.bigdata.pig.FundsRetrive;

/**
 * Created by WangBin on 2017/8/24.
 */
public class StringTest {
    public static void main(String[] args)throws Exception{
        String val = new FundsRetrive().getHistoryData("150008");
        System.out.println("==>"+val);
    }
}
