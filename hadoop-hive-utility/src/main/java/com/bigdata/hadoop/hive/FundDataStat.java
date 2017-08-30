package com.bigdata.hadoop.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

/**
 * Created by WangBin on 2017/8/29.
 */
public class FundDataStat {
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://djt002:10000/default";
    private static String user = "";
    private static String password = "";
    private static String sql = "";
    private static ResultSet res;
    private static final Logger log = LoggerFactory.getLogger(FundDataStat.class);

    public static void main(String[] arg){

    }

}