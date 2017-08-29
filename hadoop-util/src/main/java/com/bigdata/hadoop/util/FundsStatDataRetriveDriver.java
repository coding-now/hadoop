package com.bigdata.hadoop.util;

import com.bigdata.hadoop.util.mapreduce.FundItemMapper;
import com.bigdata.hadoop.util.mapreduce.FundItemReducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by WangBin on 2017/8/18.
 */
public class FundsStatDataRetriveDriver {

    public static void main(String[] args)throws Exception {
        //http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=150008&page=1&per=1000&sdate=2015-08-01&edate=
        logger.info("FundsStatDataRetriveDriver-start...");
        System.setProperty("hadoop.home.dir", "D:\\hadoop-workspace\\hadoop-2.8");
        JobConf c = new JobConf();
        c.set("fs.defaultFS", "hdfs://master.hadoop:9000");
        c.set("mapreduce.app-submission.cross-platform", "true");
        c.set("mapreduce.framework.name", "yarn");
        c.set("mapreduce.job.jar","D:\\hadoop-workspace\\hadoop\\hadoop-util-jar-with-dependencies.jar");
        c.setJar("D:\\hadoop-workspace\\hadoop\\hadoop-util\\target\\hadoop-util-jar-with-dependencies.jar");
        FileSystem fs = FileSystem.get(c);
        Path pout = new Path(args[1]);
        if(fs.exists(pout)){
            fs.delete(pout, true);
            System.out.println("存在此路径, 已经删除......"+pout.toUri().getRawPath());
        }

        Job job = Job.getInstance(c);
        job.setJarByClass(FundsStatDataRetriveDriver.class);
        job.setJobName("FundStatDataLoaderDriver");

        job.setMapperClass(FundItemMapper.class);
        job.setReducerClass(FundItemReducer.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //job.setInputFormatClass(ExcelInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean ret = job.waitForCompletion(true);
        logger.info("excelResolver,ret:{}", ret);
    }
    private static Logger logger = LoggerFactory.getLogger(FundsStatDataRetriveDriver.class);

}
