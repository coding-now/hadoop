package com.bigdata.hadoop.util.mapreduce;

import com.bigdata.hadoop.util.ExcelResolveDriver;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Created by WangBin on 2017/7/28.
 */
public class Config extends Configured implements Tool {
    public static enum LineCounter {
        EXCEL_LINE_SKIP
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println("Excel Resolver Driver start...");

        Job job = new Job();
        job.setJarByClass(ExcelResolveDriver.class);
        job.setJobName("Excel-Record-Resolver");

        job.setMapperClass(ExcelMapper.class);
        job.setReducerClass(ExcelReducer.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(ExcelInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean ret = job.waitForCompletion(true);
        System.out.println("excelResolver,ret:"+ret);

        //输出任务完成情况
        System.out.println( "任务名称：" + job.getJobName() );
        System.out.println( "任务成功：" + ( job.isSuccessful()?"是":"否" ) );
        System.out.println( "输入行数：" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue() );
        System.out.println( "输出行数：" + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue() );
        System.out.println( "跳过的行：" + job.getCounters().findCounter(LineCounter.EXCEL_LINE_SKIP).getValue() );

        return job.isSuccessful() ? 0 : 1;
    }
}
