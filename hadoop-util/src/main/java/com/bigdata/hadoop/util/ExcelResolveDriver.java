
package com.bigdata.hadoop.util;
import com.bigdata.hadoop.util.mapreduce.ExcelInputFormat;
import com.bigdata.hadoop.util.mapreduce.ExcelMapper;
import com.bigdata.hadoop.util.mapreduce.ExcelReducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * excel解析入口
 */
public class ExcelResolveDriver {
	
	private static Logger logger = LoggerFactory.getLogger(ExcelResolveDriver.class);
	 /**
     * Main entry point for the example.
     *
     * @param args arguments
     * @throws Exception when something goes wrong
     */
	public static void main(String[] args) throws Exception {		
		//判断参数个数是否正确
		//如果无参数运行则显示以作程序说明
		System.out.println("args="+StringUtils.arrayToString(args));
		if ( args.length < 2 ) {
			System.err.println("");
			System.err.println("Usage:ExcelResolveDriver < input path > < output path > ");
			System.err.println("Example: hadoop jar ~/ExcelResolveDriver input out");
			System.exit(-1);
		}

		//记录开始时间
		DateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
		Date start = new Date();

		//ExcelConfig conf = new ExcelConfig();
		System.setProperty("hadoop.home.dir", "D:\\hadoop-workspace\\hadoop-2.8");
		JobConf c = new JobConf();
		c.set("fs.defaultFS", "hdfs://master.hadoop:9000");
		c.set("mapreduce.app-submission.cross-platform", "true");
		c.set("mapreduce.framework.name", "yarn");
		c.set("mapreduce.job.jar","D:\\hadoop-workspace\\hadoop\\hadoop-util-jar-with-dependencies.jar");
		//c.setJar("D:\\hadoop-workspace\\hadoop\\hadoop-util\\target\\hadoop-util-jar-with-dependencies.jar");
		FileSystem fs = FileSystem.get(c);
		Path pout = new Path(args[1]);
		if(fs.exists(pout)){
			fs.delete(pout, true);
			System.out.println("存在此路径, 已经删除......"+pout.toUri().getRawPath());
		}
		//运行任务
		//int res = ToolRunner.run(c,conf,args);
		org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(c);
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
		//输出任务耗时
		Date end = new Date();
		float time =  (float) (( end.getTime() - start.getTime() ) / 60000.0) ;
		System.out.println( "任务开始：" + formatter.format(start) );
		System.out.println( "任务结束：" + formatter.format(end) );
		System.out.println( "任务耗时：" + String.valueOf( time ) + " 分钟" );

		System.exit(job.waitForCompletion(true)?1:0);
	}

}
