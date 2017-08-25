/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bigdata.hadoop.util;
import com.bigdata.hadoop.util.mapreduce.ExcelConfig;
import org.apache.hadoop.util.ToolRunner;
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
		if ( args.length != 2 )
		{
			System.err.println("");
			System.err.println("Usage:ExcelResolveDriver < input path > < output path > ");
			System.err.println("Example: hadoop jar ~/ExcelResolveDriver input out");
			System.err.println("Counter:");
			System.exit(-1);
		}

		//记录开始时间
		DateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
		Date start = new Date();

		ExcelConfig conf = new ExcelConfig();
		//运行任务
		int res = ToolRunner.run(conf.getConf(),conf,args);

		//输出任务耗时
		Date end = new Date();
		float time =  (float) (( end.getTime() - start.getTime() ) / 60000.0) ;
		System.out.println( "任务开始：" + formatter.format(start) );
		System.out.println( "任务结束：" + formatter.format(end) );
		System.out.println( "任务耗时：" + String.valueOf( time ) + " 分钟" );

		System.exit(res);
	}

}
