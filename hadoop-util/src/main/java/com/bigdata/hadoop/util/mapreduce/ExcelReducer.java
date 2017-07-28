package com.bigdata.hadoop.util.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by WangBin on 2017/7/28.
 */
public class ExcelReducer extends Reducer<Text,Text,Text,Text> {
    private static Logger logger = LoggerFactory.getLogger(ExcelMapper.class);

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        logger.info("excelReducer.run....");
        super.run(context);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger.info("excelReducer.setup....");
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        logger.info("excelReducer.reduce-called....");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        logger.info("excelReducer.cleanup....");
    }
}
