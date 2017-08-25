package com.bigdata.hadoop.util.mapreduce;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.InputStream;

/**
 * Created by WangBin on 2017/8/25.
 */
public class FundCodeRecordReader extends TextRecordReader {

    @Override
    protected String readSplitFile(TaskAttemptContext context, InputStream is) {
        return null;
    }
}
