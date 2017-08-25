package com.bigdata.hadoop.util.mapreduce;

import com.bigdata.hadoop.util.parser.ExcelParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by WangBin on 2017/8/25.
 */
public abstract class TextRecordReader extends RecordReader<LongWritable, Text> {

    protected static Logger logger = LoggerFactory.getLogger(ExcelRecordReader.class);
    private LongWritable key;
    private Text value;
    private InputStream is;
    private String[] lines;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {

        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        final Path file = split.getPath();

        FileSystem fs = file.getFileSystem(job);
        try {
            FSDataInputStream fileIn = fs.open(split.getPath());
            is = fileIn;
            String line = readSplitFile(context,is);
            this.lines = line.split("\n");
        }catch (IOException ex){
            logger.error("hadoop-fs-getInputError:{}",split.getPath(),ex);
            throw ex;
        }
    }
    protected abstract String readSplitFile(TaskAttemptContext context,InputStream is);

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable(0);
            value = new Text(lines[0]);
        } else {
            if (key.get() < (this.lines.length - 1)) {
                long pos = (int) key.get();
                key.set(pos + 1);
                value.set(this.lines[(int) (pos + 1)]);
                pos++;
            } else {
                return false;
            }
        }
        if (key == null || value == null) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        if (is != null) {
            is.close();
        }
    }
}
