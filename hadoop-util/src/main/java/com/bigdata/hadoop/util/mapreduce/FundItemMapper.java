package com.bigdata.hadoop.util.mapreduce;

import com.bigdata.util.FundLoadUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by WangBin on 2017/8/25.
 */
public class FundItemMapper extends Mapper<LongWritable, Text, Iterator<Text>, Text> {

    private static Logger logger = LoggerFactory.getLogger(FundItemMapper.class);

    public void map(LongWritable key, Text value, Context context)
            throws InterruptedException, IOException {
        String line = value.toString();
        line = line.split("\t")[0];
        logger.info("load-history-pe-for-{}",line);
        line = FundLoadUtil.loadByCode(line);
        ArrayList<Text> list = new ArrayList();
        String[] r = line.split("\n");
        if(r!=null&& r.length>0){
            for (String s : r) {
                list.add(new Text(s));
            }
        }
        context.write(list.iterator(), null);
        logger.info("Map processing finished,fund-pe-result:{}",line);
    }
}
