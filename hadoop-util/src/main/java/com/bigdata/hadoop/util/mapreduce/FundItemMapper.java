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
public class FundItemMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Logger logger = LoggerFactory.getLogger(FundItemMapper.class);

    public void map(LongWritable key, Text value, Context context)
            throws InterruptedException, IOException {
        String code = value.toString();
        code = code.split("\t")[0];
        logger.info("load-history-pe-for-{}",code);
        String line = FundLoadUtil.loadByCode(code);
        //ArrayList<Text> list = new ArrayList();
        String[] r = line.split("\n");
        if(r!=null&& r.length>0){
            //String[] row = null;
            for (String s : r) {
                //list.add(new Text(s));
                //row = s.split(" ");
                context.write(new Text(s),null);
            }
        }
        //context.write(new Text(line), null);
        logger.info("Map processing finished,fund-pe-result:{}",line);
    }
}
