package com.bigdata.hadoop.util.mapreduce;

import com.bigdata.util.FundLoadUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by WangBin on 2017/8/25.
 */
public class FundItemReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //String code = key.toString();

        super.reduce(key, values, context);
    }

}
