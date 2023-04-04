package com.jia.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Jia Qi
 * @create 2023-02-02 11:24
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //遍历集合
        long totalUp = 0;
        long tataldown = 0;
        for (FlowBean value : values) {
            totalUp += value.getUpFlow();
            tataldown += value.getDownFlow();
        }

        //封装
        outV.setUpFlow(totalUp);
        outV.setDownFlow(tataldown);
        outV.setSumFlow();

        //写出
        context.write(key,outV);
    }
}
