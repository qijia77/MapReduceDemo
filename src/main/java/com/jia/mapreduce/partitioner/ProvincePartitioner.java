package com.jia.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Jia Qi
 * @create 2023-02-03 12:14
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phone = text.toString();
        String prephone = phone.substring(0, 3);
        int partiton;
        if ("136".equals(prephone)) {
            partiton = 0;
        } else if ("137".equals(prephone)) {
            partiton = 1;
        } else if ("138".equals(prephone)) {
            partiton = 2;
        } else if ("139".equals(prephone)) {
            partiton = 3;
        } else {
            partiton = 4;
        }
        return partiton;

    }
}
