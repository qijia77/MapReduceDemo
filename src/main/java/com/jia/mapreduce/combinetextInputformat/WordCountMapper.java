package com.jia.mapreduce.combinetextInputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Jia Qi
 * @create 2023-02-01 16:18
 */

/**
 * KEYIN, map阶段输入的key类型 Longwriteable
 * VALUEIN, map阶段输入的value类型 Text类型
 * KEYOUT, map阶段的输出类型 text
 * VALUEOUT map阶段的输出的value类型 Int
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outk = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割数据 按空格切分
        String[] words = line.split(" ");
        //循环写出
        for (String word : words) {
            outk.set(word);
            context.write(outk, outV);
        }
    }
}
