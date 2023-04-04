package com.jia.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Jia Qi
 * @create 2023-02-01 16:19
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

    private IntWritable outV = new IntWritable();;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum+= value.get();
        }
        outV.set(sum);
        context.write(key, outV);
    }
}
