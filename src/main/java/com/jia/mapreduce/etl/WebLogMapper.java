package com.jia.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Jia Qi
 * @create 2023-02-16 17:06
 */
public class WebLogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
//        String[] line = line.split(" ");
        boolean result = parseLog(line,context);
        //判断清晰
        if (!result){
            return;
        }
        //写出
        context.write(value,NullWritable.get());

    }

    private boolean parseLog(String line, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
        //切割
        String[] fields = line.split(" ");
        //判断日志长度
        if (fields.length>11){
            return true;
        }else {
            return false;
        }
    }
}
