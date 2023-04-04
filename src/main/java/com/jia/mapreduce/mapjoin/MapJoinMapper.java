package com.jia.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author Jia Qi
 * @create 2023-02-16 16:21
 */
public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private HashMap<Object, Object> pdmap = new HashMap<>();
    private Text outK = new Text();
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //获取缓存文件，并把文件内容封装到集合pd.txt中
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));
        //从流中读数据
//        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis, String.valueOf(new InputStreamReader(fis, "UTF-8"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){
            String[] fields = line.split("\t");
            pdmap.put(fields[0],fields[1]);


        }
        IOUtils.closeStream(reader);

    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        //获取pid
        String pname = (String) pdmap.get(fields[1]);
        //获取订单id和数量
        outK.set(fields[0]+"\t"+pname+"\t"+fields[2]);
        context.write(outK,NullWritable.get());

    }
}
