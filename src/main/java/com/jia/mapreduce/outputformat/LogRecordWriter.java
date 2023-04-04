package com.jia.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Jia Qi
 * @create 2023-02-11 18:00
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private  FSDataOutputStream atguiguOut;
    private  FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext taskAttemptContext) {
        //准备创建两条流
        try {
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
            atguiguOut = fs.create(new Path("D:\\output\\logoutput\\atguigu.log"));
            otherOut = fs.create(new Path("D:\\output\\logoutput\\other.log"));
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
        String log = key.toString();

        if (log.contains("atguigu")){
            atguiguOut.writeBytes(log+"\n");
        }else {
            otherOut.writeBytes(log+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //关流
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
