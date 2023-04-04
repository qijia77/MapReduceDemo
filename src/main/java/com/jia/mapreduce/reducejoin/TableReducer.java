package com.jia.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Array;
import java.util.ArrayList;

/**
 * @author Jia Qi
 * @create 2023-02-16 15:37
 */
public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        //循环遍历
        for (TableBean value : values) {
            if ("order".equals(value.getFlag())){
                TableBean tmptablebean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmptablebean,value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
                orderBeans.add(tmptablebean);
            }else {
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }

        }
        //循环遍历
        for (TableBean orderBean : orderBeans) {
            orderBean.setFlag(pdBean.getPname());
            context.write(orderBean,NullWritable.get());
        }
    }
}
