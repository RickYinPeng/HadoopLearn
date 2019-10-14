package com.atyp.mr.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    String name;
    TableBean tableBean = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取文件名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();

        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        if (name.startsWith("order")) {
            // 订单表
            String[] fields = line.split("\t");

            // 封装 key 和 value
            tableBean.setId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            // 没有不能为空，否则会报错
            tableBean.setPname("");
            tableBean.setFlag("order");

            k.set(fields[1]);
        } else {
            //产品表
            String[] fiedls = line.split("\t");

            tableBean.setId("");
            tableBean.setPid(fiedls[0]);
            tableBean.setAmount(0);
            tableBean.setPname(fiedls[1]);
            tableBean.setFlag("pd");

            k.set(fiedls[0]);
        }
        context.write(k, tableBean);
    }
}
