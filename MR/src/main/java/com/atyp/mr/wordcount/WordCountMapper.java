package com.atyp.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.print.attribute.standard.JobStateReasons;
import java.io.IOException;
import java.util.HashSet;

// map阶段
// KEYIN    输入数据的key(偏移量)
// VALUEIN  输入数据的value(Text)
// KEYOUT   输出数据的key的类型(数据格式：atyp,1   ss,1)前面是key后面是value
// VALUEOUT 输出数据的value的类型
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // atyp atyp

        System.out.println(key.toString());

        // 1.获取一行
        String line = value.toString();

        // 2.切割单词
        String[] words = line.split(" ");

        // 3.循环写出
        for (String word : words) {
            // atyp
            //Text k = new Text();
            k.set(word);
            // 1
            //IntWritable v = new IntWritable();
            //v.set(1);
            context.write(k, v);
            HashSet s = new HashSet();

        }
    }
}
