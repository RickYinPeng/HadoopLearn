package OrderTest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapperTest extends Mapper<LongWritable, Text, LongWritable, OrderBeanTest> {
    LongWritable k = new LongWritable();
    OrderBeanTest v = new OrderBeanTest();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //000000001 222.8
        // 1.获取一行数据
        String line = value.toString();

        // 2.切割
        String[] fields = line.split("\t");

        // 3.封装对象
        k.set(Long.parseLong(fields[0]));
        v.setOrder_id(Integer.parseInt(fields[0]));
        v.setPrice(Double.parseDouble(fields[2]));

        // 4.写出
        context.write(k,v);
    }
}
