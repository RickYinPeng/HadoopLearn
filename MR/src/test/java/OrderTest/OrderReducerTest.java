package OrderTest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducerTest extends Reducer<LongWritable, OrderBeanTest, LongWritable, OrderBeanTest> {
    @Override
    protected void reduce(LongWritable key, Iterable<OrderBeanTest> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        System.out.println("----------------");
        System.out.println("key:"+key.toString());

        for (OrderBeanTest value : values) {
            sb.append(value.toString()+"--");
            context.write(key, value);
        }
        System.out.println("values:"+sb);
    }
}
