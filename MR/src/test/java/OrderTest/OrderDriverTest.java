package OrderTest;

import com.atyp.mr.order.OrderGroupingComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriverTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\WorkSpace\\IdeaWorpace\\hadoop_input_output\\input_07_order", "D:\\WorkSpace\\IdeaWorpace\\hadoop_input_output\\output_07_orderTest"};

        // 1.获取配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置Jar包加载路径
        job.setJarByClass(OrderDriverTest.class);

        // 3.加载map/reduce类
        job.setMapperClass(OrderMapperTest.class);
        job.setReducerClass(OrderReducerTest.class);

        // 4.加载map输出数据key和value类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(OrderBeanTest.class);

        // 5.设置最终输出数据的key和value类型
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(OrderBeanTest.class);

        // 6.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8.设置reducer端的分组
        //job.setGroupingComparatorClass(OrderGroupingComparator.class);

        // 7.提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
