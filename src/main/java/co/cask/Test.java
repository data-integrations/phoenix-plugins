package co.cask;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
//import org.apache.phoenix.mapreduce.bean.StockBean;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;

public class Test {
//    final Configuration conf = new Configuration();
//         HBaseConfiguration.addHbaseResources(conf);
//         conf.set(HConstants.ZOOKEEPER_QUORUM, zkUrl);
//    final Job job = Job.getInstance(conf, "stock-stats-job");
//    final String selectQuery = "SELECT STOCK_NAME,RECORDING_YEAR,RECORDINGS_QUARTER FROM STOCKS ";

    public void test() {
//         PhoenixMapReduceUtil.setInput(job, StockWritable.class, "STOCKS", selectQuery);
//         PhoenixMapReduceUtil.setOutput(job, "STOCKS", "STOCK_NAME,RECORDING_YEAR,RECORDINGS_AVG");
//         job.setMapperClass(StockMapper.class);
//         job.setOutputFormatClass(PhoenixOutputFormat.class);
//         job.setNumReduceTasks(0);
//         job.setOutputKeyClass(NullWritable.class);
//         job.setOutputValueClass(StockWritable.class);
//         TableMapReduceUtil.addDependencyJars(job);
    }
}
