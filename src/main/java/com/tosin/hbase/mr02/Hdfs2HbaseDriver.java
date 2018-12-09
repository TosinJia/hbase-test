package com.tosin.hbase.mr02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Hdfs2HbaseDriver implements Tool {
    private Configuration conf;
    public void setConf(Configuration configuration) {
        try {
//            Connection connection = ConnectionFactory.createConnection(configuration);
            conf = HBaseConfiguration.create(configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Configuration getConf() {
        return conf;
    }

    public int run(String[] strings) throws Exception {
        //1. 创建job
        Job job = Job.getInstance(conf);
        job.setJarByClass(Hdfs2HbaseDriver.class);
        //2. 配置mapper
        job.setMapperClass(ReadFromHdfsMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        //3. 配置reducer
        TableMapReduceUtil.initTableReducerJob("user_info_mr04", WriteHbaseReducer.class, job);
        //4. 配置inputformat
        FileInputFormat.addInputPath(job, new Path("/hbase/mr/demo02"));
        //5. 输出
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args){
        try {
            int ret = ToolRunner.run(new Hdfs2HbaseDriver(), args);
            System.out.println(ret);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
