package com.tosin.hbase.mr01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FilterDataDriver implements Tool {
    private Configuration conf;
    public void setConf(Configuration configuration) {
        conf = HBaseConfiguration.create(configuration);
    }
    public Configuration getConf() {
        return this.conf;
    }

    //业务逻辑
    public int run(String[] strings) throws Exception {
        //1. 实例化job
        Job job = Job.getInstance(conf);
        // 指定主类
        job.setJarByClass(FilterDataDriver.class);
        //2. 配置Mapper
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob("user_info_mr02",
                scan,
                FilterDataMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);

        //3. 配置Reducer
        //4. 配置inputformat
        //5. 输出
        TableMapReduceUtil.initTableReducerJob("user_info_mr03",
                FilterDataReducer.class,
                job);

        //设置reducetask个数
        job.setNumReduceTasks(1);
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args){
        try {
            int status = ToolRunner.run(new FilterDataDriver(), args);
            System.out.println(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
