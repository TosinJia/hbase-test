package com.tosin.hbase.mr02;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadFromHdfsMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1. 读取数据
        String line = value.toString();
        //2. 切分数据
        String[] cells = line.split("\t");
        //3. 封装数据
        String rowkey = cells[0];
        String name = cells[1];
        String age = cells[2];
        String province = cells[3];
        String city = cells[4];
        //封装Put
        String family1 = "basic";
        String family2 = "address";
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("age"), Bytes.toBytes(age));
        put.addColumn(Bytes.toBytes(family2), Bytes.toBytes("province"), Bytes.toBytes(province));
        put.addColumn(Bytes.toBytes(family2), Bytes.toBytes("city"), Bytes.toBytes(city));
        //4. 输出到Reducer端
        context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put);
    }
}
