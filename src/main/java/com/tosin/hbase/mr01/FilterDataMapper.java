package com.tosin.hbase.mr01;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterDataMapper extends TableMapper<ImmutableBytesWritable, Put>{
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //1. 读取数据拿到rowkey
        Put put = new Put(key.get());
        //2. 遍历
        Cell[] cells = value.rawCells();
        for (Cell cell: cells) {
            //3. 过滤列族、列
            if("basic".equals(Bytes.toString(CellUtil.cloneFamily(cell)))
                    && "name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                put.add(cell);
            }
            if("address".equals(Bytes.toString(CellUtil.cloneFamily(cell)))
                    && "city".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                put.add(cell);
            }
        }
        //4. 输出到reducer阶段
        context.write(key, put);
    }
}
