package com.tosin.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

public class HBaseDao {
    private static String quorum = PropertiesUtil.getProperty("hbase.zookeeper.quorum");
    private static String clientPort = PropertiesUtil.getProperty("hbase.zookeeper.property.clientport");
    private static String nameSpace = PropertiesUtil.getProperty("hbase.namespace");
    private static String tableName = PropertiesUtil.getProperty("hbase.tablename");
    private static String columnFamily = PropertiesUtil.getProperty("hbase.columnfamily");
    private static Integer regions = Integer.parseInt(PropertiesUtil.getProperty("hbase.regions"));

    /**
     * 判断表是否存在
     *  1.初始化命名空间
     *  2.创建表
     * @param args
     */
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.property.clientport", clientPort);

        if(!HBaseUtil.isExistTable(conf, tableName)){
//            HBaseUtil.initNameSapce(conf, nameSpace);
            HBaseUtil.createRegionsTable(conf, tableName, regions, columnFamily);
        }

    }
}
