package com.tosin.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

public class HBaseUtil {

    /**
     * 初始化命名空间
     * @param conf 配置对象
     * @param nameSpace 命名空间的名字
     * @throws IOException
     */
    public static void initNameSapce(Configuration conf, String nameSpace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        // 命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace)
                .addConfiguration("AUTHOR", "Tosin")
                .build();
        // 通过admin对象来创建命名空间
        admin.createNamespace(namespaceDescriptor);
        System.out.println("命名空间["+namespaceDescriptor+"]已创建");
        // 关闭两个对象
        close(admin, connection);
    }

    /**
     * 关闭admin对象和connection对象
     * @param admin
     * @param connection
     * @throws IOException
     */
    private static void close(Admin admin, Connection connection) throws IOException {
        if(admin != null){
            admin.close();
        }
        if(connection != null){
            connection.close();
        }
    }

    /**
     * 创建HBase的表
     * @param conf
     * @param tableName
     * @param regions
     * @param columnFamily
     * @throws IOException
     */
    public static void createRegionsTable(Configuration conf, String tableName, int regions, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //判断表
        if(isExistTable(conf, tableName)){
            return;
        }

        // 表描述器 HTableDescriptor
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for(String cf: columnFamily){
            // 列描述器 ：HColumnDescriptor
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }

        // 创建表
        admin.createTable(hTableDescriptor, genSplitKeys(regions));
        System.out.println("表["+tableName+"-"+regions+"-"+columnFamily.toString()+"]已创建");
        // 关闭对象
        close(admin, connection);
    }

    /**
     * 分区键
     * @param regions region个数
     * @return
     */
    public static byte[][] genSplitKeys(int regions) {
        // 存放分区键的数组
        String[] keys = new String[regions];
        // 格式化分区键的形式  00| 01| 02|
        DecimalFormat decimalFormat = new DecimalFormat("00");
        for(int i=0; i<regions; i++){
            keys[i] = decimalFormat.format(i) + "|";
        }
        byte[][] splitKeys = new byte[regions][];
        // 排序 保证你这个分区键是有序得
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for(int i=0; i<regions; i++){
            treeSet.add(Bytes.toBytes(keys[i]));
        }
        // 输出
        Iterator<byte[]> iterator = treeSet.iterator();
        int index = 0;
        while (iterator.hasNext()){
            byte[] next = iterator.next();
            splitKeys[index++]=next;
        }
        return splitKeys;
    }

    /**
     * 判断表是否存在
     * @param conf
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        boolean ret = admin.tableExists(TableName.valueOf(tableName));
        close(admin, connection);
        return ret;
    }
}
