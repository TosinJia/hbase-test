package com.tosin.hbase;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseTest {
    private static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
    }

    //1. 判断一张表是否存在
    //hbase(main):003:0> list 'user_info_s'
    public static boolean isExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }
    //2. 创建表
    //hbase(main):004:0> create 'user_info_s','basic','address'
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
        // 判断表是否存在
        if(isExist(tableName)){
            System.out.println("表" + tableName + "已存在!");
        }else{
            // 创建表描述器
            HTableDescriptor htd =  new HTableDescriptor(tableName);

            //创建列描述器
            for(String cf: columnFamily){
                HColumnDescriptor hcd = new HColumnDescriptor(cf);
                htd.addFamily(hcd);
            }
            //创建表
            admin.createTable(htd);
            System.out.println("表" + tableName + "已创建成功!");
        }
    }

    //3. 删除表
    //hbase(main):005:0> disable 'user_info_s'
    //hbase(main):006:0> drop 'user_info_s'
    public static void deleteTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin hBaseAdmin = (HBaseAdmin) connection.getAdmin();

        if(isExist(tableName)){
            // 禁用表
            hBaseAdmin.disableTable(TableName.valueOf(tableName));
            // 删除表
            hBaseAdmin.deleteTable(TableName.valueOf(tableName));
        }else{
            System.out.println("表" + tableName + "不存在!");
        }
    }

    //4. 添加数据
    //hbase(main):010:0> put 'user_info_s','100','basic:name','tosin'
    public static void putData(String tableName, String rowKey, String columnFamily, String qualifier, String value) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        //1. 用put方式添加数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //2. 添加列族、列、值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put);
    }

    //5. 删除表中一行数据
    //hbase(main):015:0> deleteall 'user_info','100'
    //hbase(main):013:0> delete 'user_info','100','basic:age'
    public static void deleteData(String tableName, String rowKey, String columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 根据rowkey删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 删除
        table.delete(delete);
    }

    //6. 删除多行数据
    public static void deleteMutilData(String tableName, String... rowKeys) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        List<Delete> list = new ArrayList<Delete>();
        for(String rowKey: rowKeys){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            list.add(delete);
        }
        table.delete(list);
    }

    //7. 扫描表数据
    //hbase(main):023:0> scan 'user_info'
    public static void scanAll(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        //1. 实例化Scan
        Scan scan = new Scan();
        //2. 获取Scanner
        ResultScanner rs = table.getScanner(scan);
        //3. 遍历
        StringBuffer sb = new StringBuffer();
        for(Result r: rs){
            Cell[] cells = r.rawCells();
            // 遍历具体数据
            for(Cell c: cells){
                sb.setLength(0);
                sb.append("行键：" + Bytes.toString(CellUtil.cloneRow(c))).append("\t");
                sb.append("列族：" + Bytes.toString(CellUtil.cloneFamily(c))).append("\t");
                sb.append("列：" + Bytes.toString(CellUtil.cloneQualifier(c))).append("\t");
                sb.append("值：" + Bytes.toString(CellUtil.cloneValue(c)));
                System.out.println(sb);
            }
            System.out.println("====================");
        }
    }

    //8. 扫描指定行键的数据
    //hbase(main):025:0> scan 'user_info',{STARTROW => '100',STOPROW=>'100'}
    //hbase(main):026:0> get 'user_info','100'
    //hbase(main):027:0> get 'user_info','100','basic:age'
    public static void getRow(String tableName, String rowKey, String family) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        // 可加过滤条件 列族
        get.addFamily(Bytes.toBytes(family));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        // 遍历具体数据
        StringBuffer sb = new StringBuffer();
        for(Cell cell: cells){
            sb.setLength(0);
            sb.append("行键：" + Bytes.toString(CellUtil.cloneRow(cell))).append("\t");
            sb.append("列族：" + Bytes.toString(CellUtil.cloneFamily(cell))).append("\t");
            sb.append("列：" + Bytes.toString(CellUtil.cloneQualifier(cell))).append("\t");
            sb.append("值：" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println(sb);
        }
    }
    /**
     * user_info
     *              basic               address
     * rowkey       name    age         province    city
     * **/
    public static void main(String[] args) throws IOException {
        String tableName = "user_info";
        boolean b = isExist(tableName);
        System.out.println(b);

        tableName = "user_info";
        String family1 = "basic";
        String family2 = "address";
        createTable(tableName, family1, family2);

        //deleteTable(tableName);
        //putData(tableName, "100", family1, "name", "tosin");
        //putData(tableName, "100", family1, "age", "18");
        //putData(tableName, "100",family2, "province", "陕西");
        //putData(tableName, "100",family2, "city", "西安");
        //putData(tableName, "101",family1, "name", "jacky");
        //putData(tableName, "101",family1, "age", "20");
        //putData(tableName, "101",family2, "province", "北京");
        //putData(tableName, "101",family2, "city", "朝阳");

        //deleteData(tableName, "101", family1);
        //deleteMutilData(tableName, "101", "100");

        //scanAll(tableName);
        getRow(tableName, "100", "basic");
    }

}
