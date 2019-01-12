package com.wangyg.util;

import com.wangyg.constant.HbaseConstant;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 主要用于相关操作的封装，创建名称空间， 创建表，
 * 设置分区键，rowkey等
 */
public class HbaseUtil {

    /**
     * 创建命名空间
     */
    public static void createNamespace(String namespace) {

        //获取连接
        try {
            //通过constant常量方式获取conf文件
            Connection connection = ConnectionFactory.createConnection(HbaseConstant.CONFIGURATION);

            Admin admin = connection.getAdmin();
            String ns = PropertiesUtil.getPropertiesValue(namespace);
            //使用try..catch方式获取数据
            try {
                NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
                //创建名称空间
                admin.createNamespace(namespaceDescriptor);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("命名空间已存在...");
                admin.close();
                connection.close();
                return ;
            }


//            try {
//                admin.getNamespaceDescriptor(ns);
//
//            } catch (NamespaceNotFoundException e) {
//                //使用namespaceDescriptor描述器.create().build()方法
//            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
////                //创建名称空间
//                admin.createNamespace(namespaceDescriptor);
//
//
//                //进行关闭资源
//                admin.close();
//                connection.close();
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //
    }

    //创建表
    public static void createTable(String tableName, String... columnFamily) {
        try {
            //获取连接
            Connection connection = ConnectionFactory.createConnection(HbaseConstant.CONFIGURATION);
            //获取admin对象

            Admin admin = connection.getAdmin();
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //for循环遍历添加列族
            for (String column : columnFamily) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes(column));
                tableDescriptor.addFamily(columnDescriptor);
            }

            //需要先判断表是否存在---如果表不存在，不会进行创建
            if(isExistsTable(tableName)){
                System.out.println(tableName+"表已存在");
                return ;
            }

            //首先通过配置文件获取分区个数
            String regionNum = PropertiesUtil.getPropertiesValue("hbase.regions");
            byte[][] splitkey = getSplitKeys(Integer.parseInt(regionNum));

//            //设置协处理器
          tableDescriptor.addCoprocessor("com.wangyg.coprocesor.CalleeWriteObserver");

            //创建表--第一个参数HtableDescriptor 第二个参数split key：切分的键
            admin.createTable(tableDescriptor, splitkey);
            //输出到控制台，进行提示
            System.out.println(tableName+"表创建完成...");

            //进行关闭资源
            admin.close();
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(tableName+"表创建失败...");
        }
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     */
    private static boolean isExistsTable(String tableName) {
        try {
            //获取连接
            Connection connection = ConnectionFactory.createConnection(HbaseConstant.CONFIGURATION);
            //获取admin管理对象
            Admin admin = connection.getAdmin();

            //tableName
            TableName tn = TableName.valueOf(tableName);

            boolean b = admin.tableExists(tn);


            //先进行释放资源
            admin.close();
            connection.close();

            //返回结果
            return b;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 设置与分区键：0| 1| 2| 3| 4| 5| 6|
     * @param numregion
     * @return
     */
    private static byte[][] getSplitKeys(int numregion) {
        //
        byte[][] ret = new byte[numregion][];
        for (int i = 0; i < numregion; i++) {
            ret[i] = Bytes.toBytes(i+"|");
        }
        return ret;
    }
}
