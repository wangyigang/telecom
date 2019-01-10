package com.wangyg.coprocesor;

import com.wangyg.constant.HbaseConstant;
import com.wangyg.dao.HBaseDao;
import com.wangyg.util.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CalleeWriteObserver extends BaseRegionObserver {
    private String columnFamily = PropertiesUtil.getPropertiesValue("hbase.columnFamily");

    /**
     * 重写postput方法，将数据进行调换位置，重新计算，然后进行put操作
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //首先获取要操作的表，判断是否和现有表相同，防御性编程
        String tableName = PropertiesUtil.getPropertiesValue("hbase.table.name");

        //获取现有的表
        TableName table = e.getEnvironment().getRegionInfo().getTable();
        String nameAsString = table.getNameAsString();

        //如果名称不相同
        if(!tableName.equals(nameAsString)){
            return ; //防御性编程
        }



        //获取数据
        String rowkey = Bytes.toString(put.getRow());
        //切分---         splitnum + "_" +
        //                call1 + "_" +
        //                calltime + "_" +
        //                call2 + "_" +
        //                flag+"_"+
        //                duration;
        String[] split = rowkey.split("_");//以_下划线进行切割
        String call1 = split[1];
        String call2=split[3];
        String buildTime =split[2];
        String duration = split[5];


        //防止无线循环递归情况产生
        if("1".equals(split[4])){
            return ; //如果已经发生改变就结束，不会再进行put操作，防止递归情况产生
        }


        //获取分区个数
        int region = Integer.parseInt(PropertiesUtil.getPropertiesValue("hbase.regions"));
        //计算分区键
        String newkey = HBaseDao.genPartitionNumber(call2, buildTime, region);
        //计算新的分区
        String newRowKey = HBaseDao.getRowKey(newkey, call2, buildTime, call1, "1", duration);

        //封装put对象
        Put newPut = new Put(Bytes.toBytes(newRowKey));
        newPut.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("call1"), Bytes.toBytes(call2));
        newPut.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("call2"), Bytes.toBytes(call1));
        newPut.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        newPut.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //获取连接
        Connection connection = ConnectionFactory.createConnection(HbaseConstant.CONFIGURATION);
        Table connectionTable = connection.getTable(table);
        //提交put数据
        connectionTable.put(newPut);

        //关闭资源
        connectionTable.close();
        connection.close();

    }
}
