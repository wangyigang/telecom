package com.wangyg.dao;

import com.wangyg.constant.HbaseConstant;
import com.wangyg.util.HbaseUtil;
import com.wangyg.util.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 1.用于封装为Put数据
 * 2. rowkey的设计
 * 关于rowkey的设计，根据目前的需求，如果单按照人来进行分区，可能会造成数据倾斜，那么需要添加年 月 日 ，
 * 但是一年365天，太过散列，如果按照年的角度进行获取数据，需要365次取出数据
 * 所以按照月来分的话，按月和日 都是取一次即可，按照年12次即可
 */
public class HBaseDao {
    private Connection connection = null; //连接
    private Table table = null; //表
    private List<Put> putList;
    private String tableName;
    private String columnFamily; //列族
    private String namespace; //名称空间

    /**
     * 默认构造器：对属性进行初始化
     */
    public HBaseDao() {

        try {
            //获取连接
            connection = ConnectionFactory.createConnection(HbaseConstant.CONFIGURATION);
            //talename
            tableName = PropertiesUtil.getPropertiesValue("hbase.table.name");
            table = connection.getTable(TableName.valueOf(tableName));
            //获取名称空间
            namespace = PropertiesUtil.getPropertiesValue("hbase.namespace");
            //获取列族
            columnFamily = PropertiesUtil.getPropertiesValue("hbase.columnFamily");
            //创建名称空间
            HbaseUtil.createNamespace(namespace);
            //创建表
            HbaseUtil.createTable(tableName, columnFamily);//表名和列名
            //创建集合容器
            putList = new ArrayList<>();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取分区号--分区号的设计包含了电话号码和年月日
    //187123456789--位数太多，区后滋味
    //2019-01-01:只取出年和月
    public static String genPartitionNumber(String phone, String callTime, int regoin) {

        String phonesubstring = phone.substring(phone.length() - 4);
        String calltimeSubstring = callTime.replaceAll("-", "").substring(0, 6);

        //返回两个值异或或相乘 后模除--2019.1.10发现相乘有数据倾斜情况，采用^异或方式处理
        int i = (Integer.parseInt(phonesubstring) ^ Integer.parseInt(calltimeSubstring)) % regoin;
        return i + "";
    }


    /**
     * 获取rowkey,第一个参数时分区个数，第二个是主动打电话的人，低三个是打电话时间，精确到毫秒，
     * 第四个被打电话的人，第五个是持续的时间
     *
     * @param splitnum
     * @param call1
     * @param calltime
     * @param call2
     * @param duration
     * @return
     */
    public static String getRowKey(String splitnum, String call1, String calltime, String call2,String flag ,String duration) {
        return splitnum + "_" +
                call1 + "_" +
                calltime + "_" +
                call2 + "_" +
                flag+"_"+
                duration;
    }

    /**
     * 批量处理put数据--15596505995,19920860202,2017-07-31 01:39:26,0326
     * 现在我们要使用HBase查找数据时，尽可能的使用rowKey去精准的定位数据位置，而非使用ColumnValueFilter
     * 或者SingleColumnValueFilter，按照单元格Cell中的Value过滤数据，这样做在数据量巨大的情况下，
     * 效率是极低的——如果要涉及到全表扫描。所以尽量不要做这样可怕的事情。注意，这并非ColumnValueFilter
     * 就无用武之地。现在，我们将使用协处理器，将数据一分为二。
     *
     * 数据消费时，使用过滤器意味着要进行全表扫描，过滤掉非法数据，可以将rowkey重新设计，拆分成两个，并使用标记位0 、1
     * 记录谁打给谁
     *
     * 这时意味着要put 插入两条数据，可以进行直接put  或者使用协处理器：协处理器在put 之后，自动调用postput,
     * 协处理器的添加在创建表的时候进行添加，表描述器，htabledecriptor
     *
     * * @param value
     */
    public void putBatch(String value) {
        //获取到表数据后，进行封装put,然后放入到putlists中，如果达到一个一个阈值，就批量


        //获取rowkey
        String[] split = value.split(",");
        //防御性编程
        if (split.length < 4) {
            return;
        }
        //对rowkey进行分割，获取对应数据
        String call1 = split[0];
        String call2 = split[1];
        String buildTime = split[2];
        String duration = split[3];

        //获取分区个数
        String regions = PropertiesUtil.getPropertiesValue("hbase.regions");
        //获取分区键
        String partitionkeys = HBaseDao.genPartitionNumber(call1, buildTime, Integer.parseInt(regions));
        //通过参数获取rowkey
        String rowKey = HBaseDao.getRowKey(partitionkeys, call1, buildTime, call2, 0+"",duration);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("call1"), Bytes.toBytes(call1));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("call2"), Bytes.toBytes(call2));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //将数据放入在集合容器中
        putList.add(put);

        //判断集合大小个数，超过限制，批量进行提交
        if(putList.size()>=20){
            //
            try {
                table.put(putList);

                //提交数据完成后，将容器进行清空
                putList.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
