package com.wangyg.constant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseConstant {
    /**
     *     conf.addResource("hbase-default.xml");
     *     conf.addResource("hbase-site.xml");
     */
    public static  final Configuration CONFIGURATION=HBaseConfiguration.create(); //加载两个配置信息
}
