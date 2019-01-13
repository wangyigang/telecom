package com.wangyg.mapreduce;

import com.wangyg.bean.CommonDimension;
import com.wangyg.output.MysqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CountDurationDriver extends Configuration implements Tool {
    private Configuration configuration;


    @Override
    public int run(String[] args) throws Exception {
        //设置job对象
        Job job = Job.getInstance(configuration);

        //设置jar包
        job.setJarByClass(CountDurationDriver.class);
        //设置mapper
        TableMapReduceUtil.initTableMapperJob(args[0],new Scan(),
                CountDurationMapper.class,
                CommonDimension.class,
                Text.class,
                job);
        //设置reducer
        job.setReducerClass(CountDurationReducer.class);
        //这是outputformat
        job.setOutputFormatClass(MysqlOutputFormat.class);
        //不需要设置输入输出路径

        //提交数据
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        return b?0:1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }


    public static void main(String[] args) {
        //获取配置信息
        Configuration conf = HBaseConfiguration.create();

        try {
            int run = ToolRunner.run(conf, new CountDurationDriver(),
                    args);

            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
