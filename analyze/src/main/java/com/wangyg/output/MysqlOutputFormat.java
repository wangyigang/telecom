package com.wangyg.output;

import com.wangyg.bean.CommonDimension;
import com.wangyg.bean.CountAndDuration;
import com.wangyg.conver.DimensionConvertor;
import com.wangyg.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlOutputFormat extends OutputFormat<CommonDimension, CountAndDuration> {
    //看fileoutputformat源码，
    private FileOutputCommitter committer = null;


    @Override
    public RecordWriter<CommonDimension, CountAndDuration> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        try {
            return new MysqlRecordWriter();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 主要用于格式校验的，目前不需要,判断是否目录文件存在
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    /**
     *  这个目前不影响当前逻辑，
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    private static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null: new Path(name);
    }


    protected  static class MysqlRecordWriter extends  RecordWriter<CommonDimension, CountAndDuration>{
        private DimensionConvertor dimensionConvertor ;
        private Connection connection = null;
        //sql语句
        private String sql;
        //批量提交初始值
        private int count ;
        //批量提价阈值
        private int countBound;
        //预编译prepareStatement;
        private PreparedStatement preparedStatement;


        public MysqlRecordWriter() throws SQLException {
            //构造器中，对属性进行初始化
            dimensionConvertor = new DimensionConvertor();
            connection = JDBCUtil.getInstance();
            sql = "INSERT INTO `tb_call` VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE `call_sum`=?,`call_duration_sum`=?";

            //TODO
            preparedStatement = connection.prepareStatement(sql) ;
            count =0;
            countBound = 500;
        }

        /**
         * 真正执行write操作的类
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */

        //对write方法进行加锁--多个线程共同写lruCache缓存区时，需要
        @Override
        public synchronized void write(CommonDimension key, CountAndDuration value) throws IOException, InterruptedException {
            //与mysql进行交互--封装一个类--总表有五个字段吗，

            try {
                //根据维度信息获取维度ID--封装一个方法，既能传时间维度，联系人维度--可以用object 或封装父类
                int contactId = dimensionConvertor.getId(key.getContactdimension());
                int dataId = dimensionConvertor.getId(key.getDateDimension());

                //进行拼接主题
                String contactDateDimension = contactId+"_"+dataId; // 两个字符串进行拼接
                //去除通话次数
                int callCount = value.getCallCount();

                //获取通话时长
                int calldureation = value.getCallDuration();
                //获取通话时长
                int i=0;


                //预编译sql赋值 --第一个值是拼接而成在主键
                preparedStatement.setString(++i, contactDateDimension);
                //第二个值是dataId--插入到时间维度表后，获取到的
                preparedStatement.setInt(++i, dataId);
                preparedStatement.setInt(++i, contactId);
                preparedStatement.setInt(++i, callCount);
                preparedStatement.setInt(++i, calldureation);
                preparedStatement.setInt(++i, callCount);
                preparedStatement.setInt(++i, calldureation);

                //进行批量提交
                preparedStatement.addBatch();

                count++; //记录提交到batch中的个数
                if(count>=countBound){
                    preparedStatement.executeBatch();
                    count=0;

                    //提交
                    connection.commit();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

        /**
         * 资源关闭操作
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            //主要负责两个事情，清空缓存中的数据，关闭资源
            //提交
            try {
                preparedStatement.executeBatch();
                //进行提交数据，因为关了自动提交 setAutocommit(false)
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                JDBCUtil.clost(null, preparedStatement, connection);
            }
        }
    }
}
