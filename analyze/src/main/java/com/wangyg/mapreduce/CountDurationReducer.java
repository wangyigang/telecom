package com.wangyg.mapreduce;

import com.wangyg.bean.CommonDimension;
import com.wangyg.bean.CountAndDuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


//reduce阶段
//reduce阶段输出的类型是两个维度的数据 还有 两个结果数据，所以将v的恋歌结果数据再次进行封装
public class CountDurationReducer extends Reducer<CommonDimension, Text,CommonDimension, CountAndDuration> {
    //value对象--提前声明
    private CountAndDuration countAndDuration = new CountAndDuration();

    //reduce阶段进行汇总计算
    @Override
    protected void reduce(CommonDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //对数据进行汇总--相同类型数据进行汇总--汇总次数和计算通话时长

        int callCount =0;
        int callDuration =0;

        for (Text value : values) {
            ++callCount;
            callDuration+= Integer.parseInt(value.toString());
        }

        //对象进行初始化
        countAndDuration.setCallCount(callCount);
        countAndDuration.setCallDuration(callDuration);

        //将数据进行写出
        context.write(key, countAndDuration);
        //将数据写出到outputFormat--因为数据需要写出到mysql中，所以需要自定义outputformat
    }
}
