package com.wangyg.mapreduce;

import com.wangyg.bean.CommonDimension;
import com.wangyg.bean.Contactdimension;
import com.wangyg.bean.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;

public class CountDurationMapper extends TableMapper<CommonDimension, Text> {
    //将输出结果的对象先创建出来，避免后面重复创建
    //总维度表
    private CommonDimension commonDimension = new CommonDimension();
    private Text v = new Text();
    //commonDimension由两个类属性对象构成，所以需要在创建出来
    //时间维度表
    private DateDimension dateDimension = new DateDimension();
    //联系人维度
    private Contactdimension contactdimension = new Contactdimension();
    //
    private  HashMap<String, String> contacts = null;

    //使用setup的目的是为了获取手机号对应的姓名，数据是由map进行产生的
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        contacts = new HashMap<>();
        contacts.put("15369468720", "李雁");
        contacts.put("19920860202", "卫艺");
        contacts.put("18411925860", "仰莉");
        contacts.put("14473548449", "陶欣悦");
        contacts.put("18749966182", "施梅梅");
        contacts.put("19379884788", "金虹霖");
        contacts.put("19335715448", "魏明艳");
        contacts.put("18503558939", "华贞");
        contacts.put("13407209608", "华啟倩");
        contacts.put("15596505995", "仲采绿");
        contacts.put("17519874292", "卫丹");
        contacts.put("15178485516", "戚丽红");
        contacts.put("19877232369", "何翠柔");
        contacts.put("18706287692", "钱溶艳");
        contacts.put("18944239644", "钱琳");
        contacts.put("17325302007", "缪静欣");
        contacts.put("18839074540", "焦秋菊");
        contacts.put("19879419704", "吕访琴");
        contacts.put("16480981069", "沈丹");
        contacts.put("18674257265", "褚美丽");
        contacts.put("18302820904", "孙怡");
        contacts.put("15133295266", "许婵");
        contacts.put("17868457605", "曹红恋");
        contacts.put("15490732767", "吕柔");
        contacts.put("15064972307", "冯怜云");
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取rowkey
        String rowKey = Bytes.toString(key.get());

        //截取字段：0_13412341234_2019-01-01 12:12:12_15698769876_0_0123
        String[] splits = rowKey.split("_");

        String call1 = splits[1];//联系人
        String buildTime = splits[2];
        String year = buildTime.substring(0, 4);
        String month = buildTime.substring(5, 7);
        String day = buildTime.substring(8, 10);
        String duration = splits[5];

        //封装Value
        v.set(duration);

        //封装联系人维度
        contactdimension.setPhone(call1);
        contactdimension.setName(contacts.get(call1));

        //封装时间维度(day)
        dateDimension.setYear(year);
        dateDimension.setMonth(month);
        dateDimension.setDate(day);

        //封装总维度
        commonDimension.setContactdimension(contactdimension);
        commonDimension.setDateDimension(dateDimension);

        //写出day维度
        context.write(commonDimension, v);

        //封装月维度
        dateDimension.setDate("-1");
        commonDimension.setDateDimension(dateDimension);
        context.write(commonDimension, v);

        //封装年维度
        dateDimension.setMonth("-1");
        commonDimension.setDateDimension(dateDimension);
        context.write(commonDimension, v);
    }
}
