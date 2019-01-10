package com.wangyg.consumer;


import com.wangyg.dao.HBaseDao;
import com.wangyg.util.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * 主要用于消费kafka中的数据，然后将数据进行持久化存入到HBase中
 */
public class HBaseConsumer {
    /**
     *  使用idea方法进行消费kafka中数据
     * @param args
     */
    public static void main(String[] args) {

        //获取配置信息
        Properties properties = PropertiesUtil.properties;
        //创建kafka消费者，用于消费flume中数据
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //进行订阅主题
        String topic = properties.getProperty("kafka.topics");

        //两种方式进行转换成collection
        consumer.subscribe(Collections.singletonList(topic));
//        consumer.subscribe(Arrays.asList(topic));
         HBaseDao hBaseDao = new HBaseDao();
        //订阅后，进行消费
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                //record是一个key,value方式的记录，直接获取value
                System.out.println(record.value());

                //TODO--将数据传入到HBase中
                hBaseDao.putBatch(record.value());
            }
        }

    }
}
