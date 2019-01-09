package com.wangyg.dao;

/**
 * 1.用于封装为Put数据
 * 2. rowkey的设计
 * 关于rowkey的设计，根据目前的需求，如果单按照人来进行分区，可能会造成数据倾斜，那么需要添加年 月 日 ，
 * 但是一年365天，太过散列，如果按照年的角度进行获取数据，需要365次取出数据
 * 所以按照月来分的话，按月和日 都是取一次即可，按照年12次即可
 */
public class HBaseDao {

    //获取分区号--分区号的设计包含了电话号码和年月日
    //187123456789--位数太多，区后滋味
    //2019-01-01:只取出年和月
    public static String genPartitionNumber(String phone, String callTime, int regoin) {

        String phonesubstring = phone.substring(phone.length() - 4);
        String calltimeSubstring = callTime.replaceAll("-", "").substring(0, 6);

        //返回两个值异或或相乘 后模除
        int i = Integer.parseInt(phonesubstring) * Integer.parseInt(calltimeSubstring) % regoin;
        return i + "";
    }


    /**
     *  获取rowkey,第一个参数时分区个数，第二个是主动打电话的人，低三个是打电话时间，精确到毫秒，
     *  第四个被打电话的人，第五个是持续的时间
     * @param splitnum
     * @param call1
     * @param calltime
     * @param call2
     * @param duration
     * @return
     */
    public static String getRowKey(String splitnum, String call1, String calltime, String call2, String duration) {
        return splitnum+"_"+
        call1+"_"+
        calltime+"_"+
        call2+"_"+
        duration;
    }
}
