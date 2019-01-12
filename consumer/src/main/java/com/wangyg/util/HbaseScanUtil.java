package com.wangyg.util;

import com.wangyg.dao.HBaseDao;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

public class HbaseScanUtil {

    /**
     * 获取分区键：返回一个集合，集合中数据保存的是每个分区键，
     * @param phone
     * @param startRow
     * @param stopRow
     * @return
     * @throws ParseException
     */
    public static ArrayList<String[]> getSplitRow(String phone, String startRow, String stopRow) throws ParseException {
        ArrayList<String[]> resultList = new ArrayList<>();


        //获取格式化
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM");

        //获取日历类对象
        Calendar startCalendar = Calendar.getInstance();//获取日历类对象
        Calendar stopCalender = Calendar.getInstance();

        //给日历类设置具体日志
        startCalendar.setTime(simpleDateFormat.parse(startRow));
        stopCalender.setTime(simpleDateFormat.parse(stopRow));
        //region-- 在外面获取分区键比较合适，不要要在循环内进行重复获取
        int region = Integer.parseInt(PropertiesUtil.getPropertiesValue("hbase.regions"));
        //获取具体时间，进行比较
        while(startCalendar.getTimeInMillis()<= stopCalender.getTimeInMillis()){
            Date time = startCalendar.getTime();
            String startFormat = simpleDateFormat.format(time);

            String number = HBaseDao.genPartitionNumber(phone, startFormat, region);
            //调用获取分区键函数
            String partitionNumber = number+"_"+phone+"_"+startFormat;
            String endPartionNum = partitionNumber+"|";  //结束建加一个|

            String[] partitionArr = new String[]{partitionNumber, endPartionNum};
            resultList.add(partitionArr);

            //最后需要日历类自增，否则会陷入死循环
            startCalendar.add(Calendar.MONTH, 1);
        }
        return resultList;
    }


}
