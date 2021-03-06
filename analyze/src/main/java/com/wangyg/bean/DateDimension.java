package com.wangyg.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 日期维度
 * 记录日期： 年-月- 日
 */

//实现序列化和课比较
public class DateDimension extends BaseDimension {
    private String year;
    private String month;
    private String date;

    public DateDimension() {
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return year + "\t" + month + "\t" + date;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.year);
        out.writeUTF(this.month);
        out.writeUTF(this.date);
    }

    //序列化，反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readUTF();
        this.month = in.readUTF();
        this.date = in.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        DateDimension o1 = (DateDimension) o;
        int compare = this.year.compareTo(o1.year);
        if (compare == 0) {
            compare = this.month.compareTo(o1.month);
            if (compare == 0) {
                compare = this.date.compareTo(o1.date);
            }
        }
        return compare;
    }
}
