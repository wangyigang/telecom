package com.wangyg.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommonDimension implements WritableComparable<CommonDimension> {
    private Contactdimension contactdimension = new Contactdimension();
    private DateDimension dateDimension = new DateDimension();

    public CommonDimension() {
    }

    public Contactdimension getContactdimension() {
        return contactdimension;
    }

    public void setContactdimension(Contactdimension contactdimension) {
        this.contactdimension = contactdimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    //先比较联系人是否相同--先比较联系人，在比较
//    @Override
//    public int compareTo(CommonDimension o) {
//        int compare = this.contactdimension.compareTo(o.contactdimension);
//        if(compare ==0){
//            compare = this.dateDimension.compareTo(o.dateDimension);
//        }
//        return compare;
//    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.contactdimension.write(out);
        this.dateDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.contactdimension.readFields(in);
        this.dateDimension.readFields(in);
    }

    @Override
    public int compareTo(CommonDimension o) {
        int result;

        //先比较联系人是否相同
        result = this.contactdimension.compareTo(o.contactdimension);

        if (result == 0) {

            //比较时间维度
            result = this.dateDimension.compareTo(o.dateDimension);
        }
        return result;
    }
}
