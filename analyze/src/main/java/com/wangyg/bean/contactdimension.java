package com.wangyg.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class contactdimension implements WritableComparable<contactdimension> {
    private String phone;
    private String name;

    public contactdimension() {
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return phone +"\t" + name;
    }

    @Override
    public int compareTo(contactdimension o) {
       int compare = this.phone.compareTo(o.phone);
       return compare;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeUTF(name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.name = in.readUTF();
    }
}
