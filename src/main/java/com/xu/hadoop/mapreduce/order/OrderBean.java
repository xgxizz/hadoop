package com.xu.hadoop.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description
 * @Author xgx
 * @Date 2019/11/29 14:26
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private int orderNo;
    private double price;

    public int getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(int orderNo) {
        this.orderNo = orderNo;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public OrderBean() {
        super();
    }

    public OrderBean(int orderNo, double price) {
        this.orderNo = orderNo;
        this.price = price;
    }

    @Override
    public String toString() {
        return this.orderNo + "\t" + this.price;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderNo);
        dataOutput.writeDouble(price);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.orderNo = dataInput.readInt();
        this.price = dataInput.readDouble();
    }

    public int compareTo(OrderBean o) {
        if (this.orderNo - o.orderNo > 0){
            return 1;
        }else if (this.orderNo - o.orderNo <0){
            return -1;
        }else {
            return this.price - o.price > 0 ? -1:1;
        }
    }
}
