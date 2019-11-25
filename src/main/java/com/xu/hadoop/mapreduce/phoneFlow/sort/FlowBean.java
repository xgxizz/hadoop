package com.xu.hadoop.mapreduce.phoneFlow.sort;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description
 * @Author xgx
 * @Date 2019/11/25 15:48
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private double up;
    private double down;
    private double sum;

    public FlowBean() {
    }

    public FlowBean(double up, double down) {
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    public double getUp() {
        return up;
    }

    public void setUp(double up) {
        this.up = up;
    }

    public double getDown() {
        return down;
    }

    public void setDown(double down) {
        this.down = down;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(up);
        dataOutput.writeDouble(down);
        dataOutput.writeDouble(sum);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.up = dataInput.readDouble();
        this.down = dataInput.readDouble();
        this.sum = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return up + "\t" + down +"\t" + sum;
    }

    @Override
    public int compareTo(FlowBean o) {
        return (int) (o.getSum() - this.sum );
    }
}
