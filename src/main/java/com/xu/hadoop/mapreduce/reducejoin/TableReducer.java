package com.xu.hadoop.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 存储订单的集合
        List<TableBean> orderBeans = new ArrayList<>();
        // 存储产品的bean对象
        TableBean pdBean = new TableBean();

        for (TableBean bean: values) {
            if ("0".equals(bean.getFlag())){//订单表
                TableBean orderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orderBean, bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(orderBean);
            }else {//产品表
                try {
                    BeanUtils.copyProperties(pdBean, bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        // 表的拼接
        for (TableBean bean: orderBeans) {
            bean.setPname(pdBean.getPname());
            context.write(bean, NullWritable.get());
        }
    }
}
