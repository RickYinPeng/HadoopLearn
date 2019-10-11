package OrderTest;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBeanTest implements WritableComparable<OrderBeanTest> {

    // 订单id
    private int order_id;
    // 订单价格
    private double price;

    public OrderBeanTest() {
    }

    public OrderBeanTest(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBeanTest o) {
        // 先按照订单id排序，如果相同按照价格降序排序
        int result = 0;
        if (order_id > o.getOrder_id()) {
            result = 1;
        } else if (order_id < o.getOrder_id()) {
            result = -1;
        } else {
            result = price > o.getPrice() ? -1 : 1;
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(order_id);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        order_id = in.readInt();
        price = in.readDouble();
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return order_id + "\t" + price;
    }
}
