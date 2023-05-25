package table;


import java.util.Date;

public class Orders {
    public Long o_orderkey;
    public Long o_custkey;
    public String o_orderstatus;
    public Double o_totalprice;
    public Date o_orderdate;
    public String o_orderpriority;
    public String o_clerk;
    public Integer o_shippriority;
    public String o_comment;
    public String operator;
    public Orders(Long o_orderkey, Long o_custkey, String o_orderstatus, Double o_totalprice, Date o_orderdate, String o_orderpriority, String o_clerk, Integer o_shippriority, String o_comment,String operator) {
        this.o_orderkey = o_orderkey;
        this.o_custkey = o_custkey;
        this.o_orderstatus = o_orderstatus;
        this.o_totalprice = o_totalprice;
        this.o_orderdate = o_orderdate;
        this.o_orderpriority = o_orderpriority;
        this.o_clerk = o_clerk;
        this.o_shippriority = o_shippriority;
        this.o_comment = o_comment;
        this.operator = operator;
    }
    public Orders() {}

    @Override
    public String toString() {
        return "Order{" +
                "O_ORDERKEY=" + o_orderkey +
                ", O_CUSTKEY=" + o_custkey +
                ", O_ORDERDATE=" + o_orderdate+
                '}';
    }
}
