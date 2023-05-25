package table;


import java.util.Date;

public class Lineitem {
    public Long l_orderkey;
    public Long l_partkey;
    public Long l_suppkey;
    public Integer l_linenumber;
    public Double l_quantity;
    public Double l_extendedprice;
    public Double l_discount;
    public Double l_tax;
    public String l_returnflag;
    public String l_linestatus;
    public Date l_shipdate;
    public Date l_commitdate;
    public Date l_receiptdate;
    public String l_shipinstruct;
    public String l_shipmode;
    public String l_comment;
    public String operator;

    public Lineitem(Long l_orderkey, Long l_partkey, Long l_suppkey, Integer l_linenumber, Double l_quantity, Double l_extendedprice, Double l_discount, Double l_tax, String l_returnflag, String l_linestatus, Date l_shipdate, Date l_commitdate, Date l_receiptdate, String l_shipinstruct, String l_shipmode, String l_comment,String operator) {
        this.l_orderkey = l_orderkey;
        this.l_partkey = l_partkey;
        this.l_suppkey = l_suppkey;
        this.l_linenumber = l_linenumber;
        this.l_quantity = l_quantity;
        this.l_extendedprice = l_extendedprice;
        this.l_discount = l_discount;
        this.l_tax = l_tax;
        this.l_returnflag = l_returnflag;
        this.l_linestatus = l_linestatus;
        this.l_shipdate = l_shipdate;
        this.l_commitdate = l_commitdate;
        this.l_receiptdate = l_receiptdate;
        this.l_shipinstruct = l_shipinstruct;
        this.l_shipmode = l_shipmode;
        this.l_comment = l_comment;
        this.operator = operator;
    }
    public Lineitem() {}

    @Override
    public String toString() {
        return "Lineitem{" +
                "L_ORDERKEY=" + l_orderkey +
                ", L_PARTKEY=" + l_partkey +
                ", L_EXTENDEDPRICE=" + l_extendedprice +
                ", L_DISCOUNT=" + l_discount +
                '}';
    }

}
