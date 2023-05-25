package table;

public class Supplier {
    public Long s_suppkey;
    public String s_name;
    public String s_address;
    public Long s_nationkey;
    public String s_phone;
    public Double s_acctbal;
    public String s_comment;
    public String operator;

    public Supplier(Long s_suppkey, String s_name, String s_address, Long s_nationkey, String s_phone, Double s_acctbal,String s_comment, String operator ){
        this.s_suppkey = s_suppkey;
        this.s_name = s_name;
        this.s_address = s_address;
        this.s_nationkey = s_nationkey;
        this.s_phone = s_phone;
        this.s_acctbal = s_acctbal;
        this.s_comment = s_comment;
        this.operator = operator;
    }
    public Supplier() {}


    @Override
    public String toString() {
        return "Supplier{" +
                "operator=" + operator +
                ",S_SUPPKEY=" + s_suppkey +
                ",S_NATIONKEY=" + s_nationkey +
                '}';
    }
}
