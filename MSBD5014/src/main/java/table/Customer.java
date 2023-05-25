package table;

public class Customer {
    public Long c_custkey;
    public String c_name;
    public String c_address;
    public Long c_nationkey;
    public String c_phone;
    public Double c_acctbal;
    public String c_mktsegment;
    public String c_comment;
    public String operator;

    public Customer(Long c_custkey, String c_name, String c_address, Long c_nationkey, String c_phone, Double c_acctBal, String c_mktsegment, String c_comment,String operator){
        this.c_custkey = c_custkey;
        this.c_name = c_name;
        this.c_address = c_address;
        this.c_nationkey = c_nationkey;
        this.c_phone = c_phone;
        this.c_acctbal = c_acctBal;
        this.c_mktsegment = c_mktsegment;
        this.c_comment = c_comment;
        this.operator = operator;
    }
    public Customer() {}

    @Override
    public String toString() {
        return "Customer{" +
                "C_CUSTKEY=" + c_custkey +
                ", C_NATIONKEY=" + c_nationkey +
                '}';
    }

}
