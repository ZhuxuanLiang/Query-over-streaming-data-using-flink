package table;

public class Part {
    public Long p_partkey;
    public String p_name;
    public String p_mfgr;
    public String p_brand;
    public String p_type;
    public Integer p_size;
    public String p_container;
    public Double p_retailprice;
    public String p_comment;
    public String operator;
    public Part(Long p_partkey, String p_name, String p_mfgr, String  p_brand, String p_type, Integer  p_size, String p_container,
    Double p_retailprice, String p_comment, String operator ){
        this.p_partkey = p_partkey;
        this.p_name = p_name;
        this.p_mfgr = p_mfgr;
        this.p_brand = p_brand;
        this.p_type = p_type;
        this.p_size = p_size;
        this.p_container = p_container;
        this.p_retailprice = p_retailprice;
        this.p_comment = p_comment;
        this.operator = operator;
    }
    public Part(){}

    @Override
    public String toString() {
        return "Part{" +
                "P_PARTKEY=" + p_partkey +
                ",P_TYPE='" + p_type + '\'' +
                '}';
    }
}

