package table;

public class Region {
    public Long r_regionkey;
    public String r_name;
    public String operator;
    public Region(Long r_regionkey, String r_name, String operator){
        this.r_regionkey = r_regionkey;
        this.r_name = r_name;
        this.operator = operator;
    }
    public Region(){}
    @Override
    public String toString() {
        return "Region{" +
                "R_REGIONKEY=" + r_regionkey +
                ",R_NAME='" + r_name + '\'' +
                ",operator='" + operator + '\'' +
                '}';
    }
}
