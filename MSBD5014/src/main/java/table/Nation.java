package table;

public class Nation {
    public Long n_nationkey;
    public String n_name;
    public Long n_regionkey;
    public String n_comment;
    public String operator;

    public Nation(Long n_nationkey, String n_name, Long n_regionkey, String n_comment, String operator) {
        this.n_nationkey = n_nationkey;
        this.n_name = n_name;
        this.n_regionkey = n_regionkey;
        this.n_comment = n_comment;
        this.operator = operator;
    }
    public Nation() {}

    @Override
    public String toString() {
        return "Nation{" +
                "operator='" + operator + '\''+
                ", N_NATIONKEY=" + n_nationkey +
                ", N_NAME='" + n_name + '\'' +
                ", N_REGIONKEY=" + n_regionkey +
                '}';
    }

}
