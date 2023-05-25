import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Scanner;

public class ConnectMysql {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
        final String JDBC_URL = "jdbc:mysql://localhost:3306/tpch?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        final String JDBC_USER = "root";
        final String JDBC_PASSWORD = "password";
        Class.forName(JDBC_DRIVER);
        Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
//        conn.setAutoCommit(false);

        PreparedStatement ps = null;
        ResultSet rs;
        String filePath = "/Users/phoebe/Documents/BDT/ip/myProject/mergefile/MergeFile.tbl";
        String[] drop_tables = new String[7];
        drop_tables[0] = "drop table if exists `customer`;";
        drop_tables[1] = "drop table if exists `lineitem`;";
        drop_tables[2] = "drop table if exists `orders`;";
        drop_tables[3] = "drop table if exists `nation`;";
        drop_tables[4] = "drop table if exists `region`;";
        drop_tables[5] = "drop table if exists `supplier`;";
        drop_tables[6] = "drop table if exists `part`;";
        String[] create_tables = new String[7];
        create_tables[0] = "create table if not exists `customer`(\n" +
                "`C_CUSTKEY` bigint ,\n" +
                "`C_NAME` varchar(25),\n" +
                "`C_ADDRESS` varchar(40),\n" +
                "`C_NATIONKEY` bigint,\n" +
                "`C_PHONE` text,\n" +
                "`C_ACCTBAL` double,\n" +
                "`C_MKTSEGMENT` text,\n" +
                "`C_COMMENT` varchar(117),\n" +
                " primary key (`C_CUSTKEY`)\n" +
                " )engine=InnoDB default charset=utf8;";
        create_tables[1] = "create table if not exists `lineitem`(\n" +
                "`L_ORDERKEY` bigint,\n" +
                "`L_PARTKEY` bigint,\n" +
                "`L_SUPPKEY` bigint,\n" +
                "`L_LINENUMBER` int,\n" +
                "`L_QUANTITY` decimal,\n" +
                "`L_EXTENDEDPRICE` decimal(10,2),\n" +
                "`L_DISCOUNT` decimal(3,2),\n" +
                "`L_TAX` decimal(3,2),\n" +
                "`L_RETURNFLAG` text,\n" +
                "`L_LINESTATUS` text,\n" +
                "`L_SHIPDATE` date,\n" +
                "`L_COMMITDATE` date,\n" +
                "`L_RECEIPTDATE` date,\n" +
                "`L_SHIPINSTRUCT` text,\n" +
                "`L_SHIPMODE` text,\n" +
                "`L_COMMENT` varchar(44),\n" +
                "primary key (`L_ORDERKEY`,`L_LINENUMBER`)\n" +
                ")engine=InnoDB default charset=utf8;";
        create_tables[2] = "create table if not exists `nation`(\n" +
                "    `N_NATIONKEY` bigint,\n" +
                "    `N_NAME` text,\n" +
                "    `N_REGIONKEY` bigint,\n" +
                "    `N_COMMENT` varchar(152),\n" +
                "    primary key (`N_NATIONKEY`)\n" +
                ")engine=InnoDB default charset=utf8;";
        create_tables[3] = "create table if not exists `orders`(\n" +
                "`O_ORDERKEY` bigint,\n" +
                "`O_CUSTKEY` bigint,\n" +
                "`O_ORDERSTATUS` text,\n" +
                "`O_TOTALPRICE` decimal(10,2),\n" +
                "`O_ORDERDATE` date,\n" +
                "`O_ORDERPRIORITY` text,\n" +
                "`O_CLERK` text,\n" +
                "`O_SHIPPRIORITY` int,\n" +
                "`O_COMMENT` varchar(79),\n" +
                "primary key (`O_ORDERKEY`)\n" +
                " )engine=InnoDB default charset=utf8;";
        create_tables[4] = "create table if not exists `region`(\n" +
                "`R_REGIONKEY` bigint,\n" +
                "`R_NAME` text,\n" +
                "`R_COMMENT` varchar(152),\n" +
                "primary key (`R_REGIONKEY`)\n" +
                ")engine=InnoDB default charset=utf8;";
        create_tables[5] = "create table if not exists `part`(\n" +
                "`P_PARTKEY` bigint,\n" +
                "`P_NAME` varchar(55),\n" +
                "`P_MFGR` text,\n" +
                "`P_BRAND` text,\n" +
                "`P_TYPE` varchar(25),\n" +
                "`P_SIZE` int,\n" +
                "`P_CONTAINER` text,\n" +
                "`P_RETAILPRICE` decimal(10,2),\n" +
                "`P_COMMENT` varchar(23),\n" +
                "primary key(`P_PARTKEY`)\n" +
                ")engine=InnoDB default charset=utf8;";
        create_tables[6] = "create table if not exists `supplier`(\n" +
                "`S_SUPPKEY` bigint,\n" +
                "`S_NAME` text,\n" +
                "`S_ADDRESS` varchar(40),\n" +
                "`S_NATIONKEY` bigint,\n" +
                "`S_PHONE` text,\n" +
                "`S_ACCTBAL` decimal(10,2),\n" +
                "`S_COMMENT` varchar(101),\n" +
                "primary key(`S_SUPPKEY`)\n" +
                ")engine=InnoDB default charset=utf8;";
        String query = "select o_year, sum(case when nation = 'UNITED STATES' then volume else 0 end) / sum(volume) as mkt_share \n" +
                "from (select extract(year from o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, n2.n_name as nation \n" +
                "from part,supplier, lineitem, orders, customer, nation n1, nation n2, region \n" +
                "where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey \n" +
                "and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' \n" +
                "and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL') as all_nations \n" +
                "group by o_year ;";
        String query1 = "select n_name,s_suppkey,l_orderkey,l_linenumber from nation,supplier,lineitem where n_nationkey = s_nationkey and s_suppkey=l_suppkey";
        String ins_nation = "INSERT INTO nation (n_nationkey, n_name, n_regionkey,n_comment) VALUES (?,?,?,?)";
        String ins_cust = "INSERT INTO  customer (c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment) VALUES (?,?,?,?,?,?,?,?)";
        String ins_line = "INSERT INTO lineitem (l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax," +
                "l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        String ins_orders = "INSERT INTO orders (o_orderkey,o_custkey,o_orderstatus,o_totalprice,o_orderdate,o_orderpriority," +
                "o_clerk,o_shippriority,o_comment) VALUES (?,?,?,?,?,?,?,?,?)";
        String ins_part = "INSERT INTO part (p_partkey,p_name,p_mfgr,p_brand,p_type,p_size,p_container,p_retailprice,p_comment) VALUES (?,?,?,?,?,?,?,?,?)";
        String ins_region = "INSERT INTO region (r_regionkey,r_name,r_comment) VALUES (?,?,?)";
        String ins_supp = "INSERT INTO supplier (s_suppkey,s_name,s_address,s_nationkey,s_phone,s_acctbal,s_comment) VALUES (?,?,?,?,?,?,?)";
        String del_nation = " DELETE FROM nation WHERE n_nationkey=?";
        String del_cust = " DELETE FROM customer WHERE c_custkey=?";
        String del_orders = " DELETE FROM orders WHERE o_orderkey=?";
        String del_line = " DELETE FROM lineitem WHERE l_orderkey=? and l_linenumber=?";
        String del_supp = " DELETE FROM supplier WHERE s_suppkey=?";
        String del_part = " DELETE FROM part WHERE p_partkey=?";
        String del_region = " DELETE FROM region WHERE r_regionkey=?";

        for (int i = 0; i < drop_tables.length; i++) {
            ps = conn.prepareStatement(drop_tables[i]);
            ps.executeUpdate();
        }
        for (int i = 0; i < create_tables.length; i++) {
            ps = conn.prepareStatement(create_tables[i]);
            ps.executeUpdate();
        }
        Scanner sc = new Scanner(new File(filePath));
        while (sc.hasNextLine()) {
            String tmp = sc.nextLine();
            String[] s = tmp.split("\\|");
            if (s[0].equals("1") || s[0].equals("2") || s[0].equals("3")) {
                ps = conn.prepareStatement(query);
                rs = ps.executeQuery();
//                while (rs.next()) {
//                    String n_name = rs.getString("n_name");
//                    Long l_orderkey = rs.getLong("l_orderkey");
//                    Long s_suppkey = rs.getLong("s_suppkey");
//                    Long l_linenumber = rs.getLong("l_linenumber");
//
//                    System.out.print(n_name);
//                    System.out.print("," + l_orderkey);
//                    System.out.print("," + s_suppkey);
//                    System.out.println("," + l_linenumber);
//                    System.out.print("\n");
//                }
                while (rs.next()) {
                    String o_year = rs.getString("o_year");
                    Double mkt_share = rs.getDouble("mkt_share");
                    System.out.print(o_year);
                    System.out.print("," + mkt_share);
                    System.out.print("\n");
                }
                    if (s[0].equals("1")) {
                        System.out.println("===============25%===============");
                    } else if (s[0].equals("2")) {
                        System.out.println("===============50%===============");
                    } else if (s[0].equals("3")) {
                        System.out.println("===============75%===============");
                    }

                }
                if (s[1].equals("insert")) {
                    if (s[2].equals("nation")) {
                        ps = conn.prepareStatement(ins_nation);
                        for (int i = 1; i < s.length - 2; i++) {
                            ps.setObject(i, s[i + 2]);
                        }
                        ps.executeUpdate();

                    } else if (s[2].equals("customer")) {
                        ps = conn.prepareStatement(ins_cust);
                        for (int i = 1; i < s.length - 2; i++) {
                            ps.setObject(i, s[i + 2]);
                        }
                        ps.executeUpdate();
                    } else if (s[2].equals("lineitem")) {
                        ps = conn.prepareStatement(ins_line);
                        for (int i = 1; i < s.length - 2; i++) {
                            ps.setObject(i, s[i + 2]);
                        }
                        ps.executeUpdate();

                    } else if (s[2].equals("orders")) {
                        ps = conn.prepareStatement(ins_orders);
                        for (int i = 1; i < s.length - 2; i++) {
                            ps.setObject(i, s[i + 2]);
                        }
                        ps.executeUpdate();
                    } else if (s[2].equals("part")) {
                        ps = conn.prepareStatement(ins_part);
                        for (int i = 1; i < s.length - 2; i++) {
                            ps.setObject(i, s[i + 2]);
                        }
                        ps.executeUpdate();
                    } else if (s[2].equals("region")) {
                        ps = conn.prepareStatement(ins_region);
                        for (int i = 1; i < s.length - 2; i++) {
                            ps.setObject(i, s[i + 2]);
                        }
                        ps.executeUpdate();
                    } else if (s[2].equals("supplier")) {
                        ps = conn.prepareStatement(ins_supp);
                        for (int i = 1; i < s.length - 2; i++) {
                            ps.setObject(i, s[i + 2]);
                        }
                        ps.executeUpdate();

                    }
                } else if (s[1].equals("delete")) {
                    if (s[2].equals("nation")) {
                        ps = conn.prepareStatement(del_nation);
                        ps.setObject(1, s[3]);
//                    System.out.println("delete nationkey =" + s[3]);
                        ps.executeUpdate();
                    } else if (s[2].equals("customer")) {
                        ps = conn.prepareStatement(del_cust);
                        ps.setObject(1, s[3]);
                        ps.executeUpdate();
                    } else if (s[2].equals("orders")) {
                        ps = conn.prepareStatement(del_orders);
                        ps.setObject(1, s[3]);
                        ps.executeUpdate();
                    } else if (s[2].equals("lineitem")) {
                        ps = conn.prepareStatement(del_line);
                        ps.setObject(1, s[3]);
                        ps.setObject(2, s[6]);
                        ps.executeUpdate();
//                    System.out.println("delete line =" + s[3]+","+s[6]);
                    } else if (s[2].equals("part")) {
                        ps = conn.prepareStatement(del_part);
                        ps.setObject(1, s[3]);
                        ps.executeUpdate();
                    } else if (s[2].equals("supplier")) {
                        ps = conn.prepareStatement(del_supp);
                        ps.setObject(1, s[3]);
                        ps.executeUpdate();
//                    System.out.println("delete supplier="+ s[3]);
                    } else if (s[2].equals("region")) {
                        ps = conn.prepareStatement(del_region);
                        ps.setObject(1, s[3]);
                        ps.executeUpdate();
                    }
                }
            }

            ps = conn.prepareStatement(query);
            rs = ps.executeQuery();

            while (rs.next()) {
                String o_year = rs.getString("o_year");
                Double mkt_share = rs.getDouble("mkt_share");
                System.out.print(o_year);
                System.out.print("," + mkt_share);
                System.out.print("\n");
            }
//            ps = conn.prepareStatement(query1);
//            rs = ps.executeQuery();
//
//            while (rs.next()) {
//                String n_name = rs.getString("n_name");
//                Long l_orderkey = rs.getLong("l_orderkey");
//                Long l_linenumber = rs.getLong("l_linenumber");
//                Long s_suppkey = rs.getLong("s_suppkey");
//                System.out.print(n_name);
//                System.out.print("," + l_orderkey);
//                System.out.print("," + s_suppkey);
//                System.out.println("," + l_linenumber);
//                System.out.print("\n");
//            }
            System.out.println("===============100%===============");
            ps.close();
            conn.close();


        }
    }



