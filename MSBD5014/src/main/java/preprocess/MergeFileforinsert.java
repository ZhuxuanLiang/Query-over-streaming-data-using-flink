package preprocess;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;

public class MergeFileforinsert {
    public static void main(String[] args) throws IOException {
        ArrayList<String> content = new ArrayList<>();
        String absolutePath = "/Users/phoebe/Documents/BDT/ip/myProject/";
        String filename1 = "testdata_2/customer.tbl";
        String filename2 = "testdata_2/lineitem.tbl";
        String filename3 = "testdata_2/nation.tbl";
        String filename4 = "testdata_2/orders.tbl";
        String filename5 = "testdata_2/region.tbl";
        String filename6 = "testdata_2/part.tbl";
        String filename7 = "testdata_2/supplier.tbl";
        Scanner sc1 = new Scanner(new File(absolutePath + filename1));
        while (sc1.hasNextLine()) {
            String tmp = sc1.nextLine();
            content.add("insert|customer|" + tmp);
//            content.add("delete|customer|" + tmp);
        }
        Scanner sc2 = new Scanner(new File(absolutePath + filename2));
        while (sc2.hasNextLine()) {
            String tmp = sc2.nextLine();
            content.add("insert|lineitem|" + tmp);
 //           content.add("delete|lineitem|" + tmp);
        }
        Scanner sc3 = new Scanner(new File(absolutePath + filename3));
        while (sc3.hasNextLine()) {
            String tmp = sc3.nextLine();
            content.add("insert|nation|" + tmp);
 //           content.add("delete|nation|" + tmp);
        }
        Scanner sc4 = new Scanner(new File(absolutePath + filename4));
        while (sc4.hasNextLine()) {
            String tmp = sc4.nextLine();
            content.add("insert|orders|" + tmp);
//            content.add("delete|orders|" + tmp);
        }
        Scanner sc5 = new Scanner(new File(absolutePath + filename5));
        while (sc5.hasNextLine()) {
            String tmp = sc5.nextLine();
            content.add("insert|region|" + tmp);
//            content.add("delete|region|" + tmp);
        }
        Scanner sc6 = new Scanner(new File(absolutePath + filename6));
        while (sc6.hasNextLine()) {
            String tmp = sc6.nextLine();
            content.add("insert|part|" + tmp);
//            content.add("delete|part|" + tmp);
        }
        Scanner sc7 = new Scanner(new File(absolutePath + filename7));
        while (sc7.hasNextLine()) {
            String tmp = sc7.nextLine();
            content.add("insert|supplier|" + tmp);
//            content.add("delete|supplier|" + tmp);
        }

        Collections.shuffle(content);
        FileWriter fw = new FileWriter(absolutePath+"mergefile/MergeFile_insert.tbl");

        BufferedWriter writer = new BufferedWriter(fw);
        int count = content.size();
//        for (String i : content) {
//                writer.write(i);
//                writer.newLine();
//        }
        for (String i : content) {
            int idx = content.indexOf(i);
            if (idx == count / 4) {
                writer.write("1|" + i);
                writer.newLine();
            } else if (idx == count / 2) {
                writer.write("2|" + i);
                writer.newLine();
            } else if (idx == count / 4 * 3) {
                writer.write("3|" + i);
                writer.newLine();
            } else if (idx == count - 1) {
                writer.write("4|" + i);
                writer.newLine();
            } else {
                writer.write("0|" + i);
                writer.newLine();
            }
        }
        writer.close();

    }
}


