package preprocess;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class MergeFile {
    public static void main(String[] args) throws IOException {
        String absolutePath = "/Users/phoebe/Documents/BDT/ip/myProject/";
        String filename1 = "mergefile/MergeFile_insert.tbl";
        String filename2 = "mergefile/MergeFile_delete.tbl";
        Scanner sc1 = new Scanner(new File(absolutePath + filename1));
        Scanner sc2 = new Scanner(new File(absolutePath + filename2));
        ArrayList<String> content1 = new ArrayList<>();
        ArrayList<String> content2 = new ArrayList<>();
        while (sc1.hasNextLine()) {
            String tmp = sc1.nextLine();
            content1.add(tmp);
        }
        while (sc2.hasNextLine()) {
            String tmp = sc2.nextLine();
            content2.add(tmp);
        }
        FileWriter fw = new FileWriter(absolutePath + "mergefile/MergeFile.tbl");
        BufferedWriter writer = new BufferedWriter(fw);
        int bias = 70000;
        int count = content1.size() + content2.size() - bias;
        for (String i : content1) {
            int idx = content1.indexOf(i);
//            if (idx < bias) {
//                writer.write("0|" + i);
//                writer.newLine();
//            } else {
//                if (2 * idx - bias == count / 4 || 2 * idx - bias + 1 == count / 4) {
//                    writer.write("1|" + i);
//                    writer.newLine();
//                    writer.write("0|" + content2.get(idx - bias));
//                    writer.newLine();
//                } else if (2 * idx - bias == count / 2 || 2 * idx - bias + 1 == count / 2) {
//                    writer.write("2|" + i);
//                    writer.newLine();
//                    writer.write("0|" + content2.get(idx - bias));
//                    writer.newLine();
//                } else if (2 * idx - bias == count / 4 * 3 || 2 * idx - bias + 1 == count / 4 * 3) {
//                    writer.write("3|" + i);
//                    writer.newLine();
//                    writer.write("0|" + content2.get(idx - bias));
//                    writer.newLine();
//                } else if (idx == content1.size() - 1) {
//                    writer.write("0|" + i);
//                    writer.newLine();
//                    writer.write("4|" + content2.get(idx - bias));
//                    writer.newLine();
 //               } else {
//                    writer.write("0|" + i);
                      writer.write(i);
                      writer.newLine();
//                    writer.write("0|" + content2.get(idx - bias));
                      if(idx>=bias){
                          writer.write( content2.get(idx - bias));
                          writer.newLine();
                      }

                }
//            }
//        }
            writer.close();

    }
}
